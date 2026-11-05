defmodule Nebulex.Adapters.Replicated.ClusterMonitor do
  @moduledoc false

  use GenServer

  import Nebulex.Utils

  alias Nebulex.Adapters.Replicated.Replicator
  alias Nebulex.Distributed.{Cluster, RPC}
  alias Nebulex.Telemetry

  # State
  defstruct [
    :adapter_meta,
    :pg_group,
    :ready_pg_group,
    :pg_ref,
    :cluster_nodes,
    :task_supervisor,
    queue: :queue.new(),
    queued: MapSet.new(),
    in_flight: nil,
    ready?: false
  ]

  ## API

  @doc false
  def start_link(adapter_meta) do
    GenServer.start_link(__MODULE__, adapter_meta, name: server_name(adapter_meta))
  end

  @doc false
  @spec server_name(map()) :: atom()
  def server_name(%{name: name}) do
    camelize_and_concat([name, ClusterMonitor])
  end

  @doc false
  @spec task_supervisor_name(map()) :: atom()
  def task_supervisor_name(%{name: name}) do
    camelize_and_concat([name, "BootstrapTasks"])
  end

  @doc false
  @spec bootstrap_completed(map()) :: :ok
  def bootstrap_completed(adapter_meta) do
    GenServer.cast(server_name(adapter_meta), :bootstrap_completed)
  end

  ## GenServer Callbacks

  @impl true
  def init(%{pg_group: pg_group, ready_pg_group: ready_pg_group} = adapter_meta) do
    # Trap exit signals to run cleanup on termination
    _ = Process.flag(:trap_exit, true)

    # Subscribe to PG scope events for cluster membership changes
    pg_ref = Cluster.monitor_scope()

    # Join the PG group
    :ok = Cluster.join(pg_group)

    state = %__MODULE__{
      adapter_meta: adapter_meta,
      pg_group: pg_group,
      ready_pg_group: ready_pg_group,
      pg_ref: pg_ref,
      cluster_nodes: MapSet.new([node()]),
      task_supervisor: task_supervisor_name(adapter_meta)
    }

    {:ok, state, {:continue, :post_join}}
  end

  @impl true
  def handle_continue(:post_join, %{adapter_meta: adapter_meta} = state) do
    # After joining, reset the GC interval on all nodes to synchronize
    # generation rotation and prevent premature eviction of bootstrapped data.
    cluster_nodes = Cluster.pg_nodes(adapter_meta.pg_group)
    :ok = maybe_reset_gc_interval(adapter_meta, cluster_nodes)

    {:noreply, %{state | cluster_nodes: MapSet.new(cluster_nodes)}}
  end

  @impl true
  def handle_cast(:bootstrap_completed, %__MODULE__{} = state), do: {:noreply, mark_ready(state)}

  @impl true
  def handle_info(message, state)

  # PG join event — new node(s) joined the cluster.
  # Existing nodes push their cache entries to the new node (push-based bootstrap).
  def handle_info(
        {pg_ref, :join, pg_group, pids},
        %__MODULE__{pg_ref: pg_ref, pg_group: pg_group, cluster_nodes: cluster_nodes} = state
      ) do
    # Calculate the difference between the new nodes and the cluster nodes
    new_nodes =
      pids
      |> Enum.map(&node/1)
      |> MapSet.new()
      |> MapSet.difference(cluster_nodes)

    # The view is updated regardless of what the pushes do, so a failing push
    # cannot drop the joined nodes from it.
    state = %{state | cluster_nodes: MapSet.union(cluster_nodes, new_nodes)}

    # The pusher is elected among the nodes `:pg` knows right now, minus the
    # joiners, rather than from this process's view. The view can lag behind
    # `:pg` (events still queued, or an earlier join batch lost to a failed
    # push) and a stale view elects extra pushers; `:pg` converges as soon as
    # the connection is up.
    if MapSet.size(new_nodes) > 0 and leader?(pusher_candidates(state, new_nodes)) do
      {:noreply, new_nodes |> Enum.reduce(state, &enqueue/2) |> maybe_push_next()}
    else
      {:noreply, state}
    end
  end

  # PG leave event — node(s) left the cluster
  def handle_info(
        {pg_ref, :leave, pg_group, pids},
        %__MODULE__{pg_ref: pg_ref, pg_group: pg_group, cluster_nodes: cluster_nodes} = state
      ) do
    # Update cluster view
    updated_nodes =
      pids
      |> Enum.map(&node/1)
      |> MapSet.new()
      |> then(&MapSet.difference(cluster_nodes, &1))

    {:noreply, %{state | cluster_nodes: updated_nodes}}
  end

  # A bootstrapped node left. If it was the pusher, whatever it still had
  # queued or in flight is stranded, and the other nodes decided "not me" for
  # those joiners when they arrived. The node that leads now queues every
  # member that is not bootstrapped yet; dedupe and the membership check at
  # push time keep this cheap when nothing was stranded.
  def handle_info(
        {pg_ref, :leave, ready_pg_group, _pids},
        %__MODULE__{pg_ref: pg_ref, ready_pg_group: ready_pg_group} = state
      ) do
    if leader?(pusher_candidates(state, MapSet.new())) do
      ready = MapSet.new(Cluster.pg_nodes(ready_pg_group))

      state =
        state.pg_group
        |> Cluster.pg_nodes()
        |> Enum.reject(&(&1 == node() or MapSet.member?(ready, &1)))
        |> Enum.reduce(state, &enqueue/2)

      {:noreply, maybe_push_next(state)}
    else
      {:noreply, state}
    end
  end

  # The push finished: the target is bootstrapped, and so is this node if it
  # pushed as the fallback, which seeds the bootstrapped group for a cluster
  # that has none. Then take the next one.
  def handle_info({ref, _total}, %__MODULE__{in_flight: {node, ref}} = state) do
    _ = Process.demonitor(ref, [:flush])
    :ok = RPC.cast(node, __MODULE__, :bootstrap_completed, [state.adapter_meta])

    {:noreply, maybe_push_next(mark_ready(%{state | in_flight: nil}))}
  end

  # The push failed. The task logged it and the `:bootstrap` span reported it.
  # The target is not told the bootstrap completed, so it stays out of the
  # pusher candidates.
  def handle_info(
        {:DOWN, ref, :process, _pid, _reason},
        %__MODULE__{in_flight: {_node, ref}} = state
      ) do
    {:noreply, maybe_push_next(%{state | in_flight: nil})}
  end

  # Ignore other messages
  def handle_info(_info, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{pg_group: pg_group, ready_pg_group: ready_pg_group}) do
    # Leave the PG groups when the process terminates
    :ok = Cluster.leave(ready_pg_group)
    :ok = Cluster.leave(pg_group)
  end

  ## Public functions (called locally and via tests)

  @doc false
  def push_to_new_node(new_node, adapter_meta) do
    event = adapter_meta.telemetry_prefix ++ [:bootstrap]
    metadata = %{adapter_meta: adapter_meta, node: node(), peer: new_node}

    Telemetry.span(event, metadata, fn ->
      total = Replicator.push_entries(new_node, adapter_meta)

      {total, Map.put(metadata, :total, total)}
    end)
  end

  ## Private functions

  defp mark_ready(%__MODULE__{ready?: true} = state), do: state

  defp mark_ready(%__MODULE__{ready_pg_group: ready_pg_group} = state) do
    :ok = Cluster.join(ready_pg_group)

    %{state | ready?: true}
  end

  # The bootstrapped members, except the nodes in the join batch being
  # handled: a joiner may well have the smallest name, but it has nothing to
  # push yet. A member still receiving its bootstrap has a partial cache, so
  # it is not a candidate either. Only a cluster with no bootstrapped member
  # at all falls back to every member, which at least makes progress; the
  # pusher then becomes the first bootstrapped member. A node that connects
  # to a cluster with bootstrapped members sees them as joiners, finds no
  # candidate, and waits to be pushed to like any other joiner.
  defp pusher_candidates(%__MODULE__{pg_group: pg_group, ready_pg_group: ready_pg_group}, excluded) do
    case Cluster.pg_nodes(ready_pg_group) do
      [] -> members_except(pg_group, excluded)
      ready -> Enum.reject(ready, &MapSet.member?(excluded, &1))
    end
  end

  defp members_except(group, excluded) do
    group
    |> Cluster.pg_nodes()
    |> Enum.reject(&MapSet.member?(excluded, &1))
  end

  # Simple leader election: the candidate with the smallest name (Erlang term
  # ordering) pushes. Every node evaluates it against the same `:pg`
  # membership, so they agree as soon as `:pg` has converged. If memberships
  # temporarily diverge, the worst case is zero or multiple pushers, both
  # safe: `:put_new` is idempotent, and anti-entropy repairs any remaining
  # gaps.
  defp leader?([]), do: false
  defp leader?(candidates), do: node() == Enum.min(candidates)

  # A node that leaves and rejoins while it is still queued is pushed once. One
  # that rejoins while its push is in flight is queued again: that push may be
  # streaming to the cache that just went away, and if it fails nothing else
  # would push to the restarted node.
  defp enqueue(node, %__MODULE__{queue: queue, queued: queued} = state) do
    if MapSet.member?(queued, node) do
      state
    else
      %{state | queue: :queue.in(node, queue), queued: MapSet.put(queued, node)}
    end
  end

  # One push at a time, in arrival order, whatever join events the nodes came
  # in: `:pg` delivers one event per joiner, so a rollout is many events.
  defp maybe_push_next(%__MODULE__{in_flight: nil} = state) do
    case :queue.out(state.queue) do
      {{:value, node}, queue} ->
        state = %{state | queue: queue, queued: MapSet.delete(state.queued, node)}

        if node in Cluster.pg_nodes(state.pg_group) do
          start_push(node, state)
        else
          # The node left before its turn
          maybe_push_next(state)
        end

      {:empty, _queue} ->
        state
    end
  end

  defp maybe_push_next(%__MODULE__{} = state) do
    state
  end

  # The push runs in an unlinked task: a failure surfaces as a `:DOWN` message
  # here, in the task's log, and in the `:bootstrap` span, not as a crash of
  # this process and its queue.
  defp start_push(node, %__MODULE__{task_supervisor: task_supervisor} = state) do
    task =
      Task.Supervisor.async_nolink(task_supervisor, __MODULE__, :push_to_new_node, [
        node,
        state.adapter_meta
      ])

    %{state | in_flight: {node, task.ref}}
  end

  defp maybe_reset_gc_interval(%{cache: cache} = adapter_meta, cluster_nodes) do
    if cache.__primary__().__adapter__() == Nebulex.Adapters.Local do
      RPC.multicall(
        cluster_nodes,
        Nebulex.Adapters.Replicated,
        :with_dynamic_cache,
        [adapter_meta, :reset_gc_interval, []],
        adapter_meta.replication_timeout
      )
    end

    :ok
  end
end
