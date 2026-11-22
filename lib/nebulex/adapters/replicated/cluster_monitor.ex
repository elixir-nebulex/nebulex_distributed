defmodule Nebulex.Adapters.Replicated.ClusterMonitor do
  @moduledoc false

  use GenServer

  alias Nebulex.Adapters.Replicated.Replicator
  alias Nebulex.Distributed.{Cluster, RPC}
  alias Nebulex.Telemetry

  # State
  defstruct [:adapter_meta, :pg_group, :pg_ref, :cluster_nodes]

  ## API

  @doc false
  def start_link(%{pg_group: pg_group} = adapter_meta) do
    # Registered under the PG group name, so `Cluster.join/1` and
    # `Cluster.leave/1` act on this process from any caller.
    GenServer.start_link(__MODULE__, adapter_meta, name: pg_group)
  end

  ## GenServer Callbacks

  @impl true
  def init(%{pg_group: pg_group} = adapter_meta) do
    # Trap exit signals to run cleanup on termination
    _ = Process.flag(:trap_exit, true)

    # Subscribe to PG scope events for cluster membership changes
    pg_ref = Cluster.monitor_scope()

    # Join the PG group
    :ok = Cluster.join(pg_group)

    state = %__MODULE__{
      adapter_meta: adapter_meta,
      pg_group: pg_group,
      pg_ref: pg_ref,
      cluster_nodes: MapSet.new([node()])
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
  def handle_info(message, state)

  # PG join event — new node(s) joined the cluster.
  # Existing nodes push their cache entries to the new node (push-based bootstrap).
  def handle_info(
        {pg_ref, :join, pg_group, pids},
        %__MODULE__{
          pg_ref: pg_ref,
          pg_group: pg_group,
          adapter_meta: adapter_meta,
          cluster_nodes: cluster_nodes
        } = state
      ) do
    # Calculate the difference between the new nodes and the cluster nodes
    new_nodes =
      pids
      |> Enum.map(&node/1)
      |> MapSet.new()
      |> MapSet.difference(cluster_nodes)

    # The pusher is elected among the nodes `:pg` knows right now, minus the
    # joiners, rather than from this process's view. The view can lag behind
    # `:pg` (events still queued, or an earlier join batch lost to a failed
    # push) and a stale view elects extra pushers; `:pg` converges as soon as
    # the connection is up.
    if MapSet.size(new_nodes) > 0 do
      candidates = pusher_candidates(pg_group, new_nodes)

      new_nodes
      |> Enum.filter(&pusher?(candidates, &1))
      |> Enum.each(&safe_push(&1, adapter_meta))
    end

    # The view is updated regardless of what the pushes do, so a failing push
    # cannot drop the joined nodes from it.
    {:noreply, %{state | cluster_nodes: MapSet.union(cluster_nodes, new_nodes)}}
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

  # Ignore other messages
  def handle_info(_info, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{pg_group: pg_group}) do
    # Leave the PG group when the process terminates
    :ok = Cluster.leave(pg_group)
  end

  ## Public functions (called locally and via tests)

  @doc false
  def push_to_new_node(new_node, adapter_meta) do
    event = adapter_meta.telemetry_prefix ++ [:bootstrap]
    metadata = %{adapter_meta: adapter_meta, node: node(), peer: new_node}

    Telemetry.span(event, metadata, fn ->
      {total, errors} = Replicator.push_entries(new_node, adapter_meta)

      {{total, errors},
       Map.merge(metadata, %{
         total: total,
         errors: errors,
         anti_entropy: is_integer(adapter_meta.anti_entropy_interval)
       })}
    end)
  end

  ## Private functions

  # Every node `:pg` currently knows for the group, except the nodes in the
  # join batch being handled, which have nothing to push. This node is
  # always a candidate: it is a member of the group and its own join never
  # reaches the handler as a new node.
  defp pusher_candidates(pg_group, new_nodes) do
    pg_group
    |> Cluster.pg_nodes()
    |> Enum.reject(&MapSet.member?(new_nodes, &1))
    |> Enum.sort()
  end

  # The pusher for a joiner is picked by a hash of the joiner over the
  # sorted candidates, so the pushes of a rollout are spread across the
  # nodes. Every node evaluates it against the same `:pg` membership, so
  # they agree as soon as `:pg` has converged. If memberships temporarily
  # diverge (or a node on the previous release elects by smallest name),
  # the worst case is zero or two pushers, both safe: bootstrap entries are
  # gap fills, and the anti-entropy catch-up fills what is missing.
  defp pusher?(candidates, new_node) do
    node() == Enum.at(candidates, :erlang.phash2(new_node, length(candidates)))
  end

  # The pushes run one after another in this process. A push that raises
  # (for example, the target raising mid-bootstrap) is reported by the
  # `:bootstrap` span's `:exception` event and rescued here, so it neither
  # crashes the monitor nor stops the pushes to the other joiners.
  defp safe_push(new_node, adapter_meta) do
    _ = push_to_new_node(new_node, adapter_meta)

    :ok
  rescue
    _error -> :ok
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
