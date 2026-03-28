defmodule Nebulex.Adapters.Replicated.ClusterMonitor do
  @moduledoc false

  use GenServer

  import Nebulex.Utils

  alias Nebulex.Adapters.Replicated.Replicator
  alias Nebulex.Distributed.{Cluster, RPC}
  alias Nebulex.Telemetry

  # State
  defstruct [:adapter_meta, :pg_group, :pg_ref, :cluster_nodes]

  ## API

  @doc false
  def start_link(%{name: name} = adapter_meta) do
    name = camelize_and_concat([name, ClusterMonitor])

    GenServer.start_link(__MODULE__, adapter_meta, name: name)
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
    diff =
      pids
      |> Enum.map(&node/1)
      |> MapSet.new()
      |> MapSet.difference(cluster_nodes)

    # Push entries to genuinely new nodes (push-based bootstrap).
    if MapSet.size(diff) > 0 and leader?(cluster_nodes) do
      diff
      |> MapSet.to_list()
      |> Enum.each(&push_to_new_node(&1, adapter_meta))
    end

    # Update cluster view
    updated_nodes = MapSet.union(cluster_nodes, diff)

    {:noreply, %{state | cluster_nodes: updated_nodes}}
  rescue
    # coveralls-ignore-start
    _error -> {:noreply, state}
  catch
    :exit, _reason ->
      {:noreply, state}
      # coveralls-ignore-stop
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
      total = Replicator.push_entries(new_node, adapter_meta)

      {total, Map.put(metadata, :total, total)}
    end)
  end

  ## Private functions

  # Simple leader election: all existing nodes share the same cluster view
  # (they all received the same :pg events), so they all compute the same
  # Enum.min() — only the node with the smallest name (Erlang term ordering)
  # pushes, avoiding duplicate work. If views temporarily diverge due to
  # event ordering, the worst case is zero or multiple pushers, both safe:
  # :put_new is idempotent, and anti-entropy repairs any remaining gaps.
  defp leader?(cluster_nodes) do
    node() == cluster_nodes |> MapSet.to_list() |> Enum.min()
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
