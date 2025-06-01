defmodule Nebulex.Adapters.Partitioned.Bootstraper do
  @moduledoc false
  use GenServer

  import Nebulex.Utils

  alias ExHashRing.Ring
  alias Nebulex.Distributed.Cluster
  alias Nebulex.Telemetry

  # State
  defstruct [:adapter_meta, :ring, :pg_ref]

  ## API

  @doc false
  def start_link({%{name: name}, _} = state) do
    GenServer.start_link(
      __MODULE__,
      state,
      name: camelize_and_concat([name, Bootstrap])
    )
  end

  ## GenServer Callbacks

  @impl true
  def init({adapter_meta, _opts}) do
    # Trap exit signals to run cleanup job
    _ = Process.flag(:trap_exit, true)

    # Bootstrap started
    :ok = dispatch_telemetry_event(:started, adapter_meta)

    # Subscribe the bootstraper to updates from the cluster
    pg_ref = Cluster.monitor_scope()

    # Ensure joining the cluster when the cache supervision tree is started
    :ok = Cluster.join(adapter_meta.name)

    # Bootstrap joined the cache to the cluster
    :ok = dispatch_telemetry_event(:joined, adapter_meta)

    # Build initial state
    state = build_state(adapter_meta, pg_ref)

    # Set up the ring
    :ok = add_ring_nodes(state.ring, [node()], state.adapter_meta)

    # Start bootstrap process
    {:ok, state}
  end

  @impl true
  def handle_info(message, state)

  # Handle EXIT signals
  def handle_info({:EXIT, _from, reason}, %__MODULE__{adapter_meta: adapter_meta} = state) do
    # Bootstrap received exit signal
    :ok = dispatch_telemetry_event(:exit, adapter_meta, %{reason: reason})

    {:stop, reason, state}
  end

  # PG join event
  def handle_info(
        {pg_ref, :join, group, pids},
        %__MODULE__{pg_ref: pg_ref, adapter_meta: %{name: group} = adapter_meta, ring: ring} = state
      ) do
    :ok = add_ring_nodes(ring, pids, adapter_meta)

    {:noreply, state}
  end

  # PG leave event
  def handle_info(
        {pg_ref, :leave, group, pids},
        %__MODULE__{pg_ref: pg_ref, adapter_meta: %{name: group} = adapter_meta, ring: ring} = state
      ) do
    :ok = rem_ring_nodes(ring, pids, adapter_meta)

    {:noreply, state}
  end

  # Ignore
  def handle_info(_info, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(reason, %__MODULE__{adapter_meta: adapter_meta}) do
    # Ensure leaving the cluster when the cache stops
    :ok = Cluster.leave(adapter_meta.name)

    # Bootstrap stopped or terminated
    :ok = dispatch_telemetry_event(:stopped, adapter_meta, %{reason: reason})
  end

  ## Private Functions

  # Inline common instructions
  @compile {:inline, node_names: 1}

  defp build_state(%{hash_ring: hash_ring} = adapter_meta, pg_ref) do
    %__MODULE__{
      adapter_meta: adapter_meta,
      ring: Keyword.fetch!(hash_ring, :name),
      pg_ref: pg_ref
    }
  end

  defp dispatch_telemetry_event(event, adapter_meta, meta \\ %{}) do
    Telemetry.execute(
      adapter_meta.telemetry_prefix ++ [:bootstrap, event],
      %{system_time: System.system_time()},
      Map.merge(meta, %{
        adapter_meta: adapter_meta,
        node: node()
      })
    )
  end

  defp add_ring_nodes(ring, pids, %{name: name} = adapter_meta) do
    name
    |> Cluster.pg_nodes()
    |> MapSet.new()
    |> MapSet.difference(ring |> Cluster.ring_nodes() |> MapSet.new())
    |> MapSet.union(pids |> node_names() |> MapSet.new())
    |> MapSet.to_list()
    |> for_ring_node(&Ring.add_node(ring, &1))
    |> then(&dispatch_telemetry_event(:nodes_added, adapter_meta, %{nodes: &1}))
  end

  defp rem_ring_nodes(ring, pids, %{name: name} = adapter_meta) do
    ring
    |> Cluster.ring_nodes()
    |> MapSet.new()
    |> MapSet.difference(name |> Cluster.pg_nodes() |> MapSet.new())
    |> MapSet.union(pids |> node_names() |> MapSet.new())
    |> MapSet.to_list()
    |> for_ring_node(&Ring.remove_node(ring, &1))
    |> then(&dispatch_telemetry_event(:nodes_removed, adapter_meta, %{nodes: &1}))
  end

  defp for_ring_node(nodes, action) do
    Enum.reduce(nodes, [], fn node, acc ->
      case action.(node) do
        {:ok, _nodes} -> [node | acc]
        {:error, _reason} -> acc
      end
    end)
  end

  defp node_names(pids_or_nodes) do
    Enum.map(pids_or_nodes, fn
      pid when is_pid(pid) -> node(pid)
      node when is_atom(node) -> node
    end)
  end
end
