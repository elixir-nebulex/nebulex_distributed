defmodule Nebulex.Adapters.Partitioned.RingMonitor do
  @moduledoc """
  A `GenServer` that keeps the consistent hash ring in sync with the
  current cluster topology for `Nebulex.Adapters.Partitioned`.

  The RingMonitor is started automatically as part of the partitioned
  cache supervision tree. It performs the following duties:

    * **Cluster subscription** - Subscribes to Erlang's `:pg` (process
      groups) via `Nebulex.Distributed.Cluster.monitor_scope/0` to
      receive join and leave notifications for the cache's group.

    * **Ring synchronisation** - When nodes join or leave the cluster,
      the RingMonitor adds or removes them from the `ExHashRing.Ring`,
      ensuring key-to-node mappings stay up to date.

    * **Periodic rejoin** - To handle race conditions during concurrent
      cluster startup (where some nodes may miss initial join events),
      the RingMonitor periodically rejoins the `:pg` group at the
      interval configured by `:rejoin_interval` (default: 30 seconds).
      Because `:pg` joins are idempotent, this forces all nodes to
      refresh their ring view and guarantees eventual consistency.

  ## Telemetry events

  The RingMonitor emits Telemetry events under
  `telemetry_prefix ++ [:ring_monitor, event]`. See the
  "Adapter-specific telemetry events" section in
  `Nebulex.Adapters.Partitioned` for the full list of events,
  measurements, and metadata.
  """

  use GenServer

  import Nebulex.Utils

  alias ExHashRing.Ring
  alias Nebulex.Distributed.Cluster
  alias Nebulex.Telemetry

  # State
  defstruct adapter_meta: nil, ring: nil, pg_ref: nil, rejoin_interval: nil, rejoin_timer_ref: nil

  ## API

  @doc false
  def start_link(%{name: name} = adapter_meta) do
    GenServer.start_link(__MODULE__, adapter_meta, name: camelize_and_concat([name, RingMonitor]))
  end

  ## GenServer Callbacks

  @impl true
  def init(%{hash_ring: hash_ring, rejoin_interval: rejoin_interval} = adapter_meta) do
    # Trap exit signals to run cleanup job
    _ = Process.flag(:trap_exit, true)

    # Get the ring name
    ring = Keyword.fetch!(hash_ring, :name)

    # Ring monitor started
    :ok = dispatch_telemetry_event(:started, adapter_meta)

    # Subscribe the ring monitor to updates from the cluster
    pg_ref = Cluster.monitor_scope()

    # Build initial state
    state = %__MODULE__{
      adapter_meta: adapter_meta,
      ring: ring,
      pg_ref: pg_ref,
      rejoin_interval: rejoin_interval
    }

    # Ensure joining the cluster when the cache supervision tree is started
    :ok = join(state)

    # Add the current node to the ring
    :ok = add_ring_nodes(ring, [node()], adapter_meta)

    # Return initial state and continue with setting up the rejoin timer
    {:ok, state, {:continue, :setup_rejoin_timer}}
  end

  @impl true
  def handle_continue(:setup_rejoin_timer, state) do
    {:noreply, refresh_rejoin_timer(state)}
  end

  @impl true
  def handle_info(message, state)

  # Handle EXIT signals
  def handle_info({:EXIT, _from, reason}, %__MODULE__{adapter_meta: adapter_meta} = state) do
    # Ring monitor received exit signal
    :ok = dispatch_telemetry_event(:exit, adapter_meta, %{reason: reason})

    {:stop, reason, state}
  end

  # PG join event
  def handle_info(
        {pg_ref, :join, ring, pids},
        %__MODULE__{pg_ref: pg_ref, adapter_meta: adapter_meta, ring: ring} = state
      ) do
    :ok = add_ring_nodes(ring, pids, adapter_meta)

    {:noreply, state}
  end

  # PG leave event
  def handle_info(
        {pg_ref, :leave, ring, pids},
        %__MODULE__{pg_ref: pg_ref, adapter_meta: adapter_meta, ring: ring} = state
      ) do
    :ok = rem_ring_nodes(ring, pids, adapter_meta)

    {:noreply, state}
  end

  # Rejoin interval timeout
  def handle_info(:rejoin, %__MODULE__{} = state) do
    # Join the PG group
    :ok = join(state)

    {:noreply, refresh_rejoin_timer(state)}
  end

  # Ignore other messages
  def handle_info(_info, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(reason, %__MODULE__{ring: ring, adapter_meta: adapter_meta}) do
    # Ensure leaving the cluster when the cache stops
    :ok = Cluster.leave(ring)

    # Ring monitor stopped or terminated
    :ok = dispatch_telemetry_event(:stopped, adapter_meta, %{reason: reason})
  end

  ## Private Functions

  defp dispatch_telemetry_event(event, adapter_meta, meta \\ %{}) do
    Telemetry.execute(
      adapter_meta.telemetry_prefix ++ [:ring_monitor, event],
      %{system_time: System.system_time()},
      Map.merge(meta, %{adapter_meta: adapter_meta, node: node()})
    )
  end

  defp join(%__MODULE__{ring: ring, adapter_meta: adapter_meta}) do
    # Join the PG group
    :ok = Cluster.join(ring)

    # Ring monitor joined the cache to the cluster
    :ok = dispatch_telemetry_event(:joined, adapter_meta)
  end

  defp add_ring_nodes(ring, pids, adapter_meta) do
    ring
    |> Cluster.pg_nodes()
    |> MapSet.new()
    |> MapSet.difference(ring |> Cluster.ring_nodes() |> MapSet.new())
    |> MapSet.union(pids |> node_names() |> MapSet.new())
    |> MapSet.to_list()
    |> for_ring_node(&Ring.add_node(ring, &1))
    |> then(&dispatch_telemetry_event(:nodes_added, adapter_meta, %{nodes: &1}))
  end

  defp rem_ring_nodes(ring, pids, adapter_meta) do
    ring
    |> Cluster.ring_nodes()
    |> MapSet.new()
    |> MapSet.difference(ring |> Cluster.pg_nodes() |> MapSet.new())
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

  defp refresh_rejoin_timer(
         %__MODULE__{rejoin_timer_ref: rejoin_timer_ref, rejoin_interval: rejoin_interval} = state
       ) do
    _ignore =
      if rejoin_timer_ref do
        Process.cancel_timer(rejoin_timer_ref)
      end

    rejoin_timer_ref = Process.send_after(self(), :rejoin, rejoin_interval)

    %{state | rejoin_timer_ref: rejoin_timer_ref}
  end
end
