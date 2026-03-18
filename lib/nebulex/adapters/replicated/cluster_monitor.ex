defmodule Nebulex.Adapters.Replicated.ClusterMonitor do
  @moduledoc false

  use GenServer

  import Nebulex.Utils

  alias Nebulex.Adapters.Replicated.Replicator
  alias Nebulex.Distributed.Cluster
  alias Nebulex.Telemetry

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

    # Join the PG group
    :ok = Cluster.join(pg_group)

    {:ok, %{adapter_meta: adapter_meta, pg_group: pg_group}, {:continue, :bootstrap}}
  end

  @impl true
  def handle_continue(:bootstrap, %{adapter_meta: adapter_meta} = state) do
    adapter_meta.pg_group
    |> Cluster.pg_nodes()
    |> List.delete(node())
    |> bootstrap_from_peers(adapter_meta)

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{pg_group: pg_group}) do
    # Leave the PG group when the process terminates
    :ok = Cluster.leave(pg_group)
  end

  ## Private functions

  defp bootstrap_from_peers([], _adapter_meta) do
    :ok
  end

  defp bootstrap_from_peers([peer | rest], adapter_meta) do
    event = adapter_meta.telemetry_prefix ++ [:bootstrap]
    metadata = %{adapter_meta: adapter_meta, node: node(), peer: peer}

    Telemetry.span(event, metadata, fn ->
      total = Replicator.copy_entries(peer, adapter_meta)

      {total, Map.put(metadata, :total, total)}
    end)

    # coveralls-ignore-start
  rescue
    # Bootstrap failed for this peer, try the next one
    _error ->
      bootstrap_from_peers(rest, adapter_meta)
  catch
    :exit, _reason ->
      # Bootstrap failed for this peer, try the next one
      bootstrap_from_peers(rest, adapter_meta)
      # coveralls-ignore-stop
  end
end
