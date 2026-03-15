defmodule Nebulex.Adapters.LazyReplicated.ClusterMonitor do
  @moduledoc false

  use GenServer

  import Nebulex.Utils

  alias Nebulex.Distributed.Cluster

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

    {:ok, %{adapter_meta: adapter_meta, pg_group: pg_group}}
  end

  @impl true
  def terminate(_reason, %{pg_group: pg_group}) do
    # Leave the PG group when the process terminates
    :ok = Cluster.leave(pg_group)
  end
end
