defmodule Nebulex.Adapters.Partitioned.Supervisor do
  @moduledoc false

  use Supervisor

  import Nebulex.Utils

  alias Nebulex.Adapters.Partitioned.RingMonitor

  ## API

  @doc false
  def start_link({cache, name, adapter_meta, primary_opts}) do
    name = camelize_and_concat([name, Supervisor])

    Supervisor.start_link(__MODULE__, {cache, adapter_meta, primary_opts}, name: name)
  end

  ## Supervisor callback

  @impl true
  def init({cache, adapter_meta, primary_opts}) do
    children = [
      {cache.__primary__(), primary_opts},
      {ExHashRing.Ring, adapter_meta.hash_ring},
      {RingMonitor, Map.put(adapter_meta, :cache, cache)}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
