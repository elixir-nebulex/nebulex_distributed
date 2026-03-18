defmodule Nebulex.Adapters.LazyReplicated.Supervisor do
  @moduledoc false

  use Supervisor

  alias Nebulex.Adapters.LazyReplicated.ClusterMonitor
  alias Nebulex.Streams
  alias Nebulex.Streams.Invalidator

  ## API

  @doc false
  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg)
  end

  ## Supervisor callback

  @impl true
  def init({cache, adapter_meta, primary_opts, stream_opts}) do
    # Get the primary cache module
    primary = cache.__primary__()

    # Common options
    common_opts =
      primary_opts
      |> Keyword.take([:name])
      |> Keyword.put(:cache, primary)

    # Build stream options - register with primary cache since it's started first
    stream_opts =
      Keyword.merge(stream_opts, [broadcast_fun: :broadcast_from] ++ common_opts)

    children = [
      {primary, primary_opts},
      {Streams, stream_opts},
      {Invalidator, common_opts},
      {ClusterMonitor, adapter_meta}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
