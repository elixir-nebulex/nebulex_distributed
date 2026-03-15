defmodule Nebulex.Adapters.Replicated.Supervisor do
  @moduledoc false

  use Supervisor

  alias Nebulex.Adapters.Replicated.{ClusterMonitor, Replicator}

  ## API

  @doc false
  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg)
  end

  ## Supervisor callback

  @impl true
  def init({cache, adapter_meta, primary_opts, buffer_opts}) do
    primary = cache.__primary__()

    children = [
      {primary, primary_opts},
      Supervisor.child_spec(
        {PartitionedBuffer.Map,
         Keyword.merge(buffer_opts,
           name: adapter_meta.inbox,
           processor: {Replicator, :process_inbox, [adapter_meta]}
         )},
        id: adapter_meta.inbox
      ),
      Supervisor.child_spec(
        {PartitionedBuffer.Map,
         Keyword.merge(buffer_opts,
           name: adapter_meta.outbox,
           processor: {Replicator, :process_outbox, [adapter_meta]}
         )},
        id: adapter_meta.outbox
      ),
      {ClusterMonitor, adapter_meta}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
