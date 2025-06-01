defmodule Nebulex.Distributed.Application do
  @moduledoc false

  use Application

  alias Nebulex.Distributed.Cluster

  @impl true
  def start(_type, _args) do
    children = [
      Cluster.pg_child_spec()
    ]

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Nebulex.Distributed.Supervisor
    )
  end
end
