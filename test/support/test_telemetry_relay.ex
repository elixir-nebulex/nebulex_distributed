defmodule Nebulex.TestTelemetryRelay do
  @moduledoc false

  # Telemetry handler that forwards events to a pid. Attached on peer nodes via RPC so a
  # test on the primary node can observe spans emitted elsewhere in the cluster.

  @doc false
  @spec handle([atom()], map(), map(), %{pid: pid()}) :: {[atom()], map(), map()}
  def handle(event, measurements, metadata, %{pid: pid}) do
    send(pid, {event, measurements, metadata})
  end
end
