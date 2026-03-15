defmodule Nebulex.Adapters.Replicated.Replicator do
  @moduledoc false

  alias Nebulex.Adapters.Replicated
  alias Nebulex.Distributed.{Cluster, RPC}
  alias Nebulex.Telemetry

  ## API

  @doc false
  def stream_entries(adapter_meta) do
    adapter_meta
    |> Replicated.with_dynamic_cache(:stream!, [
      [select: {:key, :value}],
      [timeout: :infinity]
    ])
    |> Stream.map(fn {key, value} ->
      case Replicated.with_dynamic_cache(adapter_meta, :ttl, [key, []]) do
        {:ok, ttl} ->
          {key, {:put, [key, value, [ttl: ttl]]}}

        _error ->
          nil
      end
    end)
    |> Stream.reject(&is_nil/1)
    |> Enum.to_list()
  end

  @doc false
  def copy_entries(node, adapter_meta) do
    RPC.call(
      node,
      __MODULE__,
      :stream_entries,
      [adapter_meta],
      adapter_meta.replication_timeout
    )
    |> case do
      [] ->
        0

      entries ->
        version = System.monotonic_time()

        inbox_entries =
          Enum.map(entries, fn {key, command} ->
            {key, {command, :remote}, version}
          end)

        :ok = PartitionedBuffer.Map.put_all_newer(adapter_meta.inbox, inbox_entries)

        Enum.count(entries)
    end
  end

  @doc false
  def process_inbox(batch, adapter_meta) when is_list(batch) do
    Enum.each(batch, fn
      {_key, {_command, :local}, _version, _updates} ->
        :ok

      {_key, {{op, args}, :remote}, _version, _updates} ->
        Replicated.with_dynamic_cache(adapter_meta, op, args)
    end)
  end

  @doc false
  def process_outbox(batch, adapter_meta) when is_list(batch) do
    with [_ | _] = peers <-
           adapter_meta.pg_group
           |> Cluster.pg_nodes()
           |> List.delete(node()) do
      # Tag entries as :remote for peer inboxes
      remote_entries =
        Enum.map(batch, fn {key, command, version, _updates} ->
          {key, {command, :remote}, version}
        end)

      replicate_to_peers(
        peers,
        remote_entries,
        adapter_meta,
        adapter_meta.replication_retries
      )
    end
  end

  @doc false
  def replicate_to_peers(peers, entries, adapter_meta, retries_left) do
    case multicall(peers, entries, adapter_meta) do
      {_ok, [_ | _] = errors} when retries_left > 0 ->
        :ok = Process.sleep(adapter_meta.replication_retry_delay)

        failed_nodes = Enum.map(errors, fn {_error, node} -> node end)

        replicate_to_peers(
          failed_nodes,
          entries,
          adapter_meta,
          retries_left - 1
        )

      {_ok, _errors} ->
        :ok
    end
  end

  defp multicall(peers, entries, adapter_meta) do
    event = adapter_meta.telemetry_prefix ++ [:replication]
    metadata = %{adapter_meta: adapter_meta, node: node(), peers: peers}

    Telemetry.span(event, metadata, fn ->
      RPC.multicall(
        peers,
        PartitionedBuffer.Map,
        :put_all_newer,
        [adapter_meta.inbox, entries],
        adapter_meta.replication_timeout
      )
      |> case do
        {_ok, []} = result ->
          {result, Map.put(metadata, :errors, [])}

        {_ok, errors} = result ->
          {result, Map.put(metadata, :errors, errors)}
      end
    end)
  end
end
