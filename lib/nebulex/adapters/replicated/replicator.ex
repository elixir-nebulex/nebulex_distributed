defmodule Nebulex.Adapters.Replicated.Replicator do
  @moduledoc false

  alias Nebulex.Adapters.Replicated
  alias Nebulex.Adapters.Replicated.VersionLog
  alias Nebulex.Distributed.{Cluster, RPC}
  alias Nebulex.Telemetry
  alias Tidefall.HashMap, as: HM
  alias Tidefall.HashMap.Entry

  # Namespace of the buffer keys of TTL changes.
  @ttl_tag :"$nbx_replicated_ttl"

  # Buffer key of a full flush.
  @flush_key :"$nbx_replicated_flush"

  ## API

  @doc false
  def buffer_key(key, command, buffer_opts)

  # A TTL change is buffered under its own key, so it cannot replace a put
  # of the same key in the buffer. The routing takes the hash the buffer
  # uses for the user key, so both land in the same partition, here and on
  # the peers, and are processed in the order they were written.
  def buffer_key(key, {op, _args}, buffer_opts) when op in [:touch, :expire] do
    {{@ttl_tag, :erlang.phash2(key), key}, ttl_buffer_opts(buffer_opts)}
  end

  def buffer_key(_key, {:delete_all, _args}, buffer_opts) do
    {@flush_key, buffer_opts}
  end

  def buffer_key(key, _command, buffer_opts) do
    {key, buffer_opts}
  end

  @doc false
  def stream_entries(adapter_meta) do
    # `telemetry: false` is set on every internal command so we don't emit a
    # span per entry — those spans copy `adapter_meta`/`args`/`result` into
    # event metadata and dominate GC pressure on large caches (see #15).
    adapter_meta
    |> Replicated.with_dynamic_cache(:stream!, [
      [select: {:key, :value}],
      [timeout: :infinity, telemetry: false]
    ])
    |> Stream.map(fn {key, value} ->
      case Replicated.with_dynamic_cache(adapter_meta, :ttl, [key, [telemetry: false]]) do
        {:ok, ttl} ->
          {key, {:put, [key, value, [ttl: ttl, telemetry: false]]}}

        _error ->
          nil
      end
    end)
    |> Stream.reject(&is_nil/1)
  end

  @doc false
  def push_entries(target_node, adapter_meta) do
    # Stream the local cache in chunks and ship one RPC per chunk so peak
    # heap on both sender and receiver stays bounded regardless of total
    # cache size. The target applies the entries as gap fills. The push
    # stops at the first chunk that fails: a target that does not answer
    # would cost one timeout per chunk, and the anti-entropy catch-up fills
    # what is left.
    adapter_meta
    |> stream_entries()
    |> Stream.chunk_every(adapter_meta.bootstrap_chunk_size)
    |> Enum.reduce_while({0, []}, fn chunk, {total, []} ->
      bootstrap_chunk =
        Enum.map(chunk, fn {key, {:put, args}} -> {key, {:put_new, args}} end)

      case RPC.call(
             target_node,
             __MODULE__,
             :apply_bootstrap_entries,
             [adapter_meta, bootstrap_chunk],
             adapter_meta.replication_timeout
           ) do
        :ok -> {:cont, {total + length(bootstrap_chunk), []}}
        error -> {:halt, {total, [{error, target_node}]}}
      end
    end)
  end

  @doc false
  def apply_bootstrap_entries(adapter_meta, entries) do
    Enum.each(entries, fn {_key, {:put_new, [key, value, opts]}} ->
      :ok = gap_fill(adapter_meta, key, value, opts)
    end)
  end

  @doc false
  def process_inbox(batch, adapter_meta) when is_list(batch) do
    # A TTL change applies to the value written by the latest put or delete,
    # so the TTL changes of a batch go after its other changes.
    {ttl_changes, changes} = Enum.split_with(batch, &ttl_change?/1)

    stale =
      Enum.reduce(changes ++ ttl_changes, 0, fn entry, acc ->
        acc + apply_entry(entry, adapter_meta)
      end)

    if stale > 0 do
      Telemetry.execute(
        adapter_meta.telemetry_prefix ++ [:replication, :stale],
        %{count: stale},
        %{adapter_meta: adapter_meta, node: node()}
      )
    end

    :ok
  end

  @doc false
  def process_outbox(batch, adapter_meta) when is_list(batch) do
    with [_ | _] = peers <-
           adapter_meta.pg_group
           |> Cluster.pg_nodes()
           |> List.delete(node()) do
      # Tag entries as :remote for peer inboxes
      remote_entries =
        Enum.map(batch, fn %Entry{key: key, value: command, version: version} ->
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

  ## Apply path

  # Every change from a peer is applied here, and only when its version is
  # newer than the one the version log holds for the key (and than the
  # flush barrier). Each function returns the number of stale commands it
  # dropped.

  defp ttl_change?(%Entry{value: {{op, _args}, _origin}}), do: op in [:touch, :expire]

  # A local change is already in the primary cache. Recording it is how the
  # log learns about local writes.
  defp apply_entry(%Entry{value: {command, :local}, version: version}, adapter_meta) do
    _ignore = record_local(command, version, adapter_meta.version_log)

    0
  end

  defp apply_entry(%Entry{value: {command, :remote}, version: version}, adapter_meta) do
    apply_remote(command, version, adapter_meta)
  end

  defp record_local({:put, [key | _]}, version, log) do
    VersionLog.record(log, key, version, :put)
  end

  defp record_local({:delete, [key | _]}, version, log) do
    VersionLog.record(log, key, version, :delete)
  end

  defp record_local({:delete_all, [query, _opts]}, version, log) do
    flush?(query) and VersionLog.put_barrier(log, version)
  end

  defp record_local(_ttl_change, _version, _log) do
    false
  end

  defp apply_remote({:put, [key | _] = args}, version, %{version_log: log} = adapter_meta) do
    if VersionLog.record(log, key, version, :put) do
      _ = Replicated.with_dynamic_cache(adapter_meta, :put, args)

      # A flush processed by another partition in the meantime may have
      # scanned the cache before this write landed.
      :ok = maybe_flushed(adapter_meta, key, version)

      0
    else
      1
    end
  end

  defp apply_remote({:delete, [key | _] = args}, version, %{version_log: log} = adapter_meta) do
    if VersionLog.record(log, key, version, :delete) do
      _ = Replicated.with_dynamic_cache(adapter_meta, :delete, args)

      0
    else
      1
    end
  end

  defp apply_remote({op, [key | _] = args}, version, %{version_log: log} = adapter_meta)
       when op in [:touch, :expire] do
    if VersionLog.newer?(log, key, version) do
      _ = Replicated.with_dynamic_cache(adapter_meta, op, args)

      0
    else
      1
    end
  end

  defp apply_remote({:delete_all, [query, opts]}, version, adapter_meta) do
    if flush?(query) do
      apply_flush(query, opts, version, adapter_meta)
    else
      apply_query_delete(query, opts, version, adapter_meta)
    end
  end

  # A flush removes every entry except the keys changed after it.
  defp apply_flush(query, opts, version, %{version_log: log} = adapter_meta) do
    if VersionLog.put_barrier(log, version) do
      case VersionLog.puts_newer_than(log, version) do
        [] ->
          _ = Replicated.with_dynamic_cache(adapter_meta, :delete_all, [query, opts])

        _keep ->
          # The log is checked again for each chunk, right before deleting
          # it, so a put another partition applies meanwhile is kept.
          adapter_meta
          |> Replicated.with_dynamic_cache(:stream!, [
            [select: :key],
            [timeout: :infinity, telemetry: false]
          ])
          |> Enum.chunk_every(adapter_meta.bootstrap_chunk_size)
          |> Enum.each(&delete_unchanged(adapter_meta, &1, version, opts))
      end

      0
    else
      1
    end
  end

  defp delete_unchanged(%{version_log: log} = adapter_meta, keys, version, opts) do
    keys = Enum.reject(keys, &changed_after?(log, &1, version))

    Replicated.with_dynamic_cache(adapter_meta, :delete_all, [[in: keys], opts])
  end

  defp changed_after?(log, key, version) do
    case VersionLog.lookup(log, key) do
      {logged, _kind} -> logged > version
      nil -> false
    end
  end

  # Nodes on a previous release replicate a `delete_all` with a query as is.
  # It becomes one delete per matching key, so each one follows the rule.
  defp apply_query_delete(query, opts, version, adapter_meta) do
    adapter_meta
    |> Replicated.matching_keys(query)
    |> Enum.reduce(0, fn key, acc ->
      acc + apply_remote({:delete, [key, opts]}, version, adapter_meta)
    end)
  end

  defp maybe_flushed(%{version_log: log} = adapter_meta, key, version) do
    case VersionLog.barrier(log) do
      barrier when is_integer(barrier) and barrier > version ->
        _ = Replicated.with_dynamic_cache(adapter_meta, :delete, [key, [telemetry: false]])

        :ok

      _else ->
        :ok
    end
  end

  defp flush?(query), do: match?([{:query, nil} | _], query)

  # A gap fill only writes a key this node knows nothing about: no logged
  # change, no flush within the window, and no value. Any change wins over
  # it, so a change applied while the gap fill was being written undoes it.
  defp gap_fill(%{version_log: log} = adapter_meta, key, value, opts) do
    with nil <- VersionLog.lookup(log, key),
         nil <- VersionLog.barrier(log),
         {:ok, true} <- Replicated.with_dynamic_cache(adapter_meta, :put_new, [key, value, opts]) do
      undo_gap_fill(adapter_meta, key)
    end

    :ok
  end

  defp undo_gap_fill(%{version_log: log} = adapter_meta, key) do
    if changed_meanwhile?(log, key) do
      Replicated.with_dynamic_cache(adapter_meta, :delete, [key, [telemetry: false]])
    end
  end

  defp changed_meanwhile?(log, key) do
    case VersionLog.lookup(log, key) do
      {_version, kind} -> kind == :delete
      nil -> is_integer(VersionLog.barrier(log))
    end
  end

  defp multicall(peers, entries, adapter_meta) do
    event = adapter_meta.telemetry_prefix ++ [:replication]
    metadata = %{adapter_meta: adapter_meta, node: node(), peers: peers}

    # The TTL changes go in a second call, after the other changes, because
    # they are routed differently.
    {ttl_changes, changes} =
      Enum.split_with(entries, &match?({{@ttl_tag, _hash, _key}, _value, _version}, &1))

    Telemetry.span(event, metadata, fn ->
      result =
        [
          {changes, adapter_meta.rt_buffer_opts},
          {ttl_changes, ttl_buffer_opts(adapter_meta.rt_buffer_opts)}
        ]
        |> Enum.reject(&match?({[], _opts}, &1))
        |> Enum.reduce({[], []}, fn {group, opts}, {ok_acc, errors_acc} ->
          {ok, errors} =
            RPC.multicall(
              peers,
              HM,
              :put_all_newer,
              [adapter_meta.inbox, group, opts],
              adapter_meta.replication_timeout
            )

          {ok_acc ++ ok, errors_acc ++ errors}
        end)
        |> then(fn {ok, errors} -> {ok, Enum.uniq_by(errors, &elem(&1, 1))} end)

      {result, Map.put(metadata, :errors, elem(result, 1))}
    end)
  end

  # The routing function travels with the batch to the peers, and
  # `Kernel.elem/2` is there on every node, including the ones on a
  # previous release.
  defp ttl_buffer_opts(buffer_opts) do
    [partition_key: {Kernel, :elem, [1]}] ++ buffer_opts
  end
end
