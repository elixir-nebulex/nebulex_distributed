defmodule Nebulex.Adapters.Replicated.AntiEntropy do
  @moduledoc false

  # Anti-entropy never undoes a write or a delete: when it cannot tell which
  # side is right, it does nothing.
  #
  # A normal cycle (steady state) compares the version logs of this node and
  # a random peer, and takes the peer's changes that are newer than the ones
  # logged here, with their real versions, through the inbox. A node that
  # logged a change and later evicted the entry does not take it back,
  # because the peer offers the same version.
  #
  # A catch-up pass compares the whole caches and fills the keys this node
  # knows nothing about, as gap fills. It runs when the node starts and when
  # a partition heals, and stops after a pass that repairs nothing or after
  # `@catch_up_passes` passes, so it does not keep refilling evicted entries.

  use GenServer

  import Nebulex.Utils

  alias Nebulex.Adapters.Replicated
  alias Nebulex.Adapters.Replicated.{Replicator, VersionLog}
  alias Nebulex.Distributed.{Cluster, RPC}
  alias Nebulex.Telemetry
  alias Tidefall.HashMap, as: HM

  # State
  defstruct [:adapter_meta, :interval, :timer_ref, :pg_ref, catch_up: 0, departed: MapSet.new()]

  # Number of fixed buckets for the hash digest. Each bucket stores the XOR
  # of hash(key, value_hash) for all keys that fall into it. 1024 gives a
  # good balance between digest size (~4 KB) and divergence granularity.
  @buckets 1024

  # Maximum number of catch-up passes in a row.
  @catch_up_passes 3

  ## API

  @doc false
  def start_link(%{name: name} = adapter_meta) do
    name = camelize_and_concat([name, AntiEntropy])

    GenServer.start_link(__MODULE__, adapter_meta, name: name)
  end

  ## GenServer Callbacks

  @impl true
  def init(adapter_meta) do
    interval = adapter_meta.anti_entropy_interval

    # The first cycle runs one interval after start, so the catch-up does
    # not overlap the bootstrap push this node is receiving.
    state = %__MODULE__{
      adapter_meta: adapter_meta,
      interval: interval,
      timer_ref: schedule_cycle(interval),
      pg_ref: Cluster.monitor_scope(),
      catch_up: @catch_up_passes
    }

    {:ok, state}
  end

  @impl true
  def handle_info(message, state)

  def handle_info(:run, %__MODULE__{adapter_meta: adapter_meta, catch_up: catch_up} = state) do
    catch_up =
      case get_peers(adapter_meta.pg_group) do
        [] ->
          catch_up

        peers ->
          mode = if catch_up > 0, do: :catch_up, else: :steady_state

          adapter_meta
          |> reconcile(Enum.random(peers), mode)
          |> next_catch_up(catch_up)
      end

    {:noreply, %{state | catch_up: catch_up, timer_ref: schedule_cycle(state.interval)}}
  rescue
    # A failed cycle is reported by the span; another peer is picked next
    # time. A failed catch-up pass still counts as one.
    _error ->
      {:noreply,
       %{state | catch_up: max(state.catch_up - 1, 0), timer_ref: schedule_cycle(state.interval)}}
  end

  # A peer process that leaves and comes back is a healed partition: this
  # node may have missed what was written on the other side. A peer that
  # comes back as a new process restarted, and catching up is its own job.
  def handle_info(
        {pg_ref, :leave, pg_group, pids},
        %__MODULE__{pg_ref: pg_ref, adapter_meta: %{pg_group: pg_group}} = state
      ) do
    departed =
      pids
      |> Enum.reject(&(node(&1) == node()))
      |> MapSet.new()
      |> MapSet.union(state.departed)

    {:noreply, %{state | departed: departed}}
  end

  def handle_info(
        {pg_ref, :join, pg_group, pids},
        %__MODULE__{pg_ref: pg_ref, adapter_meta: %{pg_group: pg_group}} = state
      ) do
    healed? = Enum.any?(pids, &MapSet.member?(state.departed, &1))
    joined_nodes = MapSet.new(pids, &node/1)
    departed = MapSet.reject(state.departed, &MapSet.member?(joined_nodes, node(&1)))
    catch_up = if healed?, do: @catch_up_passes, else: state.catch_up

    {:noreply, %{state | departed: departed, catch_up: catch_up}}
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  ## Steady state

  @doc false
  def steady_state(peer, adapter_meta, peer_meta \\ nil) do
    peer_meta = peer_meta || adapter_meta
    log = adapter_meta.version_log

    # Half the window, so a change is offered in at least two cycles and
    # neither node has already forgotten a change the other one offers.
    since =
      System.system_time() -
        System.convert_time_unit(div(adapter_meta.version_window, 2), :millisecond, :native)

    local = VersionLog.digest(log, since)
    remote = rpc!(peer, :log_digest, [peer_meta, since], adapter_meta)

    case divergent_buckets(local, remote) do
      [] ->
        {0, 0}

      divergent ->
        repaired =
          peer
          |> rpc!(:log_entries, [peer_meta, divergent, since], adapter_meta)
          |> Enum.filter(fn {key, version, _kind} -> VersionLog.newer?(log, key, version) end)
          |> Enum.map(&elem(&1, 0))
          |> Enum.chunk_every(adapter_meta.bootstrap_chunk_size)
          |> Enum.reduce(0, &(&2 + repair(peer, peer_meta, &1, adapter_meta)))

        {repaired, length(divergent)}
    end
  end

  @doc false
  def log_digest(%{version_log: log}, since) do
    VersionLog.digest(log, since)
  end

  @doc false
  def log_entries(%{version_log: log}, buckets, since) do
    VersionLog.entries(log, buckets, since)
  end

  # The logged version is read before the value, so the value is at least
  # as new as the version it is applied with.
  @doc false
  def log_values(%{version_log: log} = adapter_meta, keys) do
    Enum.flat_map(keys, &log_value(adapter_meta, &1, VersionLog.lookup(log, &1)))
  end

  defp log_value(_adapter_meta, key, {version, :delete}) do
    [{key, version, :delete}]
  end

  defp log_value(adapter_meta, key, {version, :put}) do
    for {^key, value, ttl} <- values(adapter_meta, [key]), do: {key, version, {:put, value, ttl}}
  end

  defp log_value(_adapter_meta, _key, nil) do
    []
  end

  # The peer's changes go through the inbox with their real versions, so
  # the version rule decides, as for any replicated change.
  defp repair(peer, peer_meta, keys, %{version_log: log} = adapter_meta) do
    entries =
      for {key, version, change} <- rpc!(peer, :log_values, [peer_meta, keys], adapter_meta),
          VersionLog.newer?(log, key, version) do
        {key, {repair_command(key, change), :remote}, version}
      end

    :ok = HM.put_all_newer(adapter_meta.inbox, entries, adapter_meta.rt_buffer_opts)

    length(entries)
  end

  defp repair_command(key, :delete) do
    {:delete, [key, [telemetry: false]]}
  end

  defp repair_command(key, {:put, value, ttl}) do
    {:put, [key, value, [ttl: ttl, telemetry: false]]}
  end

  ## Catch-up

  @doc false
  def catch_up(peer, adapter_meta, peer_meta \\ nil) do
    peer_meta = peer_meta || adapter_meta

    local = bucket_digest(adapter_meta)
    remote = rpc!(peer, :bucket_digest, [peer_meta], adapter_meta)

    divergent =
      for {bucket, {hash, count}} <- remote, hash_of(local, bucket) != hash, do: {bucket, count}

    repaired =
      divergent
      |> group_buckets(adapter_meta.bootstrap_chunk_size)
      |> Enum.reduce(0, fn buckets, acc ->
        peer
        |> rpc!(:bucket_keys, [peer_meta, buckets], adapter_meta)
        |> Enum.reject(&known?(adapter_meta, &1))
        |> Enum.chunk_every(adapter_meta.bootstrap_chunk_size)
        |> Enum.reduce(acc, fn keys, acc ->
          acc + fill(peer, peer_meta, keys, adapter_meta)
        end)
      end)

    {repaired, length(divergent)}
  end

  @doc false
  def bucket_digest(adapter_meta) do
    adapter_meta
    |> stream_entries()
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      hash = :erlang.phash2({key, :erlang.phash2(value)})

      Map.update(acc, :erlang.phash2(key, @buckets), {hash, 1}, fn {h, count} ->
        {Bitwise.bxor(h, hash), count + 1}
      end)
    end)
  end

  @doc false
  def bucket_keys(adapter_meta, buckets) do
    buckets = MapSet.new(buckets)

    adapter_meta
    |> Replicated.with_dynamic_cache(:stream!, [
      [select: :key],
      [timeout: :infinity, telemetry: false]
    ])
    |> Enum.filter(&MapSet.member?(buckets, :erlang.phash2(&1, @buckets)))
  end

  @doc false
  def values(adapter_meta, keys) do
    Enum.flat_map(keys, fn key ->
      with {:ok, value} <-
             Replicated.with_dynamic_cache(adapter_meta, :fetch, [key, [telemetry: false]]),
           {:ok, ttl} <-
             Replicated.with_dynamic_cache(adapter_meta, :ttl, [key, [telemetry: false]]) do
        [{key, value, ttl}]
      else
        _error -> []
      end
    end)
  end

  defp fill(peer, peer_meta, keys, adapter_meta) do
    entries =
      for {key, value, ttl} <- rpc!(peer, :values, [peer_meta, keys], adapter_meta) do
        {key, {:put_new, [key, value, [ttl: ttl, telemetry: false]]}}
      end

    :ok = Replicator.apply_bootstrap_entries(adapter_meta, entries)

    length(entries)
  end

  # Only keys this node has neither a value nor a logged change for are
  # fetched. The gap fill checks again when it writes.
  defp known?(%{version_log: log} = adapter_meta, key) do
    VersionLog.lookup(log, key) != nil or
      Replicated.with_dynamic_cache(adapter_meta, :has_key?, [key, [telemetry: false]]) ==
        {:ok, true}
  end

  defp hash_of(digest, bucket) do
    case Map.fetch(digest, bucket) do
      {:ok, {hash, _count}} -> hash
      :error -> 0
    end
  end

  # Groups the buckets so that each group holds about `size` keys on the
  # peer. A bucket with more keys than that makes a group of its own.
  defp group_buckets(buckets, size) do
    buckets
    |> Enum.chunk_while(
      {[], 0},
      fn {bucket, count}, {group, total} ->
        if group != [] and total + count > size,
          do: {:cont, group, {[bucket], count}},
          else: {:cont, {[bucket | group], total + count}}
      end,
      fn
        {[], 0} -> {:cont, {[], 0}}
        {group, _total} -> {:cont, group, {[], 0}}
      end
    )
  end

  ## Functions kept for nodes on the previous release, which call them on
  ## their peers during their cycle.

  @doc false
  def build_buckets(adapter_meta) do
    digest = bucket_digest(adapter_meta)

    for i <- 0..(@buckets - 1), do: hash_of(digest, i)
  end

  @doc false
  def entries_for_buckets(adapter_meta, bucket_indices) do
    keys = bucket_keys(adapter_meta, bucket_indices)

    for {key, value, ttl} <- values(adapter_meta, keys) do
      {key, {:put, [key, value, [ttl: ttl, telemetry: false]]}}
    end
  end

  ## Private Functions

  defp get_peers(pg_group) do
    pg_group
    |> Cluster.pg_nodes()
    |> List.delete(node())
  end

  defp reconcile(adapter_meta, peer, mode) do
    event = adapter_meta.telemetry_prefix ++ [:anti_entropy]

    metadata = %{
      adapter_meta: adapter_meta,
      node: node(),
      peer: peer,
      mode: mode
    }

    Telemetry.span(event, metadata, fn ->
      {repaired, divergent_buckets} =
        case mode do
          :steady_state -> steady_state(peer, adapter_meta)
          :catch_up -> catch_up(peer, adapter_meta)
        end

      {{mode, repaired},
       Map.merge(metadata, %{repaired: repaired, divergent_buckets: divergent_buckets})}
    end)
  end

  defp next_catch_up({:steady_state, _repaired}, catch_up), do: catch_up
  defp next_catch_up({:catch_up, 0}, _catch_up), do: 0
  defp next_catch_up({:catch_up, _repaired}, catch_up), do: catch_up - 1

  defp divergent_buckets(local, remote) do
    local
    |> Map.merge(remote, fn _bucket, l, r -> if l == r, do: :same, else: l end)
    |> Enum.reject(fn {_bucket, hash} -> hash == :same end)
    |> Enum.map(&elem(&1, 0))
  end

  defp stream_entries(adapter_meta) do
    Replicated.with_dynamic_cache(adapter_meta, :stream!, [
      [select: {:key, :value}],
      [timeout: :infinity, telemetry: false]
    ])
  end

  defp rpc!(peer, fun, args, adapter_meta) do
    case RPC.call(peer, __MODULE__, fun, args, adapter_meta.replication_timeout) do
      {:error, %Nebulex.Error{} = error} -> raise error
      result -> result
    end
  end

  defp schedule_cycle(interval) do
    Process.send_after(self(), :run, interval)
  end
end
