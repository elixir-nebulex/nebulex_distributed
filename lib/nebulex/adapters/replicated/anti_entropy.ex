defmodule Nebulex.Adapters.Replicated.AntiEntropy do
  @moduledoc false

  use GenServer

  import Nebulex.Utils

  alias Nebulex.Adapters.Replicated
  alias Nebulex.Distributed.{Cluster, RPC}
  alias Nebulex.Telemetry

  # State
  defstruct [:adapter_meta, :interval, :timer_ref]

  # Number of fixed buckets for the hash digest. Each bucket stores the XOR
  # of hash(key, value_hash) for all keys that fall into it. 1024 gives a
  # good balance between digest size (~4 KB) and divergence granularity.
  @buckets 1024

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
    timer_ref = schedule_cycle(interval)

    state = %__MODULE__{
      adapter_meta: adapter_meta,
      interval: interval,
      timer_ref: timer_ref
    }

    {:ok, state}
  end

  @impl true
  def handle_info(:run, %__MODULE__{adapter_meta: adapter_meta, interval: interval} = state) do
    peers = get_peers(adapter_meta.pg_group)

    if peers != [] do
      peer = Enum.random(peers)

      reconcile(peer, adapter_meta)
    end

    timer_ref = schedule_cycle(interval)

    {:noreply, %{state | timer_ref: timer_ref}}
  rescue
    # coveralls-ignore-start
    _error -> {:noreply, %{state | timer_ref: schedule_cycle(interval)}}
  catch
    :exit, _reason ->
      {:noreply, %{state | timer_ref: schedule_cycle(interval)}}
      # coveralls-ignore-stop
  end

  ## Public functions (called locally and via RPC)

  @doc false
  def build_buckets(adapter_meta) do
    adapter_meta
    |> Replicated.with_dynamic_cache(:stream!, [
      [select: {:key, :value}],
      [timeout: :infinity, telemetry: false]
    ])
    |> Enum.reduce(:array.new(@buckets, default: 0), fn {key, value}, acc ->
      idx = :erlang.phash2(key, @buckets)
      hash = :erlang.phash2({key, :erlang.phash2(value)})
      current = :array.get(idx, acc)

      :array.set(idx, Bitwise.bxor(current, hash), acc)
    end)
    |> then(&for i <- 0..(@buckets - 1), do: :array.get(i, &1))
  end

  @doc false
  def entries_for_buckets(adapter_meta, bucket_indices) do
    bucket_set = MapSet.new(bucket_indices)

    adapter_meta
    |> Replicated.with_dynamic_cache(:stream!, [
      [select: {:key, :value}],
      [timeout: :infinity, telemetry: false]
    ])
    |> Stream.filter(fn {key, _value} ->
      MapSet.member?(bucket_set, :erlang.phash2(key, @buckets))
    end)
    |> Stream.map(fn {key, value} ->
      case Replicated.with_dynamic_cache(adapter_meta, :ttl, [key, [telemetry: false]]) do
        {:ok, ttl} -> {key, {:put, [key, value, [ttl: ttl, telemetry: false]]}}
        _error -> nil
      end
    end)
    |> Stream.reject(&is_nil/1)
    |> Enum.to_list()
  end

  ## Private Functions

  defp get_peers(pg_group) do
    pg_group
    |> Cluster.pg_nodes()
    |> List.delete(node())
  end

  defp reconcile(peer, adapter_meta) do
    event = adapter_meta.telemetry_prefix ++ [:anti_entropy]

    metadata = %{
      adapter_meta: adapter_meta,
      node: node(),
      peer: peer
    }

    Telemetry.span(event, metadata, fn ->
      {repaired, divergent_buckets} = do_reconcile(peer, adapter_meta)

      {repaired, Map.merge(metadata, %{repaired: repaired, divergent_buckets: divergent_buckets})}
    end)
  end

  @doc false
  def do_reconcile(
        peer,
        %{
          replication_timeout: replication_timeout,
          inbox: inbox
        } = adapter_meta
      ) do
    # Build local bucket hashes
    local_buckets = build_buckets(adapter_meta)

    # Get peer's bucket hashes via RPC
    peer_buckets =
      RPC.call(
        peer,
        __MODULE__,
        :build_buckets,
        [adapter_meta],
        replication_timeout
      )

    # Find divergent bucket indices
    divergent =
      local_buckets
      |> Enum.zip(peer_buckets)
      |> Enum.with_index()
      |> Enum.filter(fn {{local, remote}, _idx} -> local != remote end)
      |> Enum.map(fn {_, idx} -> idx end)

    case divergent do
      [] ->
        {0, 0}

      _ ->
        # Fetch entries for divergent buckets from peer
        peer_entries =
          RPC.call(
            peer,
            __MODULE__,
            :entries_for_buckets,
            [adapter_meta, divergent],
            replication_timeout
          )

        # Write to inbox — "newer version wins" handles conflicts
        version = System.system_time()

        inbox_entries =
          Enum.map(peer_entries, fn {key, command} ->
            {key, {command, :remote}, version}
          end)

        :ok = PartitionedBuffer.Map.put_all_newer(inbox, inbox_entries)

        {Enum.count(peer_entries), Enum.count(divergent)}
    end
  end

  defp schedule_cycle(interval) do
    Process.send_after(self(), :run, interval)
  end
end
