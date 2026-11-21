defmodule Nebulex.Adapters.ReplicatedApplyTest do
  # Covers the apply path of the replicated adapter on one node: commands are
  # delivered to the inbox processor with chosen versions, in a chosen order
  # and grouping (each call to `process_inbox/2` is one processing tick), and
  # the test reads the cache. The cache runs with a long interval, so its own
  # ticks never run during a test.
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Mimic, only: [expect: 3, stub: 3]
  import Nebulex.CacheCase, only: [safe_stop: 1, with_telemetry_handler: 3, wait_until: 1]

  alias Nebulex.Adapter
  alias Nebulex.Adapters.Replicated.{Replicator, VersionLog}
  alias Nebulex.Distributed.TestCache.ReplicatedCache, as: Cache
  alias Nebulex.Telemetry
  alias Tidefall.HashMap.Entry

  @moduletag capture_log: true

  @flush_query [query: nil, select: {:key, :value}]
  @stale Telemetry.default_prefix(Cache) ++ [:replication, :stale]

  setup context do
    {name, meta} = start_cache(replication: [interval: :timer.minutes(1)])

    default_dynamic_cache = Cache.get_dynamic_cache()
    _ = Cache.put_dynamic_cache(name)

    on_exit(fn -> Cache.put_dynamic_cache(default_dynamic_cache) end)

    {:ok, meta: meta, test: context.test}
  end

  describe "version rule" do
    test "an older put after the tick that processed a newer local write is dropped", %{
      meta: meta
    } do
      :ok = Cache.put(:k, "local")

      deliver(meta, [put(:k, "local", 200, :local)])
      deliver(meta, [put(:k, "old", 100)])

      assert Cache.get!(:k) == "local"
    end

    test "an older put after the tick that processed a newer local delete is dropped", %{
      meta: meta
    } do
      :ok = Cache.put(:k, "v")
      :ok = Cache.delete(:k)

      deliver(meta, [delete(:k, 200, :local)])
      deliver(meta, [put(:k, "old", 100)])

      assert Cache.has_key?(:k) == {:ok, false}
    end

    test "an older delete after the tick that processed a newer put is dropped", %{meta: meta} do
      deliver(meta, [put(:k, "new", 200)])
      deliver(meta, [delete(:k, 100)])

      assert Cache.get!(:k) == "new"
    end

    test "a newer change from a peer is applied", %{meta: meta} do
      :ok = Cache.put(:k, "local")

      deliver(meta, [put(:k, "local", 100, :local)])
      deliver(meta, [put(:k, "remote", 200)])

      assert Cache.get!(:k) == "remote"

      deliver(meta, [delete(:k, 300)])

      assert Cache.has_key?(:k) == {:ok, false}
    end

    test "a command delivered twice changes nothing the second time", %{meta: meta} do
      deliver(meta, [put(:k, "a", 100)])
      deliver(meta, [put(:k, "b", 200)])
      deliver(meta, [put(:k, "a", 100)])

      assert Cache.get!(:k) == "b"

      deliver(meta, [delete(:k, 300)])
      :ok = Cache.put(:k, "c")
      deliver(meta, [put(:k, "c", 400, :local)])
      deliver(meta, [delete(:k, 300)])

      assert Cache.get!(:k) == "c"
    end

    test "a TTL change older than the last put is dropped", %{meta: meta} do
      deliver(meta, [put(:k, "v", 200)])
      deliver(meta, [{:expire, [:k, :timer.hours(1), []]} |> entry(:remote, 100)])
      deliver(meta, [{:touch, [:k, []]} |> entry(:remote, 100)])

      assert Cache.ttl!(:k) == :infinity
    end

    test "a TTL change goes after the put of the same batch", %{meta: meta} do
      deliver(meta, [
        {:expire, [:k, :timer.hours(1), []]} |> entry(:remote, 200),
        put(:k, "v", 100)
      ])

      assert Cache.get!(:k) == "v"
      assert Cache.ttl!(:k) <= :timer.hours(1)
    end

    test "a local flush keeps working when the log is gone" do
      stub(VersionLog, :put_barrier, fn _log, _version -> raise ArgumentError end)

      :ok = Cache.put(:k, "v")

      assert Cache.delete_all!() == 1
    end

    test "a local write keeps working when the log is gone" do
      stub(VersionLog, :next_version, fn _log, _key -> raise ArgumentError end)

      assert Cache.put(:k, "v") == :ok
      assert Cache.get!(:k) == "v"
    end
  end

  describe "flush" do
    test "a late command older than a flush is dropped", %{meta: meta} do
      deliver(meta, [flush(200)])
      deliver(meta, [put(:k, "old", 100)])

      assert Cache.has_key?(:k) == {:ok, false}
    end

    test "a key written after a flush survives it", %{meta: meta} do
      :ok = Cache.put(:unlogged, "v")
      deliver(meta, [put(:old, "old", 100), put(:new, "new", 300)])
      deliver(meta, [flush(200)])

      assert Cache.has_key?(:unlogged) == {:ok, false}
      assert Cache.has_key?(:old) == {:ok, false}
      assert Cache.get!(:new) == "new"
    end

    test "a flush older than the barrier is dropped", %{meta: meta} do
      deliver(meta, [flush(200)])
      deliver(meta, [put(:k, "v", 300)])
      deliver(meta, [flush(100)])

      assert Cache.get!(:k) == "v"
    end

    test "a local flush drops older puts before its marker is processed", %{meta: meta} do
      :ok = Cache.put(:k, "v")
      assert Cache.delete_all!() == 1

      deliver(meta, [put(:k, "old", System.system_time() - 1_000_000)])

      assert Cache.has_key?(:k) == {:ok, false}
    end

    test "a local flush sets the barrier", %{meta: meta} do
      :ok = Cache.put(:k, "v")
      assert Cache.delete_all!() == 1

      deliver(meta, [{:delete_all, [@flush_query, []]} |> entry(:local, 200)])
      deliver(meta, [put(:k, "old", 100)])

      assert Cache.has_key?(:k) == {:ok, false}
    end

    test "a put that lands after a flush scanned the cache is removed", %{meta: meta} do
      stub(VersionLog, :barrier, fn _log -> 300 end)

      deliver(meta, [put(:k, "v", 200)])

      assert Cache.has_key?(:k) == {:ok, false}
    end
  end

  describe "delete_all with a query" do
    test "from a previous release is applied per key", %{meta: meta} do
      :ok = Cache.put_all(a1: 1, a2: 2, b1: 3)

      deliver(meta, [put(:a2, 20, 300)])
      deliver(meta, [{:delete_all, [[query: big_values_ms(), select: :key], []]} |> entry(200)])

      assert Cache.get_all!(in: [:a1, :a2, :b1]) |> Map.new() == %{a1: 1, a2: 20}
    end

    test "resolves the keys through the primary adapter for other queries" do
      :ok = Cache.put(:k, "v", ttl: 1)

      wait_until(fn -> assert Cache.count_all!(query: :expired) == 1 end)

      # The local adapter does not count expired entries it deletes.
      assert Cache.delete_all!(query: :expired) == 0
      assert Cache.count_all!(query: :expired) == 0
    end
  end

  describe "bootstrap entries" do
    test "fill a key the node knows nothing about", %{meta: meta} do
      bootstrap(meta, [{:k, "v", [ttl: :timer.hours(1)]}])

      assert Cache.get!(:k) == "v"
      assert Cache.ttl!(:k) <= :timer.hours(1)
    end

    test "do not bring back a key deleted here", %{meta: meta} do
      :ok = Cache.put(:k, "v")
      :ok = Cache.delete(:k)
      deliver(meta, [delete(:k, 200, :local)])

      bootstrap(meta, [{:k, "old", []}])

      assert Cache.has_key?(:k) == {:ok, false}
    end

    test "do not bring back a key a peer deleted", %{meta: meta} do
      deliver(meta, [put(:k, "v", 100)])
      deliver(meta, [delete(:k, 200)])

      bootstrap(meta, [{:k, "old", []}])

      assert Cache.has_key?(:k) == {:ok, false}
    end

    test "do not change a key changed here", %{meta: meta} do
      deliver(meta, [put(:k, "new", 200)])

      bootstrap(meta, [{:k, "old", []}])

      assert Cache.get!(:k) == "new"
    end

    test "are skipped after a flush within the window", %{meta: meta} do
      deliver(meta, [flush(200)])

      bootstrap(meta, [{:k, "old", []}])

      assert Cache.has_key?(:k) == {:ok, false}
    end

    test "are undone by a delete applied meanwhile", %{meta: meta} do
      VersionLog
      |> expect(:lookup, fn _log, :k -> nil end)
      |> expect(:lookup, fn _log, :k -> {200, :delete} end)

      bootstrap(meta, [{:k, "old", []}])

      assert Cache.has_key?(:k) == {:ok, false}
    end

    test "are undone by a flush applied meanwhile", %{meta: meta} do
      VersionLog
      |> expect(:barrier, fn _log -> nil end)
      |> expect(:barrier, fn _log -> 200 end)

      bootstrap(meta, [{:k, "old", []}])

      assert Cache.has_key?(:k) == {:ok, false}
    end
  end

  describe "stale event" do
    test "reports the number of commands dropped in a batch", %{meta: meta} do
      deliver(meta, [put(:k1, "new", 200), put(:k2, "new", 200)])

      with_telemetry_handler __MODULE__, [@stale], fn ->
        deliver(meta, [put(:k1, "old", 100), delete(:k2, 100), put(:k3, "v", 100)])

        assert_receive {@stale, %{count: 2}, %{adapter_meta: %{name: _}, node: node}}
        assert node == node()

        deliver(meta, [put(:k4, "v", 100)])

        refute_receive {@stale, _, _}, 100
      end
    end
  end

  describe "log window" do
    test "an entry is kept for one rotation and dropped at the next one", %{meta: meta} do
      deliver(meta, [put(:k, "new", 200)])

      rotate(meta)
      deliver(meta, [put(:k, "old", 100)])

      assert Cache.get!(:k) == "new"

      rotate(meta)
      deliver(meta, [put(:k, "old", 100)])

      assert Cache.get!(:k) == "old"
    end

    test "an entry expires after the window" do
      {name, meta} = start_cache(replication: [interval: 10, timeout: 1, retries: 0])

      Cache.with_dynamic_cache(name, fn ->
        deliver(meta, [put(:k, "new", 200)])

        # The older put is dropped until the entry for the key expires.
        wait_until(fn ->
          deliver(meta, [put(:k, "old", 100)])

          assert Cache.get!(:k) == "old"
        end)
      end)
    end

    test "the barrier expires after the window" do
      {name, meta} = start_cache(replication: [interval: 10, timeout: 1, retries: 0])

      Cache.with_dynamic_cache(name, fn ->
        deliver(meta, [flush(200)])

        wait_until(fn ->
          deliver(meta, [put(:k, "old", 100)])

          assert Cache.get!(:k) == "old"
        end)
      end)
    end
  end

  describe "convergence" do
    property "caches that receive the same commands in any order end equal to the model" do
      check all(
              commands <- commands(),
              deliveries <- list_of(delivery(length(commands)), length: 3),
              max_runs: 50
            ) do
        expected = model(commands)

        for delivery <- deliveries do
          {name, meta} = start_cache(replication: [interval: :timer.minutes(1)])

          for batch <- arrange(commands, delivery) do
            :ok = Replicator.process_inbox(batch, meta)
          end

          result = Cache.with_dynamic_cache(name, fn -> Cache.get_all!(in: keys()) end)

          assert Map.new(result) == expected

          :ok = Cache.stop(name, [])
        end
      end
    end
  end

  ## Generators and model

  defp keys, do: [:a, :b, :c, :d]

  defp commands do
    gen all(
          ops <- list_of(op(), min_length: 1, max_length: 30),
          versions <- uniq_list_of(integer(1..1_000_000), length: length(ops))
        ) do
      Enum.zip_with(ops, versions, &entry(&1, :remote, &2))
    end
  end

  defp op do
    key = member_of(keys())

    one_of([
      map({key, integer()}, fn {k, v} -> {:put, [k, v, []]} end),
      map(key, &{:delete, [&1, []]}),
      map(key, &{:touch, [&1, []]}),
      map(key, &{:expire, [&1, :infinity, []]}),
      constant({:delete_all, [@flush_query, []]})
    ])
  end

  # For one cache: the order of the commands, which ones are delivered twice,
  # and the sizes of the batches.
  defp delivery(count) do
    tuple({
      list_of(integer(), length: count),
      list_of(boolean(), length: count),
      list_of(integer(1..5), min_length: 1)
    })
  end

  defp arrange(commands, {order, duplicates, sizes}) do
    commands
    |> Enum.zip(duplicates)
    |> Enum.flat_map(fn {command, dup?} -> if dup?, do: [command, command], else: [command] end)
    |> Enum.zip(Stream.cycle(order))
    |> Enum.sort_by(&elem(&1, 1))
    |> Enum.map(&elem(&1, 0))
    |> chunk(Stream.cycle(sizes))
  end

  defp chunk([], _sizes), do: []

  defp chunk(list, sizes) do
    [size] = Enum.take(sizes, 1)
    {batch, rest} = Enum.split(list, size)

    [batch | chunk(rest, Stream.drop(sizes, 1))]
  end

  # Per key, the value of the highest-version put, or absent when the highest
  # is a delete or the key is older than the latest flush.
  defp model(commands) do
    flush =
      commands
      |> Enum.filter(&match?(%Entry{value: {{:delete_all, _}, _}}, &1))
      |> Enum.map(& &1.version)
      |> Enum.max(fn -> 0 end)

    commands
    |> Enum.filter(fn %Entry{value: {{op, _args}, _origin}, version: version} ->
      op in [:put, :delete] and version > flush
    end)
    |> Enum.group_by(fn %Entry{value: {{_op, [key | _]}, _}} -> key end)
    |> Enum.flat_map(fn {key, changes} ->
      case Enum.max_by(changes, & &1.version) do
        %Entry{value: {{:put, [_key, value, _opts]}, _}} -> [{key, value}]
        _delete -> []
      end
    end)
    |> Map.new()
  end

  ## Helpers

  defp start_cache(opts) do
    name = :"replicated_apply_#{System.unique_integer([:positive])}"
    {:ok, pid} = Cache.start_link([name: name] ++ opts)

    on_exit(fn -> safe_stop(pid) end)

    {name, Adapter.lookup_meta(name)}
  end

  # Rotates the log of the cache as its own timer does, and waits for it.
  defp rotate(%{name: name}) do
    {_id, pid, _type, _modules} =
      name
      |> Process.whereis()
      |> Supervisor.which_children()
      |> Enum.flat_map(fn {_id, sup, :supervisor, _} -> Supervisor.which_children(sup) end)
      |> List.keyfind(VersionLog, 0)

    send(pid, :rotate)

    _ = :sys.get_state(pid)
  end

  defp deliver(meta, batch) do
    :ok = Replicator.process_inbox(batch, meta)
  end

  defp bootstrap(meta, entries) do
    entries = for {key, value, opts} <- entries, do: {key, {:put_new, [key, value, opts]}}

    :ok = Replicator.apply_bootstrap_entries(meta, entries)
  end

  defp put(key, value, version, origin \\ :remote) do
    entry({:put, [key, value, []]}, origin, version)
  end

  defp delete(key, version, origin \\ :remote) do
    entry({:delete, [key, []]}, origin, version)
  end

  defp flush(version) do
    entry({:delete_all, [@flush_query, []]}, :remote, version)
  end

  defp entry({_op, [key | _]} = command, origin \\ :remote, version) do
    %Entry{key: key, value: {command, origin}, version: version, updates: 0}
  end

  # Matches the entries whose value is greater than 1.
  defp big_values_ms do
    [{{:entry, :"$1", :"$2", :_, :_, :_}, [{:>, :"$2", 1}], [true]}]
  end
end
