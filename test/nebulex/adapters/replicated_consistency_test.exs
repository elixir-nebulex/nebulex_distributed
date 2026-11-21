defmodule Nebulex.Adapters.ReplicatedConsistencyTest do
  # Covers what every node ends with when commands for a key cross processing
  # ticks, observed through the cache API on real peer nodes.
  use Nebulex.NodeCase, async: true

  import Nebulex.CacheCase, only: [wait_until: 1]

  alias Nebulex.Adapter
  alias Nebulex.Adapters.Replicated.Replicator
  alias Nebulex.Distributed.TestCache.ReplicatedCache, as: Cache
  alias Tidefall.HashMap.Entry

  @moduletag capture_log: true

  @name :replicated_consistency_cache

  setup do
    nodes = [node() | Node.list()]
    node_pid_list = start_caches(nodes, [{Cache, name: @name, replication: [interval: 100]}])

    wait_until(fn ->
      assert Cache.nodes(@name) |> Enum.sort() == Enum.sort(nodes)
    end)

    default_dynamic_cache = Cache.get_dynamic_cache()
    _ = Cache.put_dynamic_cache(@name)

    on_exit(fn ->
      _ = Cache.put_dynamic_cache(default_dynamic_cache)

      stop_caches(node_pid_list)
    end)

    {:ok, peers: Node.list()}
  end

  describe "local writes" do
    test "a write made while the clock is behind the logged version wins everywhere", %{
      peers: peers
    } do
      future = System.system_time() + System.convert_time_unit(3_600, :second, :native)
      command = %Entry{key: :k, value: {{:put, [:k, "future", []]}, :remote}, version: future}

      for node <- [node() | peers] do
        :ok =
          rpc(node, Replicator, :process_inbox, [
            [command],
            rpc(node, Adapter, :lookup_meta, [@name])
          ])
      end

      :ok = Cache.put(:k, "local")

      assert_peers(peers, :get!, [@name, :k, nil, []], &(&1 == "local"))
    end
  end

  describe "TTL changes" do
    setup %{peers: peers} do
      :ok = Cache.put(:k, "old")

      assert_peers(peers, :get!, [@name, :k, nil, []], &(&1 == "old"))

      :ok
    end

    test "put followed by touch delivers the new value", %{peers: peers} do
      :ok = Cache.put(:k, "new")
      assert Cache.touch(:k) == {:ok, true}

      assert_peers(peers, :get!, [@name, :k, nil, []], &(&1 == "new"))
    end

    test "put followed by expire delivers the new value and the TTL", %{peers: peers} do
      :ok = Cache.put(:k, "new")
      assert Cache.expire(:k, :timer.minutes(1)) == {:ok, true}

      assert_peers(peers, :get!, [@name, :k, nil, []], &(&1 == "new"))

      assert_peers(
        peers,
        :ttl!,
        [@name, :k, []],
        &(is_integer(&1) and &1 <= :timer.minutes(1))
      )
    end

    test "expire followed by put keeps the TTL of the put", %{peers: peers} do
      assert Cache.expire(:k, :timer.minutes(1)) == {:ok, true}
      :ok = Cache.put(:k, "new", ttl: :timer.hours(1))

      assert_peers(peers, :get!, [@name, :k, nil, []], &(&1 == "new"))

      assert_peers(peers, :ttl!, [@name, :k, []], fn ttl ->
        is_integer(ttl) and ttl > :timer.minutes(1)
      end)
    end
  end

  describe "delete_all" do
    setup %{peers: peers} do
      :ok = Cache.put_all(k1: 1, k2: 2, k3: 3, k4: 4)

      assert_peers(peers, :count_all!, [@name, [], []], &(&1 == 4))

      :ok
    end

    test "two calls with a query in one interval both take effect on the peers", %{
      peers: peers
    } do
      assert Cache.delete_all!(query: value_ms(1)) == 1
      assert Cache.delete_all!(query: value_ms(3)) == 1

      assert_peers(peers, :get_all!, [@name, [in: [:k1, :k2, :k3, :k4]], []], fn entries ->
        Map.new(entries) == %{k2: 2, k4: 4}
      end)
    end

    test "a flush made while the clock is behind the log removes every key", %{peers: peers} do
      future = System.system_time() + System.convert_time_unit(3_600, :second, :native)
      command = %Entry{key: :k1, value: {{:put, [:k1, 10, []]}, :remote}, version: future}

      for node <- [node() | peers] do
        :ok =
          rpc(node, Replicator, :process_inbox, [
            [command],
            rpc(node, Adapter, :lookup_meta, [@name])
          ])
      end

      assert Cache.delete_all!() == 4

      assert_peers(peers, :count_all!, [@name, [], []], &(&1 == 0))
    end
  end

  ## Helpers

  # Test closures cannot run on the peers, so the cache function is called
  # by name and `check` runs here on its result.
  defp assert_peers(peers, fun, args, check) do
    for node <- peers do
      wait_until(fn ->
        assert node |> rpc(Cache, fun, args) |> check.()
      end)
    end
  end

  # Matches the entries with the given value.
  defp value_ms(value) do
    [{{:entry, :_, :"$1", :_, :_, :_}, [{:==, :"$1", value}], [true]}]
  end
end
