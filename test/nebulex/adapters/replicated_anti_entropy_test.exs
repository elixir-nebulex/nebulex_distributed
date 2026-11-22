defmodule Nebulex.Adapters.ReplicatedAntiEntropyTest do
  # Covers anti-entropy on real peer nodes. The cycles are run from the test
  # process (steady state and catch-up) or by sending `:run` to the cache's
  # anti-entropy process, whose own timer is set to an hour.
  use Nebulex.NodeCase, async: true

  import Mimic, only: [allow: 3, call_original: 3, stub: 3]
  import Nebulex.CacheCase, only: [safe_stop: 1, wait_until: 1, with_telemetry_handler: 3]

  alias Nebulex.Adapter
  alias Nebulex.Adapters.Replicated
  alias Nebulex.Adapters.Replicated.{AntiEntropy, Replicator}
  alias Nebulex.Distributed.RPC
  alias Nebulex.Distributed.TestCache.ReplicatedCache, as: Cache
  alias Nebulex.{Telemetry, Utils}
  alias Tidefall.HashMap.Entry

  @moduletag capture_log: true

  @name :replicated_anti_entropy_cache
  @opts [
    name: @name,
    replication: [interval: 100, anti_entropy_interval: :timer.hours(1), bootstrap_chunk_size: 2]
  ]

  @prefix Telemetry.default_prefix(Cache)
  @ae_stop @prefix ++ [:anti_entropy, :stop]
  @ae_exception @prefix ++ [:anti_entropy, :exception]

  setup do
    nodes = [node() | Node.list()]
    node_pid_list = start_caches(nodes, [{Cache, @opts}])

    wait_until(fn ->
      assert Cache.nodes(@name) |> Enum.sort() == Enum.sort(nodes)
    end)

    default_dynamic_cache = Cache.get_dynamic_cache()
    _ = Cache.put_dynamic_cache(@name)

    on_exit(fn ->
      _ = Cache.put_dynamic_cache(default_dynamic_cache)

      stop_caches(node_pid_list)
    end)

    [peer | _] = peers = Node.list()

    {:ok, meta: Adapter.lookup_meta(@name), peer: peer, peers: peers}
  end

  describe "steady state" do
    setup %{peer: peer} do
      # Written on a peer, so every node logs it as it applies it.
      :ok = rpc(peer, Cache, :put, [@name, :k, "v1", []])

      wait_until(fn -> assert Cache.get!(:k) == "v1" end)

      :ok
    end

    test "a cycle before the peer has an update keeps the update", %{meta: meta, peer: peer} do
      :ok = Cache.put(:k, "v2")

      assert {0, _} = AntiEntropy.steady_state(peer, meta)
      assert Cache.get!(:k) == "v2"

      wait_until(fn -> assert rpc(peer, Cache, :get!, [@name, :k, nil, []]) == "v2" end)
    end

    test "a cycle before the peer has a delete keeps the delete", %{meta: meta, peer: peer} do
      :ok = Cache.delete(:k)

      assert {0, _} = AntiEntropy.steady_state(peer, meta)
      assert Cache.has_key?(:k) == {:ok, false}

      wait_until(fn -> assert rpc(peer, Cache, :get!, [@name, :k, nil, []]) == nil end)
    end

    test "a missed recent delete is repaired by deleting", %{meta: meta, peer: peer} do
      deliver(peer, [remote({:delete, [:k, []]}, System.system_time())])

      assert {1, 1} = AntiEntropy.steady_state(peer, meta)

      wait_until(fn -> assert Cache.has_key?(:k) == {:ok, false} end)
    end

    test "a missed recent update is repaired to the newer value", %{meta: meta, peer: peer} do
      deliver(peer, [remote({:put, [:k, "v2", [ttl: :timer.hours(1)]]}, System.system_time())])

      assert {1, 1} = AntiEntropy.steady_state(peer, meta)

      wait_until(fn -> assert Cache.get!(:k) == "v2" end)
      assert Cache.ttl!(:k) <= :timer.hours(1)
    end

    test "an entry evicted here is not refilled", %{meta: meta, peer: peer} do
      :ok = Replicated.with_dynamic_cache(meta, :delete, [:k, []])

      assert {0, _} = AntiEntropy.steady_state(peer, meta)
      assert {0, _} = AntiEntropy.steady_state(peer, meta)
      assert Cache.has_key?(:k) == {:ok, false}
    end

    test "a put the peer no longer holds is skipped", %{meta: meta, peer: peer} do
      deliver(peer, [remote({:put, [:k, "v2", []]}, System.system_time())])

      peer_meta = rpc(peer, Adapter, :lookup_meta, [@name])
      :ok = rpc(peer, Replicated, :with_dynamic_cache, [peer_meta, :delete, [:k, []]])

      assert {0, 1} = AntiEntropy.steady_state(peer, meta)
      assert Cache.get!(:k) == "v1"
    end

    test "a transfer larger than one chunk arrives in chunks", %{meta: meta, peer: peer} do
      version = System.system_time()
      keys = for i <- 1..5, do: {:chunk, i}

      deliver(peer, for(key <- keys, do: remote({:put, [key, key, []]}, version)))

      count_rpc_calls()

      assert {5, _} = AntiEntropy.steady_state(peer, meta)
      assert rpc_calls(:log_values) == 3

      wait_until(fn -> assert Cache.get_all!(in: keys) |> Map.new() |> map_size() == 5 end)
    end
  end

  describe "catch-up" do
    test "fills the gaps of a node whose bootstrap failed", %{meta: meta, peer: peer} do
      keys = for i <- 1..5, do: {:gap, i}

      for key <- keys, do: put_on_primary(peer, key, key)

      count_rpc_calls()

      assert {5, _} = AntiEntropy.catch_up(peer, meta)
      assert Cache.get_all!(in: keys) |> Map.new() |> map_size() == 5
      assert rpc_calls(:values) == 3
    end

    test "never overwrites a value or a key with a logged change", %{meta: meta, peer: peer} do
      :ok = Replicated.with_dynamic_cache(meta, :put, [:k1, "here", []])
      deliver(node(), [remote({:delete, [:k2, []]}, System.system_time())])

      put_on_primary(peer, :k1, "there")
      put_on_primary(peer, :k2, "there")

      assert {0, _} = AntiEntropy.catch_up(peer, meta)
      assert Cache.get!(:k1) == "here"
      assert Cache.has_key?(:k2) == {:ok, false}
    end
  end

  describe "cycles" do
    test "start with catch-up and stop it after a pass that repairs nothing", %{peers: peers} do
      with_telemetry_handler __MODULE__, [@ae_stop], fn ->
        run_cycle()
        assert_receive {@ae_stop, _, %{mode: :catch_up, repaired: 0}}

        run_cycle()
        assert_receive {@ae_stop, _, %{mode: :steady_state, peer: peer}}
        assert peer in peers
      end
    end

    test "stop catch-up after three passes", %{peers: peers} do
      with_telemetry_handler __MODULE__, [@ae_stop], fn ->
        for i <- 1..3 do
          for peer <- peers, do: put_on_primary(peer, {:pass, i}, i)

          run_cycle()
          assert_receive {@ae_stop, _, %{mode: :catch_up, repaired: 1}}
        end

        for peer <- peers, do: put_on_primary(peer, {:pass, 4}, 4)

        run_cycle()
        assert_receive {@ae_stop, _, %{mode: :steady_state}}
      end
    end

    test "a healed partition starts a catch-up, a restarted peer does not", %{peer: peer} do
      %{pg_ref: pg_ref, adapter_meta: %{pg_group: pg_group}} = ae_state()
      member = Node.spawn(peer, fn -> Process.sleep(:infinity) end)
      new_member = Node.spawn(peer, fn -> Process.sleep(:infinity) end)

      with_telemetry_handler __MODULE__, [@ae_stop], fn ->
        run_cycle()
        assert_receive {@ae_stop, _, %{mode: :catch_up, repaired: 0}}

        # The peer comes back as a new process.
        send(ae_pid(), {pg_ref, :leave, pg_group, [member]})
        send(ae_pid(), {pg_ref, :join, pg_group, [new_member]})

        run_cycle()
        assert_receive {@ae_stop, _, %{mode: :steady_state}}

        # The same process comes back.
        send(ae_pid(), {pg_ref, :leave, pg_group, [new_member]})
        send(ae_pid(), {pg_ref, :join, pg_group, [new_member]})

        run_cycle()
        assert_receive {@ae_stop, _, %{mode: :catch_up}}
      end
    end

    test "a failed catch-up pass is reported and counts as a pass" do
      allow(RPC, self(), ae_pid())
      stub(RPC, :call, fn _node, _mod, _fun, _args, _timeout -> raise "boom" end)

      with_telemetry_handler __MODULE__, [@ae_stop, @ae_exception], fn ->
        for _ <- 1..3 do
          run_cycle()
          assert_receive {@ae_exception, _, %{mode: :catch_up, kind: :error}}
        end

        stub(RPC, :call, &call_original(RPC, :call, [&1, &2, &3, &4, &5]))

        run_cycle()
        assert_receive {@ae_stop, _, %{mode: :steady_state}}
      end
    end

    test "do nothing without peers" do
      name = :replicated_ae_alone

      {:ok, pid} =
        Cache.start_link(name: name, replication: [anti_entropy_interval: :timer.hours(1)])

      ae_pid = Process.whereis(ae_name(name))

      with_telemetry_handler __MODULE__, [@ae_stop], fn ->
        send(ae_pid, :run)

        _ = :sys.get_state(ae_pid)
        refute_received {@ae_stop, _, _}
      end

      safe_stop(pid)
    end

    test "an unreachable peer fails the cycle", %{meta: meta} do
      assert_raise Nebulex.Error, fn ->
        AntiEntropy.steady_state(:"nonexistent@127.0.0.1", meta)
      end
    end
  end

  describe "options" do
    test "anti-entropy is on by default, and false turns it off" do
      {:ok, pid} = Cache.start_link(name: :replicated_ae_default)
      assert Process.whereis(ae_name(:replicated_ae_default))
      safe_stop(pid)

      {:ok, pid} =
        Cache.start_link(name: :replicated_ae_off, replication: [anti_entropy_interval: false])

      refute Process.whereis(ae_name(:replicated_ae_off))
      safe_stop(pid)
    end
  end

  ## Helpers

  defp deliver(node, batch) do
    :ok =
      rpc(node, Replicator, :process_inbox, [batch, rpc(node, Adapter, :lookup_meta, [@name])])
  end

  defp remote({_op, [key | _]} = command, version) do
    %Entry{key: key, value: {command, :remote}, version: version, updates: 0}
  end

  # Writes to a node's primary cache only: no replication, no log entry.
  defp put_on_primary(node, key, value) do
    meta = rpc(node, Adapter, :lookup_meta, [@name])

    :ok = rpc(node, Replicated, :with_dynamic_cache, [meta, :put, [key, value, []]])
  end

  defp count_rpc_calls do
    test_pid = self()

    stub(RPC, :call, fn node, mod, fun, args, timeout ->
      send(test_pid, {:rpc_call, fun})

      call_original(RPC, :call, [node, mod, fun, args, timeout])
    end)
  end

  defp rpc_calls(fun, count \\ 0) do
    receive do
      {:rpc_call, ^fun} -> rpc_calls(fun, count + 1)
    after
      0 -> count
    end
  end

  defp ae_name(name), do: Utils.camelize_and_concat([name, "AntiEntropy"])

  defp ae_pid, do: Process.whereis(ae_name(@name))

  defp ae_state, do: :sys.get_state(ae_pid())

  # Runs one cycle of the cache's anti-entropy process and waits for it.
  defp run_cycle do
    send(ae_pid(), :run)

    _ = ae_state()
  end
end
