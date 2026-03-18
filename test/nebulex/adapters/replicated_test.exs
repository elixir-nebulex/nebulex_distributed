defmodule Nebulex.Adapters.ReplicatedCacheTest do
  use Nebulex.NodeCase, async: true

  # Inherit tests
  use Nebulex.CacheTestCase, except: [Nebulex.Cache.KVPropTest]

  import Mimic, only: [stub: 3, call_original: 3]
  import Nebulex.CacheCase

  alias Nebulex.Adapter
  alias Nebulex.Adapters.Replicated.Replicator
  alias Nebulex.Distributed.TestCache.{ReplicatedCache, ReplicatedNilCache}
  alias Nebulex.Telemetry

  @moduletag capture_log: true

  @primary :"primary@127.0.0.1"
  @cache_name :replicated_cache
  @telemetry_prefix Telemetry.default_prefix(ReplicatedCache)

  setup do
    cluster = :lists.usort([@primary | Application.get_env(:nebulex_distributed, :nodes, [])])
    nodes = [node() | Node.list()]

    node_pid_list =
      start_caches(
        nodes,
        [
          {ReplicatedCache, name: @cache_name, replication: [interval: 100]},
          {ReplicatedNilCache, replication: [interval: 100]}
        ]
      )

    # Wait for caches to be ready on all nodes
    :ok = Process.sleep(100)

    default_dynamic_cache = ReplicatedCache.get_dynamic_cache()
    _ = ReplicatedCache.put_dynamic_cache(@cache_name)

    on_exit(fn ->
      _ = ReplicatedCache.put_dynamic_cache(default_dynamic_cache)

      :ok = Process.sleep(100)

      stop_caches(node_pid_list)
    end)

    {:ok,
     cache: ReplicatedCache,
     name: @cache_name,
     cluster: cluster,
     nodes: nodes,
     nil_cache: ReplicatedNilCache}
  end

  describe "c:init/1" do
    test "initializes the primary store metadata" do
      adapter_meta =
        @cache_name
        |> Adapter.lookup_meta()
        |> Map.fetch!(:primary_name)
        |> Adapter.lookup_meta()

      assert adapter_meta.adapter == Nebulex.Adapters.Local
      assert adapter_meta.backend == :ets
    end

    test "raises an exception because invalid primary store" do
      msg = "invalid value for :adapter option: the module Invalid was not compiled"

      assert_raise NimbleOptions.ValidationError, ~r"#{msg}", fn ->
        defmodule Demo do
          use Nebulex.Cache,
            otp_app: :nebulex,
            adapter: Nebulex.Adapters.Replicated,
            adapter_opts: [primary_storage_adapter: Invalid]
        end
      end
    end
  end

  describe "cluster membership" do
    test "nodes returns all cluster nodes", %{nodes: nodes} do
      assert ReplicatedCache.nodes() |> Enum.sort() == Enum.sort(nodes)
    end

    test "join and leave cluster", %{nodes: nodes, name: name} do
      assert ReplicatedCache.nodes() |> Enum.sort() == Enum.sort(nodes)

      ReplicatedCache.with_dynamic_cache(name, fn ->
        :ok = ReplicatedCache.leave_cluster()

        assert_eventually fn ->
          assert ReplicatedCache.nodes() |> Enum.sort() == (nodes -- [node()]) |> Enum.sort()
        end
      end)

      ReplicatedCache.with_dynamic_cache(name, fn ->
        :ok = ReplicatedCache.join_cluster()

        assert_eventually fn ->
          assert ReplicatedCache.nodes() |> Enum.sort() == Enum.sort(nodes)
        end
      end)
    end
  end

  describe "local reads" do
    test "local cache hit returns data immediately" do
      assert ReplicatedCache.put("local_key", "local_value") == :ok
      assert ReplicatedCache.get!("local_key") == "local_value"
    end

    test "put_all and get work locally" do
      assert ReplicatedCache.put_all(a: 1, b: 2, c: 3) == :ok
      assert ReplicatedCache.get!(:a) == 1
      assert ReplicatedCache.get!(:b) == 2
      assert ReplicatedCache.get!(:c) == 3
    end

    test "cache miss returns nil" do
      assert ReplicatedCache.get(:missing_key) == {:ok, nil}
    end
  end

  describe "push-based replication" do
    test "put replicates to remote nodes", %{name: name, cluster: cluster} do
      assert ReplicatedCache.put(:rep_key, "value") == :ok
      assert ReplicatedCache.get!(:rep_key) == "value"

      # Wait for replication (outbox flush + inbox processing)
      assert_remote_nodes(cluster, :get!, [name, :rep_key, nil, []], fn result ->
        assert result == "value"
      end)
    end

    test "put_all replicates all entries to remote nodes", %{name: name, cluster: cluster} do
      assert ReplicatedCache.put_all(ra: 1, rb: 2, rc: 3) == :ok

      assert_remote_nodes(cluster, :get_all!, [name, [in: [:ra, :rb, :rc]], []], fn result ->
        assert result |> Map.new() == %{ra: 1, rb: 2, rc: 3}
      end)
    end

    test "delete replicates to remote nodes", %{name: name, cluster: cluster} do
      # Put on both nodes via replication
      assert ReplicatedCache.put(:del_key, "value") == :ok

      assert_remote_nodes(cluster, :get!, [name, :del_key, nil, []], fn result ->
        assert result == "value"
      end)

      # Delete locally, should replicate
      assert ReplicatedCache.delete(:del_key) == :ok

      assert_remote_nodes(cluster, :get, [name, :del_key, nil, []], fn result ->
        assert result == {:ok, nil}
      end)
    end

    test "take removes locally and replicates delete", %{name: name, cluster: cluster} do
      assert ReplicatedCache.put(:take_key, "take_value") == :ok

      assert_remote_nodes(cluster, :get!, [name, :take_key, nil, []], fn result ->
        assert result == "take_value"
      end)

      assert ReplicatedCache.take!(:take_key) == "take_value"
      assert ReplicatedCache.get(:take_key) == {:ok, nil}

      assert_remote_nodes(cluster, :get, [name, :take_key, nil, []], fn result ->
        assert result == {:ok, nil}
      end)
    end

    test "delete_all replicates to remote nodes", %{name: name, cluster: cluster} do
      assert ReplicatedCache.put(:da1, "v1") == :ok
      assert ReplicatedCache.put(:da2, "v2") == :ok

      assert_remote_nodes(
        cluster,
        :get_all!,
        [name, [in: [:da1, :da2]], []],
        fn result ->
          assert result |> Map.new() == %{da1: "v1", da2: "v2"}
        end
      )

      assert {:ok, count} = ReplicatedCache.delete_all()
      assert count > 0

      assert_remote_nodes(
        cluster,
        :get_all!,
        [name, [in: [:da1, :da2]], []],
        fn result ->
          assert result |> Map.new() == %{}
        end
      )
    end

    test "delete_all with :in replicates individual deletes", %{name: name, cluster: cluster} do
      assert ReplicatedCache.put(:in1, "v1") == :ok
      assert ReplicatedCache.put(:in2, "v2") == :ok
      assert ReplicatedCache.put(:in3, "v3") == :ok

      assert_remote_nodes(
        cluster,
        :get_all!,
        [name, [in: [:in1, :in2, :in3]], []],
        fn result ->
          assert result |> Map.new() == %{in1: "v1", in2: "v2", in3: "v3"}
        end
      )

      # Delete specific keys
      assert {:ok, 2} = ReplicatedCache.delete_all(in: [:in1, :in2])

      assert_remote_nodes(
        cluster,
        :get_all!,
        [
          name,
          [in: [:in1, :in2, :in3]],
          []
        ],
        fn result ->
          assert result |> Map.new() == %{in3: "v3"}
        end
      )
    end

    test "write on remote node replicates to primary", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      :ok = :rpc.call(remote_node, ReplicatedCache, :put, [name, :remote_key, "remote_val", []])

      assert_eventually fn ->
        assert ReplicatedCache.get!(:remote_key) == "remote_val"
      end
    end

    test "put overwrites previous value on remote nodes", %{name: name, cluster: cluster} do
      assert ReplicatedCache.put(:overwrite, "v1") == :ok

      assert_remote_nodes(cluster, :get!, [name, :overwrite, nil, []], fn result ->
        assert result == "v1"
      end)

      assert ReplicatedCache.put(:overwrite, "v2") == :ok

      assert_remote_nodes(cluster, :get!, [name, :overwrite, nil, []], fn result ->
        assert result == "v2"
      end)
    end

    test "expire replicates TTL change to remote nodes", %{name: name, cluster: cluster} do
      assert ReplicatedCache.put(:exp_key, "value", ttl: :timer.seconds(60)) == :ok

      assert_remote_nodes(cluster, :get!, [name, :exp_key, nil, []], fn result ->
        assert result == "value"
      end)

      # Expire with a short TTL
      assert ReplicatedCache.expire(:exp_key, :timer.seconds(1)) == {:ok, true}

      assert_remote_nodes(cluster, :ttl, [name, :exp_key, nil, []], fn {:ok, ttl} ->
        assert ttl <= :timer.seconds(1)
      end)
    end

    test "touch replicates to remote nodes", %{name: name, cluster: cluster} do
      assert ReplicatedCache.put(:touch_key, "value", ttl: :timer.seconds(60)) == :ok

      assert_remote_nodes(cluster, :get!, [name, :touch_key, nil, []], fn result ->
        assert result == "value"
      end)

      assert ReplicatedCache.touch(:touch_key) == {:ok, true}

      assert_remote_nodes(cluster, :has_key?, [name, :touch_key, nil, []], fn result ->
        assert result == {:ok, true}
      end)
    end

    test "update_counter replicates resulting value to remote nodes", %{
      name: name,
      cluster: cluster
    } do
      assert ReplicatedCache.incr!(:counter_key) == 1
      assert ReplicatedCache.incr!(:counter_key, 5) == 6

      assert_remote_nodes(cluster, :get!, [name, :counter_key, nil, []], fn result ->
        assert result == 6
      end)
    end

    test "expire on non-existing key does not replicate" do
      assert ReplicatedCache.expire(:missing_exp, :timer.seconds(1)) == {:ok, false}
    end

    test "touch on non-existing key does not replicate" do
      assert ReplicatedCache.touch(:missing_touch) == {:ok, false}
    end
  end

  describe "conditional writes" do
    test "put_new replicates to remote nodes when key doesn't exist", %{
      name: name,
      cluster: cluster
    } do
      assert ReplicatedCache.put_new!(:pn_key, "first") == true
      assert ReplicatedCache.get!(:pn_key) == "first"

      assert_remote_nodes(cluster, :get!, [name, :pn_key, nil, []], fn result ->
        assert result == "first"
      end)

      # Second put_new should not overwrite
      assert ReplicatedCache.put_new!(:pn_key, "second") == false
      assert ReplicatedCache.get!(:pn_key) == "first"

      # Remote nodes should still have the first value
      assert_remote_nodes(cluster, :get!, [name, :pn_key, nil, []], fn result ->
        assert result == "first"
      end)
    end

    test "replace replicates to remote nodes when key exists", %{
      name: name,
      cluster: cluster
    } do
      # Replace on non-existing key should fail and not replicate
      assert ReplicatedCache.replace!(:rp_key, "value") == false

      assert ReplicatedCache.put(:rp_key, "original") == :ok

      assert_remote_nodes(cluster, :get!, [name, :rp_key, nil, []], fn result ->
        assert result == "original"
      end)

      assert ReplicatedCache.replace!(:rp_key, "replaced") == true
      assert ReplicatedCache.get!(:rp_key) == "replaced"

      assert_remote_nodes(cluster, :get!, [name, :rp_key, nil, []], fn result ->
        assert result == "replaced"
      end)
    end

    test "put_new_all replicates to remote nodes when no keys exist", %{
      name: name,
      cluster: cluster
    } do
      assert ReplicatedCache.put_new_all!(pna1: "v1", pna2: "v2") == true

      assert_remote_nodes(cluster, :get_all!, [name, [in: [:pna1, :pna2]], []], fn result ->
        assert result |> Map.new() == %{pna1: "v1", pna2: "v2"}
      end)

      # Second put_new_all should fail since keys exist
      assert ReplicatedCache.put_new_all!(pna1: "x1", pna2: "x2") == false
      assert ReplicatedCache.get!(:pna1) == "v1"
    end

    test "incr on non-integer value raises error" do
      assert ReplicatedCache.put(:bad_counter, "not_an_integer") == :ok

      assert_raise ArgumentError, fn ->
        ReplicatedCache.incr!(:bad_counter)
      end
    end
  end

  describe "replication telemetry" do
    test "emits start and stop span events on replication", %{nodes: nodes} do
      started = @telemetry_prefix ++ [:replication, :start]
      stopped = @telemetry_prefix ++ [:replication, :stop]

      with_telemetry_handler __MODULE__, [started, stopped], fn ->
        assert ReplicatedCache.put(:tel_key, "value") == :ok

        assert_receive {^started, %{system_time: _}, %{adapter_meta: _, node: node, peers: peers}},
                       5000

        assert node == node()
        assert Enum.sort(peers) == Enum.reject(nodes, &(&1 == node())) |> Enum.sort()

        assert_receive {^stopped, %{duration: _},
                        %{adapter_meta: _, node: stop_node, peers: stop_peers, errors: []}},
                       5000

        assert stop_node == node()
        assert Enum.sort(stop_peers) == Enum.reject(nodes, &(&1 == node())) |> Enum.sort()
      end
    end

    test "emits stop event with errors when a peer node is unreachable" do
      adapter_meta = Adapter.lookup_meta(@cache_name)
      stopped = @telemetry_prefix ++ [:replication, :stop]
      fake_node = :"nonexistent@127.0.0.1"

      entries = [
        {:fail_key, {{:put, [:fail_key, "value", []]}, :remote}, System.monotonic_time()}
      ]

      with_telemetry_handler __MODULE__, [stopped], fn ->
        Replicator.replicate_to_peers([fake_node], entries, adapter_meta, 1)

        # First attempt fails, retries once, then gives up
        # We should receive 2 stop events (initial + 1 retry), both with errors
        assert_receive {^stopped, %{duration: _}, %{errors: [{_error, ^fake_node}]}}, 5000
        assert_receive {^stopped, %{duration: _}, %{errors: [{_error, ^fake_node}]}}, 5000
      end
    end
  end

  describe "bootstrap on node join" do
    test "copy_entries copies data with TTL from a remote peer", %{cluster: cluster} do
      adapter_meta = Adapter.lookup_meta(@cache_name)
      remote_node = find_remote_node(cluster)

      # Put data directly on the remote node's primary (bypass replication)
      :rpc.call(remote_node, Nebulex.Adapters.Replicated, :with_dynamic_cache, [
        adapter_meta,
        :put,
        [:ce_key1, "value1", []]
      ])

      :rpc.call(remote_node, Nebulex.Adapters.Replicated, :with_dynamic_cache, [
        adapter_meta,
        :put,
        [:ce_key2, "value2", [ttl: :timer.seconds(60)]]
      ])

      :rpc.call(remote_node, Nebulex.Adapters.Replicated, :with_dynamic_cache, [
        adapter_meta,
        :put,
        [:ce_key3, "value3", []]
      ])

      # Copy entries from the remote peer into the local inbox
      assert Replicator.copy_entries(remote_node, adapter_meta) >= 3

      # Wait for inbox processing
      assert_eventually fn ->
        assert ReplicatedCache.get_all!(in: [:ce_key1, :ce_key2, :ce_key3]) ==
                 %{ce_key1: "value1", ce_key2: "value2", ce_key3: "value3"}
      end

      # Verify TTL was preserved for ce_key2
      assert_eventually fn ->
        ttl = ReplicatedCache.ttl!(:ce_key2)

        assert is_integer(ttl) and ttl > 0 and ttl <= :timer.seconds(60)
      end
    end

    test "copy_entries returns 0 when peer has no data", %{cluster: cluster} do
      adapter_meta = Adapter.lookup_meta(@cache_name)
      remote_node = find_remote_node(cluster)

      # Remote node has no data, copy_entries should return 0
      assert Replicator.copy_entries(remote_node, adapter_meta) == 0
    end

    test "stream_entries skips entries when TTL lookup fails" do
      primary = ReplicatedCache.__primary__()

      # Put some data
      assert ReplicatedCache.put(:skip_key1, "value1") == :ok
      assert ReplicatedCache.put(:skip_key2, "value2") == :ok

      # Stub the primary's ttl to fail for :skip_key1
      primary
      |> stub(:ttl, fn
        :skip_key1, _opts ->
          {:error, %Nebulex.KeyError{key: :skip_key1, reason: :not_found}}

        key, opts ->
          call_original(primary, :ttl, [key, opts])
      end)

      adapter_meta = Adapter.lookup_meta(@cache_name)
      entries = Replicator.stream_entries(adapter_meta)
      entries_map = Map.new(entries)

      # skip_key1 should be filtered out, skip_key2 should be present
      refute Map.has_key?(entries_map, :skip_key1)
      assert entries_map[:skip_key2] == {:put, [:skip_key2, "value2", [ttl: :infinity]]}
    end

    test "new node bootstraps data from an existing peer", %{name: name, cluster: cluster} do
      # Put some data with and without TTL
      assert ReplicatedCache.put(:bs_key1, "value1") == :ok
      assert ReplicatedCache.put(:bs_key2, "value2", ttl: :timer.seconds(60)) == :ok
      assert ReplicatedCache.put(:bs_key3, "value3") == :ok

      # Wait for replication to all nodes
      assert_remote_nodes(cluster, :get!, [name, :bs_key1, nil, []], fn result ->
        assert result == "value1"
      end)

      # Stop the cache on the primary (current) node so bootstrap runs locally
      # (ensures coverage of stream_entries and bootstrap logic)
      :ok = ReplicatedCache.stop()

      # Restart the cache — should bootstrap from a peer
      {:ok, _pid} = ReplicatedCache.start_link(name: name, replication: [interval: 100])

      # Wait for bootstrap + inbox processing
      assert_eventually fn ->
        assert ReplicatedCache.get_all!(in: [:bs_key1, :bs_key2, :bs_key3]) ==
                 %{bs_key1: "value1", bs_key2: "value2", bs_key3: "value3"}
      end

      # Verify TTL was preserved for bs_key2
      assert_eventually fn ->
        ttl = ReplicatedCache.ttl!(:bs_key2)

        assert is_integer(ttl) and ttl > 0 and ttl <= :timer.seconds(60)
      end

      # Cleanup: stop the restarted cache
      :ok = ReplicatedCache.stop()
    end
  end

  describe "info" do
    test "returns cache info" do
      assert ReplicatedNilCache.info!() != %{}
    end
  end

  ## Private functions

  defp assert_remote_nodes(cluster, fun, args, callback) do
    cluster
    |> Enum.reject(&(&1 == node()))
    |> Enum.each(fn node ->
      assert_eventually fn ->
        result = :rpc.call(node, ReplicatedCache, fun, args)

        callback.(result)
      end
    end)
  end

  defp find_remote_node(cluster) do
    cluster
    |> Enum.reject(&(&1 == node()))
    |> Enum.random()
  end
end
