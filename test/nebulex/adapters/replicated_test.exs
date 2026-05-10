defmodule Nebulex.Adapters.ReplicatedCacheTest do
  use Nebulex.NodeCase, async: true

  # Inherit tests
  use Nebulex.CacheTestCase, except: [Nebulex.Cache.KVPropTest]

  import Mimic, only: [stub: 3, call_original: 3]
  import Nebulex.CacheCase

  alias Nebulex.Adapter
  alias Nebulex.Adapters.Replicated.{AntiEntropy, ClusterMonitor, Replicator}
  alias Nebulex.Distributed.Cluster
  alias Nebulex.Distributed.TestCache.{ReplicatedCache, ReplicatedNilCache}
  alias Nebulex.Telemetry
  alias Nebulex.Utils

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

    test "cluster monitor updates cluster view on PG leave event" do
      cm_name = Utils.camelize_and_concat([@cache_name, "ClusterMonitor"])
      cm_pid = Process.whereis(cm_name)

      # Get the current state to extract pg_ref and pg_group
      %{pg_ref: pg_ref, pg_group: pg_group, cluster_nodes: cluster_nodes} =
        :sys.get_state(cm_pid)

      assert MapSet.size(cluster_nodes) > 0

      # Simulate a PG leave event for a fake node
      fake_pid = self()
      send(cm_pid, {pg_ref, :leave, pg_group, [fake_pid]})

      # Allow the message to be processed
      :ok = Process.sleep(50)

      # Verify the cluster view was updated (current node removed)
      %{cluster_nodes: updated_nodes} = :sys.get_state(cm_pid)
      refute MapSet.member?(updated_nodes, node())
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
        {:fail_key, {{:put, [:fail_key, "value", []]}, :remote}, System.system_time()}
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

  describe "bootstrap on node join (inverted bootstrap)" do
    test "push_entries pushes data with TTL to a remote peer", %{cluster: cluster} do
      adapter_meta = Adapter.lookup_meta(@cache_name)
      remote_node = find_remote_node(cluster)

      # Put data locally
      assert ReplicatedCache.put(:pe_key1, "value1") == :ok
      assert ReplicatedCache.put(:pe_key2, "value2", ttl: :timer.seconds(60)) == :ok
      assert ReplicatedCache.put(:pe_key3, "value3") == :ok

      # Push entries to the remote peer using put_new
      assert Replicator.push_entries(remote_node, adapter_meta) >= 3

      # Wait for the remote node to process entries
      assert_eventually fn ->
        result =
          :rpc.call(remote_node, ReplicatedCache, :get_all!, [
            @cache_name,
            [in: [:pe_key1, :pe_key2, :pe_key3]],
            []
          ])

        assert result == %{pe_key1: "value1", pe_key2: "value2", pe_key3: "value3"}
      end

      # Verify TTL was preserved for pe_key2
      assert_eventually fn ->
        {:ok, ttl} =
          :rpc.call(remote_node, ReplicatedCache, :ttl, [@cache_name, :pe_key2, nil, []])

        assert is_integer(ttl) and ttl > 0 and ttl <= :timer.seconds(60)
      end
    end

    test "push_entries returns 0 when local cache has no data" do
      adapter_meta = Adapter.lookup_meta(@cache_name)
      fake_node = :"nonexistent@127.0.0.1"

      # No data locally, push_entries should return 0 without making any RPC
      assert Replicator.push_entries(fake_node, adapter_meta) == 0
    end

    test "push_entries does not overwrite existing data on target (put_new semantics)",
         %{cluster: cluster} do
      adapter_meta = Adapter.lookup_meta(@cache_name)
      remote_node = find_remote_node(cluster)

      # Put data on the remote node directly (simulating data received via replication)
      :rpc.call(remote_node, Nebulex.Adapters.Replicated, :with_dynamic_cache, [
        adapter_meta,
        :put,
        [:pn_existing, "remote_value", []]
      ])

      # Put a different value locally for the same key
      assert ReplicatedCache.put(:pn_existing, "local_value") == :ok

      # Push entries to the remote peer — should NOT overwrite remote_value
      assert Replicator.push_entries(remote_node, adapter_meta) >= 1

      # Remote node should still have the original value (put_new is a no-op)
      assert_eventually fn ->
        result =
          :rpc.call(remote_node, ReplicatedCache, :get!, [
            @cache_name,
            :pn_existing,
            nil,
            []
          ])

        assert result == "remote_value"
      end
    end

    test "apply_bootstrap_entries writes entries to local cache" do
      adapter_meta = Adapter.lookup_meta(@cache_name)

      entries = [
        {:ab_key1, {:put_new, [:ab_key1, "value1", [ttl: :infinity]]}},
        {:ab_key2, {:put_new, [:ab_key2, "value2", [ttl: :timer.seconds(60)]]}}
      ]

      assert Replicator.apply_bootstrap_entries(adapter_meta, entries) == :ok

      assert ReplicatedCache.get!(:ab_key1) == "value1"
      assert ReplicatedCache.get!(:ab_key2) == "value2"
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

      assert entries_map[:skip_key2] ==
               {:put, [:skip_key2, "value2", [ttl: :infinity, telemetry: false]]}
    end

    test "new node receives data from existing peers via inverted bootstrap", %{
      name: name,
      cluster: cluster
    } do
      # Put some data with and without TTL
      assert ReplicatedCache.put(:bs_key1, "value1") == :ok
      assert ReplicatedCache.put(:bs_key2, "value2", ttl: :timer.seconds(60)) == :ok
      assert ReplicatedCache.put(:bs_key3, "value3") == :ok

      # Wait for replication to all nodes
      assert_remote_nodes(cluster, :get!, [name, :bs_key1, nil, []], fn result ->
        assert result == "value1"
      end)

      # Stop the cache on the primary (current) node
      :ok = ReplicatedCache.stop()

      # Restart the cache — existing peers should detect the join and push entries
      {:ok, _pid} = ReplicatedCache.start_link(name: name, replication: [interval: 100])

      # Wait for inverted bootstrap (existing nodes push) + processing
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

    test "bootstrap emits telemetry span events", %{cluster: cluster} do
      adapter_meta = Adapter.lookup_meta(@cache_name)
      remote_node = find_remote_node(cluster)

      # Put data locally
      assert ReplicatedCache.put(:bs_tel1, "value1") == :ok

      started = @telemetry_prefix ++ [:bootstrap, :start]
      stopped = @telemetry_prefix ++ [:bootstrap, :stop]

      with_telemetry_handler __MODULE__, [started, stopped], fn ->
        # Call push_to_new_node directly (local node pushes to remote)
        ClusterMonitor.push_to_new_node(remote_node, adapter_meta)

        assert_receive {^started, %{system_time: _},
                        %{adapter_meta: _, node: pusher, peer: ^remote_node}},
                       5000

        assert pusher == node()

        assert_receive {^stopped, %{duration: _}, %{adapter_meta: _, total: total}}, 5000

        assert is_integer(total) and total >= 1
      end
    end
  end

  describe "anti-entropy" do
    test "build_buckets returns a list of 1024 bucket hashes" do
      adapter_meta = Adapter.lookup_meta(@cache_name)

      # Put some data
      assert ReplicatedCache.put(:ae_b1, "v1") == :ok
      assert ReplicatedCache.put(:ae_b2, "v2") == :ok

      buckets = AntiEntropy.build_buckets(adapter_meta)

      assert is_list(buckets)
      assert Enum.count(buckets) == 1024

      # At least some buckets should be non-zero
      assert Enum.any?(buckets, &(&1 != 0))
    end

    test "build_buckets is deterministic for the same data" do
      adapter_meta = Adapter.lookup_meta(@cache_name)

      assert ReplicatedCache.put(:ae_det1, "value1") == :ok
      assert ReplicatedCache.put(:ae_det2, "value2") == :ok

      buckets1 = AntiEntropy.build_buckets(adapter_meta)
      buckets2 = AntiEntropy.build_buckets(adapter_meta)

      assert buckets1 == buckets2
    end

    test "build_buckets differs when data diverges", %{cluster: cluster} do
      adapter_meta = Adapter.lookup_meta(@cache_name)
      remote_node = find_remote_node(cluster)

      # Put data only on the remote node's primary (bypass replication)
      :rpc.call(remote_node, Nebulex.Adapters.Replicated, :with_dynamic_cache, [
        adapter_meta,
        :put,
        [:ae_div_key, "divergent_value", []]
      ])

      local_buckets = AntiEntropy.build_buckets(adapter_meta)

      remote_buckets =
        :rpc.call(remote_node, AntiEntropy, :build_buckets, [adapter_meta])

      assert local_buckets != remote_buckets
    end

    test "entries_for_buckets returns entries in the given buckets" do
      adapter_meta = Adapter.lookup_meta(@cache_name)

      assert ReplicatedCache.put(:ae_efb1, "v1") == :ok
      assert ReplicatedCache.put(:ae_efb2, "v2", ttl: :timer.seconds(60)) == :ok

      # Find which bucket :ae_efb1 falls into
      bucket_idx = :erlang.phash2(:ae_efb1, 1024)

      entries = AntiEntropy.entries_for_buckets(adapter_meta, [bucket_idx])

      # Should contain at least :ae_efb1
      entries_map = Map.new(entries)
      assert Map.has_key?(entries_map, :ae_efb1)

      # Verify the command format
      {:put, [key, value, opts]} = entries_map[:ae_efb1]
      assert key == :ae_efb1
      assert value == "v1"
      assert Keyword.has_key?(opts, :ttl)
    end

    test "do_reconcile repairs divergent entries from peer", %{cluster: cluster} do
      adapter_meta = Adapter.lookup_meta(@cache_name)
      remote_node = find_remote_node(cluster)

      # Put data only on the remote node's primary (bypass replication)
      :rpc.call(remote_node, Nebulex.Adapters.Replicated, :with_dynamic_cache, [
        adapter_meta,
        :put,
        [:ae_direct_key, "direct_value", []]
      ])

      # Verify key does NOT exist locally
      assert ReplicatedCache.get(:ae_direct_key) == {:ok, nil}

      # Call do_reconcile directly (in test process, ensures coverage)
      {repaired, divergent_buckets} = AntiEntropy.do_reconcile(remote_node, adapter_meta)

      assert repaired > 0
      assert divergent_buckets > 0

      # Wait for inbox processing
      assert_eventually fn ->
        assert ReplicatedCache.get!(:ae_direct_key) == "direct_value"
      end
    end

    test "reconcile repairs divergent keys from peer", %{cluster: cluster} do
      adapter_meta = Adapter.lookup_meta(@cache_name)
      remote_node = find_remote_node(cluster)

      # Put data only on the remote node's primary (bypass replication)
      :rpc.call(remote_node, Nebulex.Adapters.Replicated, :with_dynamic_cache, [
        adapter_meta,
        :put,
        [:ae_repair_key, "repaired_value", []]
      ])

      # Verify key does NOT exist locally
      assert ReplicatedCache.get(:ae_repair_key) == {:ok, nil}

      # Start anti-entropy for this test (it's not started by default)
      ae_meta = Map.put(adapter_meta, :anti_entropy_interval, 500)
      {:ok, ae_pid} = AntiEntropy.start_link(ae_meta)

      # Wait for at least one cycle to run
      :ok = Process.sleep(700)

      # Key should now be repaired locally via inbox
      assert_eventually fn ->
        assert ReplicatedCache.get!(:ae_repair_key) == "repaired_value"
      end

      # Cleanup
      GenServer.stop(ae_pid)
    end

    test "anti-entropy emits telemetry span events", %{cluster: cluster} do
      adapter_meta = Adapter.lookup_meta(@cache_name)
      remote_node = find_remote_node(cluster)

      # Put data only on the remote node to create divergence
      :rpc.call(remote_node, Nebulex.Adapters.Replicated, :with_dynamic_cache, [
        adapter_meta,
        :put,
        [:ae_tel_key, "tel_value", []]
      ])

      started = @telemetry_prefix ++ [:anti_entropy, :start]
      stopped = @telemetry_prefix ++ [:anti_entropy, :stop]

      with_telemetry_handler __MODULE__, [started, stopped], fn ->
        ae_meta = Map.put(adapter_meta, :anti_entropy_interval, 500)
        {:ok, ae_pid} = AntiEntropy.start_link(ae_meta)

        assert_receive {^started, %{system_time: _}, %{adapter_meta: _, node: node, peer: peer}},
                       5000

        assert node == node()

        pg_nodes = Cluster.pg_nodes(adapter_meta.pg_group)

        assert peer in Enum.reject(pg_nodes, &(&1 == node()))

        assert_receive {^stopped, %{duration: _},
                        %{
                          adapter_meta: _,
                          node: _,
                          peer: _,
                          repaired: repaired,
                          divergent_buckets: divergent_buckets
                        }},
                       5000

        assert is_integer(repaired)
        assert is_integer(divergent_buckets)

        GenServer.stop(ae_pid)
      end
    end

    test "anti-entropy with no peers does nothing" do
      adapter_meta = Adapter.lookup_meta(@cache_name)
      ae_meta = Map.put(adapter_meta, :anti_entropy_interval, 100)

      # Temporarily leave the PG group so there are no peers
      :ok = Cluster.leave(adapter_meta.pg_group)

      {:ok, ae_pid} = AntiEntropy.start_link(ae_meta)

      # Wait for a cycle — should not crash
      :ok = Process.sleep(200)

      assert Process.alive?(ae_pid)

      GenServer.stop(ae_pid)

      # Rejoin
      :ok = Cluster.join(adapter_meta.pg_group)
    end

    test "reconcile does nothing when caches are in sync", %{name: name, cluster: cluster} do
      # Put data and wait for replication so all nodes are in sync
      assert ReplicatedCache.put(:ae_sync1, "v1") == :ok
      assert ReplicatedCache.put(:ae_sync2, "v2") == :ok

      assert_remote_nodes(cluster, :get!, [name, :ae_sync1, nil, []], fn result ->
        assert result == "v1"
      end)

      adapter_meta = Adapter.lookup_meta(@cache_name)
      stopped = @telemetry_prefix ++ [:anti_entropy, :stop]

      with_telemetry_handler __MODULE__, [stopped], fn ->
        ae_meta = Map.put(adapter_meta, :anti_entropy_interval, 500)
        {:ok, ae_pid} = AntiEntropy.start_link(ae_meta)

        assert_receive {^stopped, %{duration: _}, %{repaired: 0, divergent_buckets: 0}},
                       5000

        GenServer.stop(ae_pid)
      end
    end

    test "entries_for_buckets skips entries when TTL lookup fails" do
      primary = ReplicatedCache.__primary__()
      adapter_meta = Adapter.lookup_meta(@cache_name)

      assert ReplicatedCache.put(:ae_ttl_ok, "good") == :ok
      assert ReplicatedCache.put(:ae_ttl_fail, "bad") == :ok

      # Stub TTL to fail for :ae_ttl_fail
      primary
      |> stub(:ttl, fn
        :ae_ttl_fail, _opts ->
          {:error, %Nebulex.KeyError{key: :ae_ttl_fail, reason: :not_found}}

        key, opts ->
          call_original(primary, :ttl, [key, opts])
      end)

      bucket_ok = :erlang.phash2(:ae_ttl_ok, 1024)
      bucket_fail = :erlang.phash2(:ae_ttl_fail, 1024)

      entries_map =
        adapter_meta
        |> AntiEntropy.entries_for_buckets(Enum.uniq([bucket_ok, bucket_fail]))
        |> Map.new()

      # :ae_ttl_fail should be filtered out
      refute Map.has_key?(entries_map, :ae_ttl_fail)
      assert Map.has_key?(entries_map, :ae_ttl_ok)
    end

    test "anti-entropy is not started when interval is not configured" do
      # The default setup doesn't set anti_entropy_interval,
      # so AntiEntropy should not be in the supervision tree
      adapter_meta = Adapter.lookup_meta(@cache_name)

      ae_name = Utils.camelize_and_concat([@cache_name, "AntiEntropy"])

      assert Process.whereis(ae_name) == nil
      assert adapter_meta[:anti_entropy_interval] == nil
    end

    test "cache started with anti_entropy_interval runs the GenServer" do
      cache_name = :replicated_ae_cache

      cache_opts = [
        name: cache_name,
        replication: [interval: 100, anti_entropy_interval: 500]
      ]

      {:ok, pid} = ReplicatedCache.start_link(cache_opts)

      # The GenServer name uses the bare "AntiEntropy" atom
      ae_name = Utils.camelize_and_concat([cache_name, "AntiEntropy"])

      ae_pid = Process.whereis(ae_name)

      assert ae_pid != nil
      assert Process.alive?(ae_pid)

      Supervisor.stop(pid)
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
