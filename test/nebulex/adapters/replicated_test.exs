defmodule Nebulex.Adapters.ReplicatedCacheTest do
  use Nebulex.NodeCase

  # Inherit tests
  use Nebulex.CacheTestCase

  import Nebulex.CacheCase

  alias Nebulex.Adapter
  alias Nebulex.Distributed.TestCache.{ReplicatedCache, ReplicatedNilCache}

  @moduletag capture_log: true

  @primary :"primary@127.0.0.1"
  @cache_name :replicated_cache

  setup do
    cluster = :lists.usort([@primary | Application.get_env(:nebulex_distributed, :nodes, [])])
    nodes = [node() | Node.list()]

    node_pid_list =
      start_caches(
        nodes,
        [{ReplicatedCache, name: @cache_name}, {ReplicatedNilCache, []}]
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
      assert ReplicatedCache.put_all([{:a, 1}, {:b, 2}, {:c, 3}]) == :ok
      assert ReplicatedCache.get!(:a) == 1
      assert ReplicatedCache.get!(:b) == 2
      assert ReplicatedCache.get!(:c) == 3
    end
  end

  describe "lazy-pull replication" do
    test "cache miss pulls data from peer node", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put a value on a remote node
      :ok = :rpc.call(remote_node, ReplicatedCache, :put, [name, :pull_key, "remote_value", []])

      assert :rpc.call(remote_node, ReplicatedCache, :get!, [name, :pull_key, nil, []]) ==
               "remote_value"

      # Primary node doesn't have the value locally yet
      # But fetch should pull from the peer transparently
      assert ReplicatedCache.get!(:pull_key) == "remote_value"
    end

    test "pulled data is cached locally for subsequent reads", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node
      :ok = :rpc.call(remote_node, ReplicatedCache, :put, [name, :cached_pull, "value", []])

      # First read: pulls from peer
      assert ReplicatedCache.get!(:cached_pull) == "value"

      # Verify it's now in the local cache
      assert ReplicatedCache.has_key?(:cached_pull) == {:ok, true}
    end

    test "pulled data preserves TTL from peer", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node with a TTL
      :ok =
        :rpc.call(remote_node, ReplicatedCache, :put, [
          name,
          :ttl_pull_key,
          "value",
          [ttl: :timer.seconds(60)]
        ])

      # Pull from peer via get
      assert ReplicatedCache.get!(:ttl_pull_key) == "value"

      # Verify the local copy has a TTL (not :infinity)
      assert {:ok, ttl} = ReplicatedCache.ttl(:ttl_pull_key)
      assert is_integer(ttl) and ttl > 0 and ttl <= :timer.seconds(60)
    end

    test "returns nil when no peer has the data" do
      # No node has this key
      assert ReplicatedCache.get(:missing_key) == {:ok, nil}
    end

    test "take pulls from peer and returns value", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node
      :ok = :rpc.call(remote_node, ReplicatedCache, :put, [name, :take_key, "take_value", []])

      # Take from primary (should pull from peer)
      assert ReplicatedCache.take!(:take_key) == "take_value"
    end

    test "has_key? checks peers when key is not local", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node only
      :ok = :rpc.call(remote_node, ReplicatedCache, :put, [name, :hk_key, "value", []])

      # Primary doesn't have it locally, but has_key? should find it on a peer
      assert ReplicatedCache.has_key?(:hk_key) == {:ok, true}
    end

    test "has_key? returns false when no peer has the key" do
      assert ReplicatedCache.has_key?(:nonexistent_key) == {:ok, false}
    end

    test "ttl checks peers when key is not local", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node with a TTL
      :ok =
        :rpc.call(remote_node, ReplicatedCache, :put, [
          name,
          :ttl_key,
          "value",
          [ttl: :timer.seconds(60)]
        ])

      # Primary doesn't have it locally, but ttl should find it on a peer
      assert {:ok, ttl} = ReplicatedCache.ttl(:ttl_key)
      assert is_integer(ttl) and ttl > 0
    end

    test "ttl returns error when no peer has the key" do
      assert {:error, %Nebulex.KeyError{key: :missing_ttl_key}} =
               ReplicatedCache.ttl(:missing_ttl_key)
    end

    test "get_all pulls missing keys from peers", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put some keys on remote node only
      :ok = :rpc.call(remote_node, ReplicatedCache, :put, [name, :ga_remote1, "r1", []])
      :ok = :rpc.call(remote_node, ReplicatedCache, :put, [name, :ga_remote2, "r2", []])

      # Put one key locally
      :ok = ReplicatedCache.put(:ga_local, "local")

      # get_all with all three keys - should pull missing from peer
      assert {:ok, results} = ReplicatedCache.get_all(in: [:ga_local, :ga_remote1, :ga_remote2])
      assert Map.new(results) == %{ga_local: "local", ga_remote1: "r1", ga_remote2: "r2"}
    end

    test "get_all pulls missing keys and caches them locally", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node only
      :ok = :rpc.call(remote_node, ReplicatedCache, :put, [name, :ga_cached, "cached_val", []])

      # get_all triggers pull
      assert ReplicatedCache.get_all!(in: [:ga_cached]) == [{:ga_cached, "cached_val"}]

      # Now it should be cached locally
      assert ReplicatedCache.has_key?(:ga_cached) == {:ok, true}
    end

    test "get_all with select: :value pulls from peers", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      :ok = :rpc.call(remote_node, ReplicatedCache, :put, [name, :ga_val_key, "val", []])

      assert {:ok, values} = ReplicatedCache.get_all(in: [:ga_val_key], select: :value)
      assert "val" in values
    end

    test "get_all with select: :key pulls from peers", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      :ok = :rpc.call(remote_node, ReplicatedCache, :put, [name, :ga_key_only, "val", []])

      assert {:ok, keys} = ReplicatedCache.get_all(in: [:ga_key_only], select: :key)
      assert :ga_key_only in keys
    end
  end

  describe "distributed invalidation" do
    test "write invalidates entry on remote nodes", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put a value on remote node first
      :ok = :rpc.call(remote_node, ReplicatedCache, :put, [name, :inv_key, "remote_value", []])

      assert :rpc.call(remote_node, ReplicatedCache, :get!, [name, :inv_key, nil, []]) ==
               "remote_value"

      # Now put on primary node - this should trigger invalidation on remote
      assert ReplicatedCache.put(:inv_key, "primary_value") == :ok
      assert ReplicatedCache.get!(:inv_key) == "primary_value"

      # Wait for invalidation to propagate, then remote should pull new value
      assert_eventually fn ->
        assert :rpc.call(remote_node, ReplicatedCache, :get!, [name, :inv_key, nil, []]) ==
                 "primary_value"
      end
    end

    test "write + read from another node returns new value via pull", %{
      name: name,
      cluster: cluster
    } do
      remote_node = find_remote_node(cluster)

      # Put on primary node
      assert ReplicatedCache.put(:sync_key, "primary_value") == :ok

      # Wait for invalidation to propagate (in case remote had old data)
      :ok = Process.sleep(100)

      # Remote node reads - should pull from primary
      assert :rpc.call(remote_node, ReplicatedCache, :get!, [name, :sync_key, nil, []]) ==
               "primary_value"
    end

    test "delete invalidates on remote nodes", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node
      :ok = :rpc.call(remote_node, ReplicatedCache, :put, [name, :del_key, "value", []])

      assert :rpc.call(remote_node, ReplicatedCache, :get!, [name, :del_key, nil, []]) == "value"

      # Delete on primary
      :ok = ReplicatedCache.delete(:del_key)

      # Wait for invalidation
      assert_eventually fn ->
        assert :rpc.call(remote_node, ReplicatedCache, :get, [name, :del_key, nil, []]) ==
                 {:ok, nil}
      end
    end
  end

  describe "info" do
    test "returns cache info" do
      assert ReplicatedNilCache.info!() != %{}
    end
  end

  ## Private functions

  defp find_remote_node(cluster) do
    cluster
    |> Enum.reject(&(&1 == node()))
    |> Enum.random()
  end
end
