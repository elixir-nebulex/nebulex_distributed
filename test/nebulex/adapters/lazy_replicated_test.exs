defmodule Nebulex.Adapters.LazyReplicatedCacheTest do
  use Nebulex.NodeCase

  # Inherit tests
  use Nebulex.CacheTestCase

  import Nebulex.CacheCase

  alias Nebulex.Adapter
  alias Nebulex.Distributed.TestCache.{LazyReplicatedCache, LazyReplicatedNilCache}

  @moduletag capture_log: true

  @primary :"primary@127.0.0.1"
  @cache_name :lazy_replicated_cache

  setup do
    cluster = :lists.usort([@primary | Application.get_env(:nebulex_distributed, :nodes, [])])
    nodes = [node() | Node.list()]

    node_pid_list =
      start_caches(
        nodes,
        [{LazyReplicatedCache, name: @cache_name}, {LazyReplicatedNilCache, []}]
      )

    # Wait for caches to be ready on all nodes
    :ok = Process.sleep(100)

    default_dynamic_cache = LazyReplicatedCache.get_dynamic_cache()
    _ = LazyReplicatedCache.put_dynamic_cache(@cache_name)

    on_exit(fn ->
      _ = LazyReplicatedCache.put_dynamic_cache(default_dynamic_cache)

      :ok = Process.sleep(100)

      stop_caches(node_pid_list)
    end)

    {:ok,
     cache: LazyReplicatedCache,
     name: @cache_name,
     cluster: cluster,
     nodes: nodes,
     nil_cache: LazyReplicatedNilCache}
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
            adapter: Nebulex.Adapters.LazyReplicated,
            adapter_opts: [primary_storage_adapter: Invalid]
        end
      end
    end
  end

  describe "cluster membership" do
    test "nodes returns all cluster nodes", %{nodes: nodes} do
      assert LazyReplicatedCache.nodes() |> Enum.sort() == Enum.sort(nodes)
    end

    test "join and leave cluster", %{nodes: nodes, name: name} do
      assert LazyReplicatedCache.nodes() |> Enum.sort() == Enum.sort(nodes)

      LazyReplicatedCache.with_dynamic_cache(name, fn ->
        :ok = LazyReplicatedCache.leave_cluster()

        assert_eventually fn ->
          assert LazyReplicatedCache.nodes() |> Enum.sort() == (nodes -- [node()]) |> Enum.sort()
        end
      end)

      LazyReplicatedCache.with_dynamic_cache(name, fn ->
        :ok = LazyReplicatedCache.join_cluster()

        assert_eventually fn ->
          assert LazyReplicatedCache.nodes() |> Enum.sort() == Enum.sort(nodes)
        end
      end)
    end
  end

  describe "local reads" do
    test "local cache hit returns data immediately" do
      assert LazyReplicatedCache.put("local_key", "local_value") == :ok
      assert LazyReplicatedCache.get!("local_key") == "local_value"
    end

    test "put_all and get work locally" do
      assert LazyReplicatedCache.put_all([{:a, 1}, {:b, 2}, {:c, 3}]) == :ok
      assert LazyReplicatedCache.get!(:a) == 1
      assert LazyReplicatedCache.get!(:b) == 2
      assert LazyReplicatedCache.get!(:c) == 3
    end
  end

  describe "lazy-pull replication" do
    test "cache miss pulls data from peer node", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put a value on a remote node
      :ok = :rpc.call(remote_node, LazyReplicatedCache, :put, [name, :pull_key, "remote_value", []])

      assert :rpc.call(remote_node, LazyReplicatedCache, :get!, [name, :pull_key, nil, []]) ==
               "remote_value"

      # Primary node doesn't have the value locally yet
      # But fetch should pull from the peer transparently
      assert LazyReplicatedCache.get!(:pull_key) == "remote_value"
    end

    test "pulled data is cached locally for subsequent reads", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node
      :ok = :rpc.call(remote_node, LazyReplicatedCache, :put, [name, :cached_pull, "value", []])

      # First read: pulls from peer
      assert LazyReplicatedCache.get!(:cached_pull) == "value"

      # Verify it's now in the local cache
      assert LazyReplicatedCache.has_key?(:cached_pull) == {:ok, true}
    end

    test "pulled data preserves TTL from peer", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node with a TTL
      :ok =
        :rpc.call(remote_node, LazyReplicatedCache, :put, [
          name,
          :ttl_pull_key,
          "value",
          [ttl: :timer.seconds(60)]
        ])

      # Pull from peer via get
      assert LazyReplicatedCache.get!(:ttl_pull_key) == "value"

      # Verify the local copy has a TTL (not :infinity)
      assert {:ok, ttl} = LazyReplicatedCache.ttl(:ttl_pull_key)
      assert is_integer(ttl) and ttl > 0 and ttl <= :timer.seconds(60)
    end

    test "returns nil when no peer has the data" do
      # No node has this key
      assert LazyReplicatedCache.get(:missing_key) == {:ok, nil}
    end

    test "take pulls from peer and returns value", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node
      :ok = :rpc.call(remote_node, LazyReplicatedCache, :put, [name, :take_key, "take_value", []])

      # Take from primary (should pull from peer)
      assert LazyReplicatedCache.take!(:take_key) == "take_value"
    end

    test "has_key? checks peers when key is not local", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node only
      :ok = :rpc.call(remote_node, LazyReplicatedCache, :put, [name, :hk_key, "value", []])

      # Primary doesn't have it locally, but has_key? should find it on a peer
      assert LazyReplicatedCache.has_key?(:hk_key) == {:ok, true}
    end

    test "has_key? returns false when no peer has the key" do
      assert LazyReplicatedCache.has_key?(:nonexistent_key) == {:ok, false}
    end

    test "ttl checks peers when key is not local", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node with a TTL
      :ok =
        :rpc.call(remote_node, LazyReplicatedCache, :put, [
          name,
          :ttl_key,
          "value",
          [ttl: :timer.seconds(60)]
        ])

      # Primary doesn't have it locally, but ttl should find it on a peer
      assert {:ok, ttl} = LazyReplicatedCache.ttl(:ttl_key)
      assert is_integer(ttl) and ttl > 0
    end

    test "ttl returns error when no peer has the key" do
      assert {:error, %Nebulex.KeyError{key: :missing_ttl_key}} =
               LazyReplicatedCache.ttl(:missing_ttl_key)
    end

    test "get_all pulls missing keys from peers", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put some keys on remote node only
      :ok = :rpc.call(remote_node, LazyReplicatedCache, :put, [name, :ga_remote1, "r1", []])
      :ok = :rpc.call(remote_node, LazyReplicatedCache, :put, [name, :ga_remote2, "r2", []])

      # Put one key locally
      :ok = LazyReplicatedCache.put(:ga_local, "local")

      # get_all with all three keys - should pull missing from peer
      assert {:ok, results} = LazyReplicatedCache.get_all(in: [:ga_local, :ga_remote1, :ga_remote2])
      assert Map.new(results) == %{ga_local: "local", ga_remote1: "r1", ga_remote2: "r2"}
    end

    test "get_all pulls missing keys and caches them locally", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node only
      :ok = :rpc.call(remote_node, LazyReplicatedCache, :put, [name, :ga_cached, "cached_val", []])

      # get_all triggers pull
      assert LazyReplicatedCache.get_all!(in: [:ga_cached]) == [{:ga_cached, "cached_val"}]

      # Now it should be cached locally
      assert LazyReplicatedCache.has_key?(:ga_cached) == {:ok, true}
    end

    test "get_all with select: :value pulls from peers", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      :ok = :rpc.call(remote_node, LazyReplicatedCache, :put, [name, :ga_val_key, "val", []])

      assert {:ok, values} = LazyReplicatedCache.get_all(in: [:ga_val_key], select: :value)
      assert "val" in values
    end

    test "get_all with select: :key pulls from peers", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      :ok = :rpc.call(remote_node, LazyReplicatedCache, :put, [name, :ga_key_only, "val", []])

      assert {:ok, keys} = LazyReplicatedCache.get_all(in: [:ga_key_only], select: :key)
      assert :ga_key_only in keys
    end
  end

  describe "distributed invalidation" do
    test "write invalidates entry on remote nodes", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put a value on remote node first
      :ok = :rpc.call(remote_node, LazyReplicatedCache, :put, [name, :inv_key, "remote_value", []])

      assert :rpc.call(remote_node, LazyReplicatedCache, :get!, [name, :inv_key, nil, []]) ==
               "remote_value"

      # Now put on primary node - this should trigger invalidation on remote
      assert LazyReplicatedCache.put(:inv_key, "primary_value") == :ok
      assert LazyReplicatedCache.get!(:inv_key) == "primary_value"

      # Wait for invalidation to propagate, then remote should pull new value
      assert_eventually fn ->
        assert :rpc.call(remote_node, LazyReplicatedCache, :get!, [name, :inv_key, nil, []]) ==
                 "primary_value"
      end
    end

    test "write + read from another node returns new value via pull", %{
      name: name,
      cluster: cluster
    } do
      remote_node = find_remote_node(cluster)

      # Put on primary node
      assert LazyReplicatedCache.put(:sync_key, "primary_value") == :ok

      # Wait for invalidation to propagate (in case remote had old data)
      :ok = Process.sleep(100)

      # Remote node reads - should pull from primary
      assert :rpc.call(remote_node, LazyReplicatedCache, :get!, [name, :sync_key, nil, []]) ==
               "primary_value"
    end

    test "delete invalidates on remote nodes", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node
      :ok = :rpc.call(remote_node, LazyReplicatedCache, :put, [name, :del_key, "value", []])

      assert :rpc.call(remote_node, LazyReplicatedCache, :get!, [name, :del_key, nil, []]) ==
               "value"

      # Delete on primary
      :ok = LazyReplicatedCache.delete(:del_key)

      # Wait for invalidation
      assert_eventually fn ->
        assert :rpc.call(remote_node, LazyReplicatedCache, :get, [name, :del_key, nil, []]) ==
                 {:ok, nil}
      end
    end
  end

  describe "info" do
    test "returns cache info" do
      assert LazyReplicatedNilCache.info!() != %{}
    end
  end

  ## Private functions

  defp find_remote_node(cluster) do
    cluster
    |> Enum.reject(&(&1 == node()))
    |> Enum.random()
  end
end
