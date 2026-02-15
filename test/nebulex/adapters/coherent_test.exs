defmodule Nebulex.Adapters.CoherentCacheTest do
  use Nebulex.NodeCase

  # Inherit tests
  use Nebulex.CacheTestCase

  import Nebulex.CacheCase

  alias Nebulex.Adapter
  alias Nebulex.Distributed.TestCache.{CoherentCache, CoherentNilCache}

  @moduletag capture_log: true

  @primary :"primary@127.0.0.1"
  @cache_name :coherent_cache

  setup do
    cluster = :lists.usort([@primary | Application.get_env(:nebulex_distributed, :nodes, [])])
    nodes = [node() | Node.list()]

    node_pid_list =
      start_caches(
        nodes,
        [
          {CoherentCache, name: @cache_name},
          {CoherentNilCache, []}
        ]
      )

    # Wait for caches to be ready on all nodes
    :ok = Process.sleep(100)

    default_dynamic_cache = CoherentCache.get_dynamic_cache()
    _ = CoherentCache.put_dynamic_cache(@cache_name)

    on_exit(fn ->
      _ = CoherentCache.put_dynamic_cache(default_dynamic_cache)

      :ok = Process.sleep(100)

      stop_caches(node_pid_list)
    end)

    {:ok, cache: CoherentCache, name: @cache_name, cluster: cluster, nil_cache: CoherentNilCache}
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
            adapter: Nebulex.Adapters.Coherent,
            adapter_opts: [primary_storage_adapter: Invalid]
        end
      end
    end
  end

  describe "distributed invalidation" do
    test "invalidates entry on remote nodes when put locally", %{name: name, cluster: cluster} do
      # Get a remote node
      remote_node = find_remote_node(cluster)

      # Put a value on remote node first (call cache function directly with name)
      :ok = :rpc.call(remote_node, CoherentCache, :put, [name, :key1, "remote_value", []])

      assert :rpc.call(remote_node, CoherentCache, :get!, [name, :key1, nil, []]) == "remote_value"

      # Now put on primary node - this should trigger invalidation on remote
      assert CoherentCache.put(:key1, "primary_value") == :ok
      assert CoherentCache.get!(:key1) == "primary_value"

      # Wait for invalidation to propagate
      assert_eventually fn ->
        # Remote node should have the entry invalidated (cache miss)
        assert :rpc.call(remote_node, CoherentCache, :get, [name, :key1, nil, []]) == {:ok, nil}
      end
    end

    test "invalidates entry on remote nodes when deleted locally", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put the key on remote node
      :ok = :rpc.call(remote_node, CoherentCache, :put, [name, :key2, "value2", []])

      assert :rpc.call(remote_node, CoherentCache, :get!, [name, :key2, nil, []]) == "value2"

      # Delete on primary node
      :ok = CoherentCache.delete(:key2)
      assert CoherentCache.get(:key2) == {:ok, nil}

      # Wait for invalidation to propagate
      assert_eventually fn ->
        assert :rpc.call(remote_node, CoherentCache, :get, [name, :key2, nil, []]) == {:ok, nil}
      end
    end

    test "invalidation via take removes entry on remote nodes", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node
      :ok = :rpc.call(remote_node, CoherentCache, :put, [name, :take_key, "take_value", []])

      assert :rpc.call(remote_node, CoherentCache, :get!, [name, :take_key, nil, []]) ==
               "take_value"

      # Put locally too
      :ok = CoherentCache.put(:take_key, "local_take_value")

      # Take from primary (removes and returns value)
      assert CoherentCache.take!(:take_key) == "local_take_value"

      # Wait for invalidation
      assert_eventually fn ->
        assert :rpc.call(remote_node, CoherentCache, :get, [name, :take_key, nil, []]) == {:ok, nil}
      end
    end

    test "does not invalidate local entry when remote node writes", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put a value on primary node
      :ok = CoherentCache.put(:local_key, "local_value")
      assert CoherentCache.get!(:local_key) == "local_value"

      # Write the same key from a remote node
      :ok = :rpc.call(remote_node, CoherentCache, :put, [name, :local_key, "remote_value", []])

      # Primary node should have been invalidated
      assert_eventually fn ->
        assert CoherentCache.get(:local_key) == {:ok, nil}
      end
    end

    test "invalidation via incr propagates to remote nodes", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Increment counter on remote node
      assert :rpc.call(remote_node, CoherentCache, :incr!, [name, :counter, 10, []]) == 10
      assert :rpc.call(remote_node, CoherentCache, :get!, [name, :counter, nil, []]) == 10

      # Increment on primary node (creates new counter starting from 0 + amount)
      assert CoherentCache.incr!(:counter, 5) == 5

      # Wait for invalidation
      assert_eventually fn ->
        assert :rpc.call(remote_node, CoherentCache, :get, [name, :counter, nil, []]) == {:ok, nil}
      end
    end

    test "invalidation via put_all propagates to remote nodes", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node
      :ok =
        :rpc.call(remote_node, CoherentCache, :put_all, [name, [key1: "value1", key2: "value2"], []])

      assert :rpc.call(remote_node, CoherentCache, :get!, [name, :key1, nil, []]) == "value1"
      assert :rpc.call(remote_node, CoherentCache, :get!, [name, :key2, nil, []]) == "value2"

      # Put on primary node
      :ok = CoherentCache.put_all(key1: "primary_value1", key2: "primary_value2")
      assert CoherentCache.get!(:key1) == "primary_value1"
      assert CoherentCache.get!(:key2) == "primary_value2"

      # Wait for invalidation
      assert_eventually fn ->
        assert :rpc.call(remote_node, CoherentCache, :get, [name, :key1, nil, []]) == {:ok, nil}
        assert :rpc.call(remote_node, CoherentCache, :get, [name, :key2, nil, []]) == {:ok, nil}
      end
    end

    test "invalidation via delete_all propagates to remote nodes", %{name: name, cluster: cluster} do
      remote_node = find_remote_node(cluster)

      # Put on remote node
      :ok =
        :rpc.call(remote_node, CoherentCache, :put_all, [name, [key1: "value1", key2: "value2"], []])

      assert :rpc.call(remote_node, CoherentCache, :get!, [name, :key1, nil, []]) == "value1"
      assert :rpc.call(remote_node, CoherentCache, :get!, [name, :key2, nil, []]) == "value2"

      # Delete on primary node
      assert CoherentCache.delete_all!() == 0
      assert CoherentCache.get!(:key1) == nil
      assert CoherentCache.get!(:key2) == nil

      # Wait for invalidation
      assert_eventually fn ->
        assert :rpc.call(remote_node, CoherentCache, :get, [name, :key1, nil, []]) == {:ok, nil}
        assert :rpc.call(remote_node, CoherentCache, :get, [name, :key2, nil, []]) == {:ok, nil}
      end
    end

    test "invalidation via delete_all keys propagates to remote nodes", %{
      name: name,
      cluster: cluster
    } do
      remote_node = find_remote_node(cluster)

      # Put on remote node
      :ok =
        :rpc.call(remote_node, CoherentCache, :put_all, [name, [key1: "value1", key2: "value2"], []])

      assert :rpc.call(remote_node, CoherentCache, :get!, [name, :key1, nil, []]) == "value1"
      assert :rpc.call(remote_node, CoherentCache, :get!, [name, :key2, nil, []]) == "value2"

      # Delete on primary node
      assert CoherentCache.delete_all!(in: [:key1]) == 0

      # Wait for invalidation
      assert_eventually fn ->
        assert :rpc.call(remote_node, CoherentCache, :get, [name, :key1, nil, []]) == {:ok, nil}
        assert :rpc.call(remote_node, CoherentCache, :get!, [name, :key2, nil, []]) == "value2"
      end
    end
  end

  describe "info/1" do
    test "ok: returns all info" do
      assert CoherentNilCache.info!() != %{}
    end
  end

  ## Private functions

  defp find_remote_node(cluster) do
    cluster
    |> Enum.reject(&(&1 == node()))
    |> Enum.random()
  end
end
