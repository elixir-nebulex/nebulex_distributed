defmodule Nebulex.Adapters.PartitionedNodeFilterTest do
  use Nebulex.NodeCase

  import Nebulex.CacheCase

  alias Nebulex.Distributed.TestCache.{PartitionedCache, TestNodeSelector}

  @moduletag capture_log: true

  @primary :"primary@127.0.0.1"

  describe "node_filter" do
    setup do
      nodes = [node() | Node.list()]

      {:ok, nodes: nodes}
    end

    test "includes all nodes by default", %{nodes: nodes} do
      node_pid_list =
        start_caches(
          nodes,
          [{PartitionedCache, name: :partitioned_no_selector}]
        )

      assert_eventually fn ->
        assert PartitionedCache.nodes(:partitioned_no_selector) |> Enum.sort() == Enum.sort(nodes)
      end

      stop_caches(node_pid_list)
    end

    test "function-based filter excludes nodes from the ring", %{nodes: nodes} do
      node_pid_list =
        start_caches(
          nodes,
          [
            {PartitionedCache,
             name: :partitioned_selector, node_filter: &TestNodeSelector.exclude_primary?/1}
          ]
        )

      expected_ring_nodes = Enum.reject(nodes, &(&1 == @primary))

      assert_eventually fn ->
        ring_nodes = PartitionedCache.nodes(:partitioned_selector)

        assert Enum.sort(ring_nodes) == Enum.sort(expected_ring_nodes)
        refute @primary in ring_nodes
      end

      # The current node (@primary) is excluded from the ring,
      # but it can still read/write via RPC to the ring nodes
      PartitionedCache.with_dynamic_cache(:partitioned_selector, fn ->
        assert PartitionedCache.put("key1", "value1") == :ok
        assert PartitionedCache.get!("key1") == "value1"

        assert PartitionedCache.put("key2", "value2") == :ok
        assert PartitionedCache.get!("key2") == "value2"

        assert PartitionedCache.delete("key1") == :ok
        refute PartitionedCache.get!("key1")
      end)

      stop_caches(node_pid_list)
    end

    test "filter that excludes all nodes results in empty ring", %{nodes: nodes} do
      node_pid_list =
        start_caches(
          nodes,
          [
            {PartitionedCache,
             name: :partitioned_all_excluded, node_filter: &TestNodeSelector.exclude_all?/1}
          ]
        )

      assert_eventually fn ->
        assert PartitionedCache.nodes(:partitioned_all_excluded) == []
      end

      # Operations should fail since no nodes are in the ring
      PartitionedCache.with_dynamic_cache(:partitioned_all_excluded, fn ->
        assert {:error, %Nebulex.Error{}} = PartitionedCache.put("key", "value")
      end)

      stop_caches(node_pid_list)
    end
  end
end
