defmodule Nebulex.Adapters.PartitionedCachexTest do
  use Nebulex.NodeCase
  use Mimic

  use Nebulex.CacheTestCase,
    except: [Nebulex.Cache.QueryableQueryErrorTest, Nebulex.Cache.ObservableTest]

  import Nebulex.CacheCase

  alias Nebulex.Distributed.TestCache.PartitionedCachex

  @moduletag :cachex_test
  @moduletag capture_log: true

  @primary :"primary@127.0.0.1"
  @cache_name :partitioned_cachex

  setup do
    cluster = :lists.usort([@primary | Application.get_env(:nebulex_distributed, :nodes, [])])

    node_pid_list =
      start_caches(
        [node() | Node.list()],
        [{PartitionedCachex, [name: @cache_name, join_timeout: 2000]}]
      )

    default_dynamic_cache = PartitionedCachex.get_dynamic_cache()
    _ = PartitionedCachex.put_dynamic_cache(@cache_name)

    on_exit(fn ->
      _ = PartitionedCachex.put_dynamic_cache(default_dynamic_cache)

      :ok = Process.sleep(100)

      stop_caches(node_pid_list)
    end)

    {:ok, cache: PartitionedCachex, name: @cache_name, cluster: cluster}
  end
end
