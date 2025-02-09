defmodule Nebulex.Adapters.MultilevelExclusiveTest do
  use Nebulex.NodeCase

  use Nebulex.CacheTestCase,
    except: [
      Nebulex.Cache.QueryableTest,
      Nebulex.Cache.QueryableExpirationTest,
      Nebulex.Cache.QueryableQueryErrorTest,
      Nebulex.Cache.ObservableTest
    ]

  use Nebulex.DistributedTest
  use Nebulex.MultilevelTest
  use Nebulex.MultilevelQueryableTest
  use Nebulex.MultilevelInfoTest

  import Nebulex.CacheCase

  alias Nebulex.Distributed.Cluster
  alias Nebulex.Distributed.TestCache.{Multilevel, MultilevelNil}
  alias Nebulex.Distributed.TestCache.Multilevel.{L1, L2, L3}

  @gc_interval :timer.hours(1)

  @levels [
    {
      L1,
      name: :multilevel_exclusive_l1, gc_interval: @gc_interval
    },
    {
      L2,
      name: :multilevel_exclusive_l2, gc_interval: @gc_interval
    },
    {
      L3,
      name: :multilevel_exclusive_l3, primary: [gc_interval: @gc_interval]
    }
  ]

  setup_with_dynamic_cache Multilevel, :multilevel_exclusive,
    inclusion_policy: :exclusive,
    levels: @levels

  setup do
    {:ok, pid} =
      MultilevelNil.start_link(
        name: :multilevel_exclusive_nil,
        inclusion_policy: :exclusive,
        levels: [
          {MultilevelNil.L1, name: :multilevel_exclusive_nil_l1, gc_interval: @gc_interval},
          {MultilevelNil.L2, name: :multilevel_exclusive_nil_l2}
        ]
      )

    default_dynamic_cache = MultilevelNil.get_dynamic_cache()
    _ = MultilevelNil.put_dynamic_cache(:multilevel_exclusive_nil)

    on_exit(fn ->
      try do
        safe_stop(pid)
      after
        MultilevelNil.put_dynamic_cache(default_dynamic_cache)
      end
    end)

    {:ok, nil_cache: MultilevelNil}
  end

  describe "multilevel exclusive" do
    test "policy" do
      assert Multilevel.inclusion_policy(:multilevel_exclusive) == :exclusive
      assert MultilevelNil.inclusion_policy(:multilevel_exclusive_nil) == :exclusive
    end

    test "get" do
      :ok = Multilevel.put(1, 1, level: 1)
      :ok = Multilevel.put(2, 2, level: 2)
      :ok = Multilevel.put(3, 3, level: 3)

      assert Multilevel.get!(1) == 1
      assert Multilevel.get!(2, return: :key) == 2
      assert Multilevel.get!(3) == 3
      refute Multilevel.get!(2, nil, level: 1)
      refute Multilevel.get!(3, nil, level: 1)
      refute Multilevel.get!(1, nil, level: 2)
      refute Multilevel.get!(3, nil, level: 2)
      refute Multilevel.get!(1, nil, level: 3)
      refute Multilevel.get!(2, nil, level: 3)
    end
  end

  describe "distributed levels" do
    test "return cluster nodes" do
      assert Cluster.get_nodes(:multilevel_exclusive_l3) == [node()]
    end

    test "joining new node" do
      node = :"node1@127.0.0.1"

      {:ok, pid} =
        start_cache(node, Multilevel,
          name: :multilevel_exclusive,
          inclusion_policy: :exclusive,
          levels: @levels
        )

      # check cluster nodes
      assert Cluster.get_nodes(:multilevel_exclusive_l3) == [node, node()]

      kv_pairs = for k <- 1..100, do: {k, k}

      Multilevel.transaction(fn ->
        assert Multilevel.put_all(kv_pairs) == :ok

        for k <- 1..100 do
          assert Multilevel.get!(k) == k
        end
      end)

      :ok = stop_cache(:"node1@127.0.0.1", pid)
    end
  end

  describe "stats:" do
    test "hits and misses", %{cache: cache} do
      :ok = Enum.each(1..2, &Multilevel.put(&1, &1, level: &1))

      assert cache.get!(1) == 1
      assert cache.has_key?(1)
      assert cache.ttl!(2) == :infinity
      refute cache.get!(3)
      refute cache.get!(4)

      assert cache.get_all!(in: [1, 2, 3, 4]) |> Map.new() == %{1 => 1, 2 => 2}

      assert cache.info!(:stats, level: 1) == %{
               # Hits: key 1 is hit 3 times
               hits: 3,
               # Misses: 3 (twice), 4 (twice), and 2 (twice - is not replicated on ttl)
               misses: 6,
               # Writes: key 1
               writes: 1,
               evictions: 0,
               expirations: 0,
               deletions: 0,
               updates: 0
             }

      assert cache.info!(:stats, level: 2) == %{
               # Hits: key 2 is hit three times (once on ttl and once on fetch)
               hits: 2,
               # Misses: 3 (twice), 4 (twice)
               misses: 4,
               # Writes: key 2
               writes: 1,
               evictions: 0,
               expirations: 0,
               deletions: 0,
               updates: 0
             }

      assert cache.info!(:stats, level: 3) == %{
               hits: 0,
               # Misses: 3 (twice), 4 (twice)
               misses: 4,
               writes: 0,
               evictions: 0,
               expirations: 0,
               deletions: 0,
               updates: 0
             }

      assert cache.info!(:stats) == %{
               hits: 5,
               misses: 14,
               writes: 2,
               evictions: 0,
               expirations: 0,
               deletions: 0,
               updates: 0
             }
    end

    test "writes and updates", %{cache: cache} do
      assert cache.put_all!(a: 1, b: 2) == :ok
      assert cache.put_all(%{a: 1, b: 2}) == :ok
      refute cache.put_new_all!(a: 1, b: 2)
      assert cache.put_new_all!(c: 3, d: 4, e: 3)
      assert cache.put!(1, 1) == :ok
      refute cache.put_new!(1, 2)
      refute cache.replace!(2, 2)
      assert cache.put_new!(2, 2)
      assert cache.replace!(2, 22)
      assert cache.incr!(:counter) == 1
      assert cache.incr!(:counter) == 2
      refute cache.expire!(:f, 1000)
      assert cache.expire!(:a, 1000)
      refute cache.touch!(:f)
      assert cache.touch!(:b)

      _ = t_sleep(1100)

      refute cache.get!(:a)

      wait_until fn ->
        assert cache.info!(:stats) == %{
                 deletions: 3,
                 evictions: 3,
                 expirations: 3,
                 hits: 0,
                 misses: 3,
                 updates: 12,
                 writes: 30
               }
      end
    end

    test "deletions", %{cache: cache} do
      entries = for x <- 1..10, do: {x, x}
      :ok = cache.put_all!(entries)

      assert cache.delete!(1) == :ok
      assert cache.take!(2) == 2

      assert_raise Nebulex.KeyError, fn ->
        cache.take!(20)
      end

      assert cache.info!(:stats) == %{
               deletions: 6,
               evictions: 0,
               expirations: 0,
               hits: 1,
               misses: 3,
               updates: 0,
               writes: 30
             }

      assert cache.delete_all!() == 24

      assert cache.info!(:stats) == %{
               deletions: 30,
               evictions: 0,
               expirations: 0,
               hits: 1,
               misses: 3,
               updates: 0,
               writes: 30
             }
    end

    test "expirations", %{cache: cache} do
      :ok = cache.put_all!(a: 1, b: 2)
      :ok = cache.put_all!([c: 3, d: 4], ttl: 1000)

      assert cache.get_all!(in: [:a, :b, :c, :d]) |> Map.new() == %{a: 1, b: 2, c: 3, d: 4}

      _ = t_sleep(1100)

      # The `get_all` doesn't trigger the expiration
      assert cache.get_all!(in: [:a, :b, :c, :d]) |> Map.new() == %{a: 1, b: 2}

      # The `get` will trigger the expiration
      refute cache.get!(:c)
      refute cache.get!(:d)

      wait_until fn ->
        assert cache.info!(:stats) == %{
                 deletions: 6,
                 evictions: 6,
                 expirations: 6,
                 hits: 6,
                 misses: 12,
                 updates: 0,
                 writes: 12
               }
      end
    end
  end
end
