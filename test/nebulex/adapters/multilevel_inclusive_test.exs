defmodule Nebulex.Adapters.MultilevelInclusiveTest do
  use Nebulex.NodeCase
  use Mimic

  use Nebulex.CacheTestCase,
    except: [
      Nebulex.Cache.QueryableTest,
      Nebulex.Cache.QueryableExpirationTest,
      Nebulex.Cache.QueryableQueryErrorTest,
      Nebulex.Cache.ObservableTest
    ]

  use Nebulex.DistributedTest
  use Nebulex.Distributed.TransactionTest
  use Nebulex.MultilevelTest
  use Nebulex.MultilevelQueryableTest
  use Nebulex.MultilevelInfoTest

  import Nebulex.CacheCase
  import Nebulex.Utils, only: [wrap_error: 2]

  alias Nebulex.Distributed.TestCache.{Multilevel, MultilevelNil}
  alias Nebulex.Distributed.TestCache.Multilevel.{L1, L2, L3}

  @gc_interval :timer.hours(1)

  @levels [
    {
      L1,
      name: :multilevel_inclusive_l1, gc_interval: @gc_interval
    },
    {
      L2,
      name: :multilevel_inclusive_l2, gc_interval: @gc_interval
    },
    {
      L3,
      name: :multilevel_inclusive_l3, primary: [gc_interval: @gc_interval]
    }
  ]

  setup_with_dynamic_cache Multilevel, :multilevel_inclusive,
    inclusion_policy: :inclusive,
    levels: @levels

  setup do
    {:ok, pid} =
      MultilevelNil.start_link(
        levels: [
          {MultilevelNil.L1, gc_interval: @gc_interval},
          {MultilevelNil.L2, []}
        ]
      )

    on_exit(fn -> safe_stop(pid) end)

    {:ok, nil_cache: MultilevelNil}
  end

  describe "multilevel inclusive" do
    test "policy" do
      assert Multilevel.inclusion_policy(:multilevel_inclusive) == :inclusive
      assert MultilevelNil.inclusion_policy() == :inclusive
    end

    test "get" do
      :ok = Enum.each(1..3, &Multilevel.put(&1, &1, level: &1))

      assert Multilevel.get!(1) == 1
      refute Multilevel.get!(1, nil, level: 2)
      refute Multilevel.get!(1, nil, level: 3)

      assert Multilevel.get!(2) == 2
      assert Multilevel.get!(2, nil, level: 1) == 2
      assert Multilevel.get!(2, nil, level: 2) == 2
      refute Multilevel.get!(2, nil, level: 3)

      assert Multilevel.get!(3, nil, level: 3) == 3
      refute Multilevel.get!(3, nil, level: 1)
      refute Multilevel.get!(3, nil, level: 2)

      assert Multilevel.get!(3) == 3
      assert Multilevel.get!(3, nil, level: 1) == 3
      assert Multilevel.get!(3, nil, level: 2) == 3
      assert Multilevel.get!(3, nil, level: 3) == 3
    end

    test "get command replicates data with [ttl: :infinity]" do
      :ok = Enum.each(1..3, &Multilevel.put(&1, &1, level: &1))

      L1
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:error, %Nebulex.KeyError{key: 3}}
      end)

      L2
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:error, %Nebulex.KeyError{key: 3}}
      end)

      L3
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:ok, 3}
      end)
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:error, %Nebulex.KeyError{key: 3}}
      end)

      assert Multilevel.get!(3) == 3
    end

    test "ttl command fails when replicating data" do
      :ok = Enum.each(1..3, &Multilevel.put(&1, &1, level: &1))

      L1
      |> expect(:with_dynamic_cache, fn _, _ ->
        wrap_error Nebulex.KeyError, key: 3
      end)

      L2
      |> expect(:with_dynamic_cache, fn _, _ ->
        wrap_error Nebulex.KeyError, key: 3
      end)

      L3
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:ok, 3}
      end)
      |> expect(:with_dynamic_cache, fn _, _ ->
        wrap_error Nebulex.Error, reason: :replication_failed
      end)

      assert_raise Nebulex.Error, ~r"command failed with reason: :replication_failed", fn ->
        Multilevel.get!(3)
      end
    end

    test "put command fails when replicating data" do
      :ok = Enum.each(1..3, &Multilevel.put(&1, &1, level: &1))

      L1
      |> expect(:with_dynamic_cache, fn _, _ ->
        wrap_error Nebulex.KeyError, key: 3
      end)

      L2
      |> expect(:with_dynamic_cache, fn _, _ ->
        wrap_error Nebulex.KeyError, key: 3
      end)

      L3
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:ok, 3}
      end)
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:ok, :infinity}
      end)

      L1
      |> expect(:with_dynamic_cache, fn _, _ ->
        wrap_error Nebulex.Error, reason: :replication_failed
      end)

      assert_raise Nebulex.Error, ~r"command failed with reason: :replication_failed", fn ->
        Multilevel.get!(3)
      end
    end

    test "get_all level error" do
      L1
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:ok, [{1, 1}]}
      end)

      L2
      |> expect(:with_dynamic_cache, fn _, _ ->
        {:ok, [{2, 2}]}
      end)

      L3
      |> expect(:with_dynamic_cache, fn _, _ ->
        wrap_error Nebulex.Error, reason: :error
      end)

      assert_raise Nebulex.Error, ~r"command failed with reason: :error", fn ->
        Multilevel.get_all!([in: [1, 2, 3]], replicate: false)
      end
    end

    test "get_all [replicate: true]" do
      :ok = Enum.each(1..3, &Multilevel.put(&1, &1, level: &1))

      assert Multilevel.get_all!(in: [1]) |> Map.new() == %{1 => 1}
      refute Multilevel.get!(1, nil, level: 2)
      refute Multilevel.get!(1, nil, level: 3)

      assert Multilevel.get_all!(in: [1, 2]) |> Map.new() == %{1 => 1, 2 => 2}
      assert Multilevel.get!(2, nil, level: 1) == 2
      assert Multilevel.get!(2, nil, level: 2) == 2
      refute Multilevel.get!(2, nil, level: 3)

      assert Multilevel.get!(3, nil, level: 3) == 3
      refute Multilevel.get!(3, nil, level: 1)
      refute Multilevel.get!(3, nil, level: 2)

      assert Multilevel.get_all!(in: [1, 2, 3]) |> Map.new() == %{1 => 1, 2 => 2, 3 => 3}
      assert Multilevel.get!(3, nil, level: 1) == 3
      assert Multilevel.get!(3, nil, level: 2) == 3
      assert Multilevel.get!(3, nil, level: 3) == 3
    end

    test "get_all [replicate: false]" do
      :ok = Enum.each(1..3, &Multilevel.put(&1, &1, level: &1))

      assert Multilevel.get_all!([in: [1]], replicate: false) |> Map.new() == %{1 => 1}
      refute Multilevel.get!(1, nil, level: 2)
      refute Multilevel.get!(1, nil, level: 3)

      assert Multilevel.get_all!([in: [1, 2]], replicate: false) |> Map.new() == %{1 => 1, 2 => 2}
      refute Multilevel.get!(2, nil, level: 1)
      assert Multilevel.get!(2, nil, level: 2) == 2
      refute Multilevel.get!(2, nil, level: 3)

      assert Multilevel.get!(3, nil, level: 3) == 3
      refute Multilevel.get!(3, nil, level: 1)
      refute Multilevel.get!(3, nil, level: 2)

      assert Multilevel.get_all!([in: [1, 2, 3]], replicate: false) |> Map.new() == %{
               1 => 1,
               2 => 2,
               3 => 3
             }

      refute Multilevel.get!(3, nil, level: 1)
      refute Multilevel.get!(3, nil, level: 2)
      assert Multilevel.get!(3, nil, level: 3) == 3
    end

    test "get boolean" do
      :ok = Multilevel.put(1, true, level: 1)
      :ok = Multilevel.put(2, false, level: 1)

      assert Multilevel.get!(1) == true
      assert Multilevel.get!(2) == false
    end

    test "fetched value is replicated with TTL on previous levels" do
      assert Multilevel.put(:a, 1, ttl: 1000) == :ok
      assert Multilevel.ttl!(:a) > 0

      _ = t_sleep(1100)

      refute Multilevel.get!(:a, nil, level: 1)
      refute Multilevel.get!(:a, nil, level: 2)
      refute Multilevel.get!(:a, nil, level: 3)

      assert Multilevel.put(:b, 1, level: 3) == :ok
      assert Multilevel.ttl!(:b) == :infinity
      assert Multilevel.expire!(:b, 1000)
      assert Multilevel.ttl!(:b) > 0
      refute Multilevel.get!(:b, nil, level: 1)
      refute Multilevel.get!(:b, nil, level: 2)
      assert Multilevel.get!(:b, nil, level: 3) == 1

      assert Multilevel.get!(:b) == 1
      assert Multilevel.get!(:b, nil, level: 1) == 1
      assert Multilevel.get!(:b, nil, level: 2) == 1
      assert Multilevel.get!(:b, nil, level: 3) == 1

      _ = t_sleep(1100)

      refute Multilevel.get!(:b, nil, level: 1)
      refute Multilevel.get!(:b, nil, level: 2)
      refute Multilevel.get!(:b, nil, level: 3)
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
               # Writes: key 1 and key 2 (replicated on get_all)
               writes: 2,
               evictions: 0,
               expirations: 0,
               deletions: 0,
               updates: 0
             }

      assert cache.info!(:stats, level: 2) == %{
               # Hits: key 2 is hit three times (twice on fetch and once on ttl)
               hits: 3,
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
               hits: 6,
               misses: 14,
               writes: 3,
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

  describe "distributed levels" do
    test "return cluster nodes" do
      assert L3.nodes(:multilevel_inclusive_l3) == [node()]
    end

    test "joining new node" do
      node = :"node1@127.0.0.1"

      {:ok, pid} =
        start_cache(node, Multilevel,
          name: :multilevel_inclusive,
          inclusion_policy: :inclusive,
          levels: @levels
        )

      # check cluster nodes
      assert L3.nodes(:multilevel_inclusive_l3) == [node, node()]

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
end
