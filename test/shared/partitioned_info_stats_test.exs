defmodule Nebulex.Adapters.PartitionedInfoStatsTest do
  import Nebulex.CacheCase, only: [deftests: 1]

  deftests do
    import Nebulex.CacheCase, only: [t_sleep: 1, wait_until: 1]

    describe "stats:" do
      test "hits and misses", %{cache: cache} do
        :ok = cache.put_all!(a: 1, b: 2)

        assert cache.get!(:a) == 1
        assert cache.has_key?(:a)
        assert cache.ttl!(:b) == :infinity
        refute cache.get!(:c)
        refute cache.get!(:d)

        assert cache.get_all!(in: [:a, :b, :c, :d]) |> Map.new() == %{a: 1, b: 2}

        assert cache.info!(:stats) == %{
                 hits: 5,
                 misses: 4,
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
                   hits: 0,
                   misses: 1,
                   writes: 10,
                   evictions: 1,
                   expirations: 1,
                   deletions: 1,
                   updates: 4
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
                 hits: 1,
                 misses: 1,
                 writes: 10,
                 evictions: 0,
                 expirations: 0,
                 deletions: 2,
                 updates: 0
               }

        assert cache.delete_all!() == 8

        assert cache.info!(:stats) == %{
                 hits: 1,
                 misses: 1,
                 writes: 10,
                 evictions: 0,
                 expirations: 0,
                 deletions: 10,
                 updates: 0
               }
      end

      test "expirations", %{cache: cache} do
        :ok = cache.put_all!(a: 1, b: 2)
        :ok = cache.put_all!([c: 3, d: 4], ttl: 1000)

        assert cache.get_all!(in: [:a, :b, :c, :d]) |> Map.new() == %{a: 1, b: 2, c: 3, d: 4}

        _ = t_sleep(1100)

        # Fetching the expired entries removes them and counts the expirations
        refute cache.get!(:c)
        refute cache.get!(:d)

        wait_until fn ->
          assert cache.info!(:stats) == %{
                   hits: 4,
                   misses: 2,
                   writes: 4,
                   evictions: 2,
                   expirations: 2,
                   deletions: 2,
                   updates: 0
                 }
        end
      end
    end
  end
end
