defmodule Nebulex.MultilevelTest do
  import Nebulex.CacheCase, only: [deftests: 1]

  deftests do
    import Nebulex.CacheCase, only: [t_sleep: 1]

    describe "c:init/1" do
      test "fails because missing levels config", %{cache: cache} do
        _ = Process.flag(:trap_exit, true)

        assert {:error,
                {%NimbleOptions.ValidationError{
                   message: "required :levels option not found, received options: []"
                 }, _}} = cache.start_link(name: :missing_levels)
      end
    end

    describe "kv" do
      test "put/3", %{cache: cache} do
        assert cache.put!(1, 1) == :ok
        assert cache.fetch!(1, level: 1) == 1
        assert cache.fetch!(1, level: 2) == 1
        assert cache.fetch!(1, level: 3) == 1

        assert cache.put!(2, 2, level: 2) == :ok
        assert cache.fetch!(2, level: 2) == 2
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(2, level: 1) end
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(2, level: 3) end

        assert cache.put!("foo", nil) == :ok
        assert cache.fetch!("foo") == nil
      end

      test "put_new/3", %{cache: cache} do
        assert cache.put_new!(1, 1)
        refute cache.put_new!(1, 2)
        assert cache.fetch!(1, level: 1) == 1
        assert cache.fetch!(1, level: 2) == 1
        assert cache.fetch!(1, level: 3) == 1

        assert cache.put_new!(2, 2, level: 2)
        assert cache.fetch!(2, level: 2) == 2
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(2, level: 1) end
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(2, level: 3) end

        assert cache.put_new!("foo", nil)
        assert cache.fetch!("foo") == nil
      end

      test "put_all/2", %{cache: cache} do
        assert cache.put_all!(
                 for x <- 1..3 do
                   {x, x}
                 end,
                 ttl: 1000
               ) == :ok

        for x <- 1..3 do
          assert cache.fetch!(x) == x
        end

        _ = t_sleep(1100)

        for x <- 1..3 do
          assert_raise Nebulex.KeyError, fn -> cache.fetch!(x) end
        end

        assert cache.put_all!(%{"apples" => 1, "bananas" => 3}) == :ok
        assert cache.put_all!(blueberries: 2, strawberries: 5) == :ok
        assert cache.fetch!("apples") == 1
        assert cache.fetch!("bananas") == 3
        assert cache.fetch!(:blueberries) == 2
        assert cache.fetch!(:strawberries) == 5

        assert cache.put_all!([]) == :ok
        assert cache.put_all!(%{}) == :ok

        refute cache.put_new_all!(%{"apples" => 100})
        assert cache.fetch!("apples") == 1
      end

      test "get_all/2", %{cache: cache} do
        assert cache.put_all!(a: 1, c: 3) == :ok
        assert cache.get_all!(in: [:a, :b, :c]) |> Map.new() == %{a: 1, c: 3}
      end

      test "delete/2", %{cache: cache} do
        assert cache.put!(1, 1)
        assert cache.put!(2, 2, level: 2)

        assert cache.delete!(1) == :ok
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(1, level: 1) end
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(1, level: 2) end
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(1, level: 3) end

        assert cache.delete!(2, level: 2) == :ok
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(2, level: 1) end
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(2, level: 2) end
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(2, level: 3) end
      end

      test "take/2", %{cache: cache} do
        assert cache.put!(1, 1) == :ok
        assert cache.put!(2, 2, level: 2) == :ok
        assert cache.put!(3, 3, level: 3) == :ok

        assert cache.take!(1) == 1
        assert cache.take!(2) == 2
        assert cache.take!(3) == 3

        for {k, l} <- [{1, 1}, {1, 2}, {1, 3}, {2, 2}, {3, 3}] do
          assert_raise Nebulex.KeyError, fn -> cache.fetch!(k, level: l) end
        end
      end

      test "has_key?/1", %{cache: cache} do
        assert cache.put!(1, 1) == :ok
        assert cache.put!(2, 2, level: 2) == :ok
        assert cache.put!(3, 3, level: 3) == :ok

        assert cache.has_key?(1) == {:ok, true}
        assert cache.has_key?(2) == {:ok, true}
        assert cache.has_key?(3) == {:ok, true}
        assert cache.has_key?(4) == {:ok, false}
      end

      test "ttl/1", %{cache: cache} do
        assert cache.put!(:a, 1, ttl: 1000) == :ok
        assert cache.ttl!(:a) > 0
        assert cache.put!(:b, 2) == :ok

        _ = t_sleep(10)

        assert cache.ttl!(:a) > 0
        assert cache.ttl!(:b) == :infinity
        assert_raise Nebulex.KeyError, fn -> cache.ttl!(:c) end

        _ = t_sleep(1100)

        assert_raise Nebulex.KeyError, fn -> cache.ttl!(:a) end
      end

      test "expire/2", %{cache: cache} do
        assert cache.put!(:a, 1) == :ok
        assert cache.ttl!(:a) == :infinity

        assert cache.expire!(:a, 1000)
        ttl = cache.ttl!(:a)
        assert ttl > 0 and ttl <= 1000

        assert cache.fetch!(:a, level: 1) == 1
        assert cache.fetch!(:a, level: 2) == 1
        assert cache.fetch!(:a, level: 3) == 1

        _ = t_sleep(1100)

        assert_raise Nebulex.KeyError, fn -> cache.fetch!(:a) end
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(:a, level: 1) end
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(:a, level: 2) end
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(:a, level: 3) end
      end

      test "touch/1", %{cache: cache} do
        assert cache.put!(:touch, 1, ttl: 1000, level: 2) == :ok

        _ = t_sleep(10)

        assert cache.touch!(:touch)

        _ = t_sleep(200)

        assert cache.touch!(:touch)
        assert cache.fetch!(:touch) == 1

        _ = t_sleep(1100)

        assert_raise Nebulex.KeyError, fn -> cache.fetch!(:touch) end

        refute cache.touch!(:non_existent)
      end

      test "get_and_update/3", %{cache: cache} do
        assert cache.put!(1, 1, level: 1) == :ok
        assert cache.put!(2, 2) == :ok

        assert cache.get_and_update!(1, &{&1, &1 * 2}, level: 1) == {1, 2}
        assert cache.fetch!(1, level: 1) == 2
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(1, level: 3) end
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(1, level: 3) end

        assert cache.get_and_update!(2, &{&1, &1 * 2}) == {2, 4}
        assert cache.fetch!(2, level: 1) == 4
        assert cache.fetch!(2, level: 2) == 4
        assert cache.fetch!(2, level: 3) == 4

        assert cache.get_and_update!(1, fn _ -> :pop end, level: 1) == {2, nil}
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(1, level: 1) end

        assert cache.get_and_update!(2, fn _ -> :pop end) == {4, nil}
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(2, level: 1) end
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(2, level: 2) end
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(2, level: 3) end
      end

      test "update/4", %{cache: cache} do
        assert cache.put!(1, 1, level: 1) == :ok
        assert cache.put!(2, 2) == :ok

        assert cache.update!(1, 1, &(&1 * 2), level: 1) == 2
        assert cache.fetch!(1, level: 1) == 2
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(1, level: 2) end
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(1, level: 3) end

        assert cache.update!(2, 1, &(&1 * 2)) == 4
        assert cache.fetch!(2, level: 1) == 4
        assert cache.fetch!(2, level: 2) == 4
        assert cache.fetch!(2, level: 3) == 4
      end

      test "incr/3", %{cache: cache} do
        assert cache.incr!(1) == 1
        assert cache.fetch!(1, level: 1) == 1
        assert cache.fetch!(1, level: 2) == 1
        assert cache.fetch!(1, level: 3) == 1

        assert cache.incr!(2, 2, level: 2) == 2
        assert cache.fetch!(2, level: 2) == 2
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(2, level: 1) end
        assert_raise Nebulex.KeyError, fn -> cache.fetch!(2, level: 3) end

        assert cache.incr!(3, 3) == 3
        assert cache.fetch!(3, level: 1) == 3
        assert cache.fetch!(3, level: 2) == 3
        assert cache.fetch!(3, level: 3) == 3

        assert cache.incr!(4, 5) == 5
        assert cache.incr!(4, -5) == 0
        assert cache.fetch!(4, level: 1) == 0
        assert cache.fetch!(4, level: 2) == 0
        assert cache.fetch!(4, level: 3) == 0
      end
    end

    describe "queryable" do
      setup %{cache: cache} do
        entries = Enum.map(1..30, &{&1, &1})

        entries
        |> Stream.chunk_every(10)
        |> Stream.with_index(1)
        |> Enum.each(fn {chunk, idx} ->
          cache.put_all!(chunk, level: idx)
        end)

        {:ok, entries: entries}
      end

      test "get_all/2 and stream/2", %{cache: cache, entries: expected} do
        assert cache.get_all!() |> Enum.sort() == expected
        assert cache.stream!() |> Enum.sort() == expected

        del =
          for x <- 20..60 do
            assert cache.delete(x) == :ok

            {x, x}
          end

        assert cache.get_all!() |> Enum.sort() == expected -- del
        assert cache.stream!() |> Enum.sort() == expected -- del
      end

      test "get_all/2 and stream/2 [in: keys]", %{cache: cache, entries: expected} do
        keys = Enum.to_list(1..100)

        assert cache.get_all!(in: keys) |> Enum.sort() == expected |> Enum.sort()

        assert cache.stream!(in: keys) |> Enum.sort() == expected |> Enum.sort()
      end

      test "delete_all/2", %{cache: cache} do
        assert count = cache.count_all!()
        assert cache.delete_all!() == count
        assert cache.get_all!() == []
      end

      test "count_all/2", %{cache: cache} do
        assert cache.count_all!() == 30

        for x <- [1, 11, 21], do: cache.delete!(x, level: 1)
        assert cache.count_all!() == 29

        assert cache.delete!(1, level: 1) == :ok
        assert cache.delete!(11, level: 2) == :ok
        assert cache.delete!(21, level: 3) == :ok
        assert cache.count_all!() == 27
      end
    end

    describe "queryable [level: n]" do
      test "ok: performs the query on the given level", %{cache: cache} do
        :ok = cache.put(1, 1, level: 1)
        :ok = cache.put(2, 2, level: 2)
        :ok = cache.put(3, 3, level: 3)

        assert cache.get_all!([], level: 1) |> Map.new() == %{1 => 1}
        assert cache.get_all!([], level: 2) |> Map.new() == %{2 => 2}
        assert cache.get_all!([], level: 3) |> Map.new() == %{3 => 3}
        assert cache.get_all!() |> Map.new() == %{1 => 1, 2 => 2, 3 => 3}

        assert cache.count_all!([], level: 1) == 1
        assert cache.count_all!([], level: 2) == 1
        assert cache.count_all!([], level: 3) == 1
        assert cache.count_all!() == 3

        assert cache.delete_all!([], level: 1) == 1
        assert cache.count_all!([], level: 1) == 0
        assert cache.count_all!() == 2

        assert cache.delete_all!([], level: 2) == 1
        assert cache.count_all!([], level: 2) == 0
        assert cache.count_all!() == 1

        assert cache.delete_all!([], level: 3) == 1
        assert cache.count_all!([], level: 3) == 0
        assert cache.count_all!() == 0
      end
    end
  end
end
