defmodule Nebulex.Adapters.MultilevelInclusiveCachexTest do
  use Nebulex.NodeCase
  use Mimic

  use Nebulex.CacheTestCase,
    except: [
      Nebulex.Cache.QueryableTest,
      Nebulex.Cache.QueryableExpirationTest,
      Nebulex.Cache.QueryableQueryErrorTest,
      Nebulex.Cache.ObservableTest
    ]

  use Nebulex.MultilevelTest
  use Nebulex.MultilevelQueryableTest

  import Nebulex.CacheCase

  alias Nebulex.Distributed.TestCache.MultilevelCachex, as: Multilevel
  alias Nebulex.Distributed.TestCache.MultilevelCachex.{L1, L2, L3}

  @levels [{L1, []}, {L2, []}, {L3, []}]

  setup_with_dynamic_cache Multilevel, :multilevel_inclusive_cachex,
    inclusion_policy: :inclusive,
    levels: @levels

  describe "multilevel inclusive" do
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
end
