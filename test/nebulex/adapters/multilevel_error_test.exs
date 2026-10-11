defmodule Nebulex.Adapters.MultilevelErrorTest do
  use ExUnit.Case, async: true

  use Nebulex.CacheTestCase,
    only: [
      Nebulex.Cache.KVErrorTest,
      Nebulex.Cache.KVExpirationErrorTest,
      Nebulex.Cache.CompositeKVErrorTest
    ]

  import Nebulex.CacheCase, only: [setup_with_dynamic_cache: 3]

  defmodule MultilevelError do
    @moduledoc false
    use Nebulex.Cache,
      otp_app: :nebulex_distributed,
      adapter: Nebulex.Adapters.Multilevel

    use Nebulex.Distributed.TestCache.Commons

    defmodule L1 do
      use Nebulex.Cache,
        otp_app: :nebulex_distributed,
        adapter: Nebulex.FakeAdapter
    end
  end

  setup_with_dynamic_cache MultilevelError, :multilevel_error,
    inclusion_policy: :inclusive,
    levels: [{MultilevelError.L1, []}]

  describe "info/1" do
    test "error: command returns an error" do
      assert_raise Nebulex.Error,
                   ~r"ommand failed with reason: :error",
                   fn -> MultilevelError.info!() end
    end
  end
end
