defmodule Nebulex.Adapters.PartitionedErrorTest do
  use ExUnit.Case, async: true

  use Nebulex.CacheTestCase,
    only: [
      Nebulex.Cache.KVErrorTest,
      Nebulex.Cache.KVExpirationErrorTest,
      Nebulex.Cache.CompositeKVErrorTest
    ]

  use Nebulex.DistributedQueryableErrorTest

  import Nebulex.CacheCase, only: [setup_with_dynamic_cache: 2]

  defmodule ErrorCache do
    use Nebulex.Cache,
      otp_app: :nebulex_distributed,
      adapter: Nebulex.Adapters.Partitioned,
      adapter_opts: [primary_storage_adapter: Nebulex.FakeAdapter]

    use Nebulex.Distributed.TestCache.Commons
  end

  setup_with_dynamic_cache ErrorCache, __MODULE__

  describe "info/1" do
    test "error: command returns an error" do
      assert_raise Nebulex.Error,
                   ~r"command failed with reason: :error",
                   fn -> ErrorCache.info!() end
    end
  end
end
