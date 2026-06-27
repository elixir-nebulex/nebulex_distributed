defmodule Nebulex.Adapters.ReplicatedKeyHasherTest do
  # Coverage for the `:key_hasher` replication option, which addresses
  # https://github.com/appcues/partitioned_buffer/issues/18: cache keys
  # containing maps can't be buffered as-is, because the second write to
  # the same key drives the buffer down the `:ets.select_replace/2` path
  # with a match spec whose head holds a raw map, which ETS rejects
  # ("not a valid match specification"). `safe_put/3` swallows that crash
  # and emits `:replication, :discarded`, so the write silently drops and
  # never replicates. Configuring `:key_hasher` hashes the key before it
  # is buffered, sidestepping the limitation.
  #
  # These tests assert the adapter's own boundary: that the configured
  # hasher is threaded into every buffer write, and that a map-containing
  # key round-trips without being discarded once it is set.

  use ExUnit.Case, async: true

  import Mimic, only: [stub: 3, call_original: 3]
  import Nebulex.CacheCase, only: [safe_stop: 1, with_telemetry_handler: 3]

  alias Nebulex.Telemetry

  @moduletag capture_log: true

  defmodule KeyHasherCache do
    @moduledoc false
    use Nebulex.Cache,
      otp_app: :nebulex_distributed,
      adapter: Nebulex.Adapters.Replicated,
      adapter_opts: [primary_storage_adapter: Nebulex.Adapters.Local]
  end

  @telemetry_prefix Telemetry.default_prefix(KeyHasherCache)
  @discarded @telemetry_prefix ++ [:replication, :discarded]

  # A key containing a map — the shape that breaks unhashed buffering.
  @map_key {:foo, %{type: :bar}}

  describe ":key_hasher is threaded into buffer writes" do
    test "put_newer receives the configured hasher when set" do
      capture_put_newer_opts()
      start_cache(:key_hasher_set, key_hasher: true)

      assert KeyHasherCache.put(:key, "value") == :ok

      # One event per buffer (inbox + outbox); both carry the hasher.
      assert_receive {:put_newer_opts, inbox_opts}
      assert_receive {:put_newer_opts, outbox_opts}
      assert inbox_opts[:key_hasher] == true
      assert outbox_opts[:key_hasher] == true
    end

    test "put_newer carries no :key_hasher when the option is unset" do
      capture_put_newer_opts()
      start_cache(:key_hasher_unset, [])

      assert KeyHasherCache.put(:key, "value") == :ok

      assert_receive {:put_newer_opts, opts}
      refute Keyword.has_key?(opts, :key_hasher)
    end
  end

  describe "map-containing keys" do
    test "round-trip without being discarded when :key_hasher is set" do
      # 60s interval keeps both writes in the same buffer generation, so
      # the second one exercises the select_replace path that issue #18
      # crashes on. With :key_hasher the key is hashed first, so no crash.
      start_cache(:key_hasher_map, key_hasher: true, interval: 60_000)

      with_telemetry_handler __MODULE__, [@discarded], fn ->
        assert KeyHasherCache.put(@map_key, "v1") == :ok
        assert KeyHasherCache.put(@map_key, "v2") == :ok

        assert KeyHasherCache.get!(@map_key) == "v2"

        refute_receive {@discarded, _measurements, _metadata}, 200
      end
    end
  end

  # Stubs `Tidefall.HashMap.put_newer/4` to forward each call's opts to
  # the test process, then run the real implementation.
  defp capture_put_newer_opts do
    parent = self()

    Tidefall.HashMap
    |> stub(:put_newer, fn buffer, key, value, opts ->
      send(parent, {:put_newer_opts, opts})

      call_original(Tidefall.HashMap, :put_newer, [buffer, key, value, opts])
    end)
  end

  defp start_cache(name, replication) do
    {:ok, pid} = KeyHasherCache.start_link(name: name, replication: replication)

    default_dynamic_cache = KeyHasherCache.get_dynamic_cache()
    KeyHasherCache.put_dynamic_cache(name)

    on_exit(fn ->
      safe_stop(pid)

      KeyHasherCache.put_dynamic_cache(default_dynamic_cache)
    end)

    pid
  end
end
