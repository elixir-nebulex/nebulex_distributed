defmodule Nebulex.Adapters.ReplicatedBufferRaceTest do
  # Regression for https://github.com/appcues/partitioned_buffer/issues/19
  # / elixir-nebulex/nebulex_distributed#17.
  #
  # During a rolling deploy, calls into `Cache.put/2` previously crashed with
  # `ArgumentError: the table identifier does not refer to an existing ETS
  # table` raised from a buffer `put_newer` landing on a partition whose ETS
  # table had just been destroyed by a dying partition process, invoked by
  # `Nebulex.Adapters.Replicated.replicate/3`.
  #
  # The fix wraps each `put_newer` call in `replicate/3` with `try`, emits a
  # `:replication, :discarded` telemetry event on failure, and always returns
  # `:ok`. The local primary write is unaffected (lands before `replicate/3`).
  # This guards against any buffer write failure during a restart window,
  # independent of the backing buffer implementation.
  #
  # These tests stub `Tidefall.HashMap.put_newer/4` (using Mimic) to
  # raise the same `ArgumentError` the production race produces.

  use ExUnit.Case, async: true

  import Mimic, only: [stub: 3, call_original: 3]
  import Nebulex.CacheCase, only: [safe_stop: 1, with_telemetry_handler: 3]

  alias Nebulex.{Adapter, Telemetry}
  alias Tidefall.HashMap, as: HM

  @moduletag capture_log: true

  defmodule BufferRaceCache do
    @moduledoc false
    use Nebulex.Cache,
      otp_app: :nebulex_distributed,
      adapter: Nebulex.Adapters.Replicated,
      adapter_opts: [primary_storage_adapter: Nebulex.Adapters.Local]
  end

  @cache_name :buffer_race_cache
  @telemetry_prefix Telemetry.default_prefix(BufferRaceCache)
  @discarded @telemetry_prefix ++ [:replication, :discarded]

  setup do
    {:ok, pid} =
      BufferRaceCache.start_link(
        name: @cache_name,
        replication: [interval: 60_000]
      )

    default_dynamic_cache = BufferRaceCache.get_dynamic_cache()
    BufferRaceCache.put_dynamic_cache(@cache_name)

    on_exit(fn ->
      safe_stop(pid)

      BufferRaceCache.put_dynamic_cache(default_dynamic_cache)
    end)

    {:ok, adapter_meta: Adapter.lookup_meta(@cache_name)}
  end

  describe "swallows buffer failures and emits telemetry" do
    test "Cache.put returns :ok and emits :discarded telemetry when outbox put_newer raises",
         %{adapter_meta: adapter_meta} do
      fail_buffer(adapter_meta.outbox)

      with_telemetry_handler __MODULE__, [@discarded], fn ->
        assert BufferRaceCache.put(:key, "value") == :ok
        assert BufferRaceCache.get!(:key) == "value"

        assert_receive {@discarded, measurements, metadata}, 500

        assert metadata.buffer == :outbox
        assert metadata.key == :key
        assert match?({:put, [:key, "value", _]}, metadata.command)
        assert metadata.kind == :error
        assert match?(%ArgumentError{}, metadata.reason)
        assert is_list(metadata.stacktrace)
        assert is_integer(measurements.system_time)
        assert metadata.node == node()
        assert metadata.adapter_meta == adapter_meta
      end
    end

    test "Cache.put returns :ok and emits :discarded telemetry with buffer: :inbox when inbox put_newer raises",
         %{adapter_meta: adapter_meta} do
      fail_buffer(adapter_meta.inbox)

      with_telemetry_handler __MODULE__, [@discarded], fn ->
        assert BufferRaceCache.put(:key2, "value2") == :ok
        assert BufferRaceCache.get!(:key2) == "value2"

        assert_receive {@discarded, measurements, metadata}, 500

        assert metadata.buffer == :inbox
        assert metadata.key == :key2
        assert match?({:put, [:key2, "value2", _]}, metadata.command)
        assert metadata.kind == :error
        assert match?(%ArgumentError{}, metadata.reason)
        assert is_list(metadata.stacktrace)
        assert is_integer(measurements.system_time)
        assert metadata.node == node()

        refute_receive {@discarded, _, _}, 100
      end
    end

    test "discarded event carries kind: :exit when put_newer exits",
         %{adapter_meta: adapter_meta} do
      fail_buffer(adapter_meta.outbox, fn -> exit(:buffer_gone) end)

      with_telemetry_handler __MODULE__, [@discarded], fn ->
        assert BufferRaceCache.put(:key3, "value3") == :ok

        assert_receive {@discarded, _measurements, metadata}, 500

        assert metadata.kind == :exit
        assert metadata.reason == :buffer_gone
      end
    end

    test "discarded event carries kind: :throw when put_newer throws",
         %{adapter_meta: adapter_meta} do
      fail_buffer(adapter_meta.outbox, fn -> throw(:buffer_thrown) end)

      with_telemetry_handler __MODULE__, [@discarded], fn ->
        assert BufferRaceCache.put(:key4, "value4") == :ok

        assert_receive {@discarded, _measurements, metadata}, 500

        assert metadata.kind == :throw
        assert metadata.reason == :buffer_thrown
      end
    end
  end

  # Stubs `Tidefall.HashMap.put_newer/4` so calls against `target_buffer`
  # invoke `failure_fn` (default: raise the production-shape ArgumentError),
  # and any other buffer falls through to the real implementation.
  defp fail_buffer(target_buffer, failure_fn \\ &raise_argument_error/0) do
    HM
    |> stub(:put_newer, fn
      ^target_buffer, _key, _value, _opts ->
        failure_fn.()

      buffer, key, value, opts ->
        call_original(HM, :put_newer, [buffer, key, value, opts])
    end)
  end

  defp raise_argument_error do
    raise ArgumentError,
          "errors were found at the given arguments:\n\n  * 1st argument: " <>
            "the table identifier does not refer to an existing ETS table\n"
  end
end
