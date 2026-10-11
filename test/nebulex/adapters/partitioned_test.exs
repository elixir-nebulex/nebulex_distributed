defmodule Nebulex.Adapters.PartitionedCacheTest do
  use Nebulex.NodeCase, async: true

  # Inherit tests
  # `CompositeKVTelemetryTest` asserts the primitive spans of the default
  # composite implementation; here they run on the owner node instead.
  use Nebulex.CacheTestCase,
    except: [Nebulex.Cache.CompositeKVTelemetryTest, Nebulex.Cache.QueryableQueryErrorTest]

  use Nebulex.DistributedTest
  use Nebulex.DistributedInfoTest
  use Nebulex.Distributed.TransactionTest
  use Nebulex.Adapters.PartitionedInfoStatsTest

  import Mimic, only: [expect: 4, stub: 3]
  import Nebulex.CacheCase

  alias Nebulex.{Adapter, Telemetry, Utils}
  alias Nebulex.Distributed.TestCache.{PartitionedCache, PartitionedNilCache}

  @moduletag capture_log: true

  @primary :"primary@127.0.0.1"
  @cache_name :partitioned_cache

  @telemetry_prefix Telemetry.default_prefix(PartitionedCache) ++ [:ring_monitor]

  setup do
    cluster = :lists.usort([@primary | Application.get_env(:nebulex_distributed, :nodes, [])])
    nodes = [node() | Node.list()]

    node_pid_list =
      start_caches(
        nodes,
        [
          {PartitionedCache, name: @cache_name, rejoin_interval: 100},
          {PartitionedNilCache, []}
        ]
      )

    assert_eventually fn ->
      assert PartitionedCache.nodes() |> Enum.count() == Enum.count(nodes)
      assert PartitionedNilCache.nodes() |> Enum.count() == Enum.count(nodes)
    end

    default_dynamic_cache = PartitionedCache.get_dynamic_cache()
    _ = PartitionedCache.put_dynamic_cache(@cache_name)

    on_exit(fn ->
      _ = PartitionedCache.put_dynamic_cache(default_dynamic_cache)

      :ok = Process.sleep(100)

      stop_caches(node_pid_list)
    end)

    {:ok,
     cache: PartitionedCache, name: @cache_name, cluster: cluster, nil_cache: PartitionedNilCache}
  end

  describe "composite KV operations" do
    test "run on the node owning the key", %{cache: cache} do
      [k1, k2, k3, k4] = remote_keys(cache, 4)

      assert cache.get_or_store!(k1, &cache.node_fun/0) == cache.find_node!(k1)
      assert cache.fetch_or_store!(k2, &cache.ok_node_fun/0) == cache.find_node!(k2)

      assert cache.get_and_update!(k3, &cache.get_and_update_node_fun/1) ==
               {nil, cache.find_node!(k3)}

      # On a miss the initial value is stored and the function isn't called
      assert cache.update!(k4, nil, &cache.update_node_fun/1) == nil
      assert cache.update!(k4, nil, &cache.update_node_fun/1) == cache.find_node!(k4)

      for key <- [k1, k2, k3, k4] do
        assert cache.fetch!(key) == cache.find_node!(key)
      end
    end

    test "run locally when the calling node owns the key", %{cache: cache} do
      [key] = local_keys(cache, 1)

      assert cache.get_or_store!(key, &cache.node_fun/0) == node()
      assert cache.fetch!(key) == node()
    end

    test "pop the entry on the owner node", %{cache: cache} do
      [key] = remote_keys(cache, 1)

      :ok = cache.put!(key, 1)

      assert cache.get_and_update!(key, &cache.pop_fun/1) == {1, nil}
      assert cache.has_key?(key) == {:ok, false}
      assert cache.get_and_update!(key, &cache.pop_fun/1) == {nil, nil}
    end

    test "forward the :ttl and :keep_ttl options to the owner node", %{cache: cache} do
      [k1, k2] = remote_keys(cache, 2)
      ttl = :timer.minutes(1)

      assert cache.update!(k1, 1, &cache.update_node_fun/1, ttl: ttl) == 1
      assert cache.ttl!(k1) in 1..ttl

      :ok = cache.put!(k2, 1, ttl: ttl)

      assert cache.get_and_update!(k2, &cache.get_and_update_fun/1, keep_ttl: true) == {1, 2}
      assert cache.ttl!(k2) in 1..ttl

      assert cache.get_and_update!(k2, &cache.get_and_update_fun/1) == {2, 4}
      assert cache.ttl!(k2) == :infinity
    end

    test "return the function error from the owner node without storing", %{cache: cache} do
      [key] = remote_keys(cache, 1)

      assert {:error, %Nebulex.Error{reason: :error, metadata: metadata}} =
               cache.fetch_or_store(key, &cache.fetch_or_store_error_fun/0)

      assert Keyword.take(metadata, [:command, :key]) == [command: :fetch_or_store, key: key]
      assert cache.has_key?(key) == {:ok, false}
    end

    test "re-raise the exceptions raised by the function", %{cache: cache} do
      for key <- remote_keys(cache, 1) ++ local_keys(cache, 1) do
        assert_raise ArgumentError, "error", fn ->
          cache.get_or_store(key, &cache.raise_error/0)
        end

        assert_raise ArgumentError, ~r"must return a two-element tuple or :pop", fn ->
          cache.get_and_update!(key, &cache.get_and_update_bad_fun/1)
        end

        assert_raise RuntimeError, ~r"must return \{:ok, value\} or \{:error, reason\}", fn ->
          cache.fetch_or_store!(key, &cache.get_or_store_fun/0)
        end

        assert cache.has_key?(key) == {:ok, false}
      end
    end

    test "return an error when the function exits on the owner node", %{cache: cache} do
      [key] = remote_keys(cache, 1)

      assert {:error, %Nebulex.Error{reason: {:rpc, {:exit, {:exception, "bye"}}}}} =
               cache.get_or_store(key, &cache.exit/0)
    end

    test "the :timeout option covers the given function", %{cache: cache} do
      [key] = remote_keys(cache, 1)
      msg = rpc_error(cache.find_node!(key), cache, :get_or_store, 3, :timeout)

      assert_raise Nebulex.Error, msg, fn ->
        cache.get_or_store!(key, &cache.sleep_fun/0, timeout: 10)
      end
    end

    test "return an error when the owner node is unreachable", %{cache: cache} do
      node = :"invalid@127.0.0.1"

      Nebulex.Distributed.Cluster
      |> stub(:find_node, fn _, _ -> {:ok, node} end)

      assert_raise Nebulex.Error, rpc_error(node, cache, :update, 4, :noconnection), fn ->
        cache.update!(:key, 1, &cache.update_node_fun/1)
      end
    end

    test "return an error when the key has no owner node", %{cache: cache} do
      error = {:error, %Nebulex.Error{reason: :no_owner}}

      Nebulex.Distributed.Cluster
      |> stub(:find_node, fn _, _ -> error end)

      assert cache.get_and_update(:key, &cache.get_and_update_fun/1) == error
      assert cache.update(:key, 1, &cache.update_node_fun/1) == error
      assert cache.fetch_or_store(:key, &cache.fetch_or_store_fun/0) == error
      assert cache.get_or_store(:key, &cache.get_or_store_fun/0) == error
    end

    test "emit the composite command span only on the calling node", %{cache: cache, name: name} do
      prefix = Adapter.lookup_meta(name).telemetry_prefix
      start = prefix ++ [:command, :start]
      stop = prefix ++ [:command, :stop]
      fun = &cache.get_or_store_fun/0

      with_telemetry_handler [start, stop], fn ->
        for key <- remote_keys(cache, 1) ++ local_keys(cache, 1) do
          assert cache.get_or_store(key, fun) == {:ok, "value"}

          assert_receive {^start, _,
                          %{command: :get_or_store, args: [^key, ^fun, :infinity, false, _]}}

          assert_receive {^stop, _, %{command: :get_or_store, result: {:ok, "value"}}}
          refute_received {_, _, %{command: :fetch}}
          refute_received {_, _, %{command: :put}}
        end

        assert cache.get_or_store!(:key, fun, telemetry: false) == "value"

        refute_received {_, _, %{command: :get_or_store}}
      end
    end
  end

  describe "ring" do
    test "test rejoin interval", %{name: name} do
      joined = @telemetry_prefix ++ [:joined]

      with_telemetry_handler __MODULE__, [joined], fn ->
        assert_receive {^joined, %{system_time: _}, %{adapter_meta: %{name: ^name}}}, 5000
      end
    end

    test "error: find_node" do
      ExHashRing.Ring
      |> expect(:find_node, 2, fn _, _ -> {:error, :not_found} end)

      assert_raise Nebulex.Error, ~r"Error finding node in ring", fn ->
        PartitionedCache.put_all!(error: :find_node_error)
      end

      assert_raise Nebulex.Error, ~r"Error finding node in ring", fn ->
        PartitionedCache.get_all!(in: [:error, :finding, :node])
      end
    end
  end

  describe "c:init/1" do
    test "initializes the primary store metadata" do
      adapter_meta =
        @cache_name
        |> Adapter.lookup_meta()
        |> Map.fetch!(:primary_name)
        |> Adapter.lookup_meta()

      assert adapter_meta.adapter == Nebulex.Adapters.Local
      assert adapter_meta.backend == :ets
    end

    test "raises an exception because invalid primary store" do
      msg =
        "invalid value for :adapter option: the module " <>
          "Nebulex.Adapters.PartitionedCache was not compiled"

      assert_raise NimbleOptions.ValidationError, ~r"#{msg}", fn ->
        defmodule Demo do
          use Nebulex.Cache,
            otp_app: :nebulex,
            adapter: Nebulex.Adapters.PartitionedCache,
            adapter_opts: [primary_storage_adapter: Invalid]
        end
      end
    end
  end

  describe "cache operation" do
    test "get_and_update" do
      assert PartitionedCache.get_and_update!(1, &PartitionedCache.get_and_update_fun/1) == {nil, 1}
      assert PartitionedCache.get_and_update!(1, &PartitionedCache.get_and_update_fun/1) == {1, 2}
      assert PartitionedCache.get_and_update!(1, &PartitionedCache.get_and_update_fun/1) == {2, 4}

      assert_raise ArgumentError, fn ->
        PartitionedCache.get_and_update!(1, &PartitionedCache.get_and_update_bad_fun/1)
      end
    end

    test "incr raises when the counter is not an integer" do
      :ok = PartitionedCache.put(:counter, "string")

      assert_raise RuntimeError, ~r"RPC runtime error occurred while executing the command", fn ->
        PartitionedCache.incr(:counter, 10)
      end
    end

    test "fetch_or_store! stores the value in the cache if the key does not exist" do
      assert PartitionedCache.fetch_or_store!("lazy", &PartitionedCache.fetch_or_store_fun/0) ==
               "value"

      assert PartitionedCache.get!("lazy") == "value"
    end

    test "get_or_store! stores the value in the cache if the key does not exist" do
      assert PartitionedCache.get_or_store!("lazy", &PartitionedCache.get_or_store_fun/0) == "value"
      assert PartitionedCache.get!("lazy") == "value"
    end
  end

  describe "cluster scenario:" do
    test "node leaves and then rejoins", %{name: name, cluster: cluster} do
      assert node() == @primary
      assert Node.list() |> :lists.usort() == cluster -- [node()]
      assert PartitionedCache.nodes() |> Enum.sort() == cluster |> Enum.sort()

      PartitionedCache.with_dynamic_cache(name, fn ->
        :ok = PartitionedCache.leave_cluster()

        assert_eventually fn ->
          assert PartitionedCache.nodes() |> Enum.sort() == (cluster -- [node()]) |> Enum.sort()
        end
      end)

      PartitionedCache.with_dynamic_cache(name, fn ->
        :ok = PartitionedCache.join_cluster()

        assert_eventually fn ->
          assert PartitionedCache.nodes() |> Enum.sort() == cluster |> Enum.sort()
        end
      end)
    end

    test "teardown cache node", %{cluster: cluster} do
      nodes_removed = @telemetry_prefix ++ [:nodes_removed]

      with_telemetry_handler __MODULE__, [nodes_removed], fn ->
        assert PartitionedCache.nodes() |> Enum.sort() == cluster |> Enum.sort()

        assert PartitionedCache.put(1234, 1234) == :ok
        assert PartitionedCache.get!(1234) == 1234

        node = teardown_cache(1234)

        assert_receive {^nodes_removed, %{system_time: _}, %{nodes: nodes}}, 5000
        assert node in nodes

        refute PartitionedCache.get!(1234)
      end

      :ok = PartitionedCache.put_all([{4, 44}, {2, 2}, {1, 1}])

      assert PartitionedCache.get!(4) == 44
      assert PartitionedCache.get!(2) == 2
      assert PartitionedCache.get!(1) == 1
    end

    test "ring monitor leaves cache from the cluster when terminated and then rejoins when restarted",
         %{name: name} do
      started = @telemetry_prefix ++ [:started]
      stopped = @telemetry_prefix ++ [:stopped]
      joined = @telemetry_prefix ++ [:joined]
      nodes_added = @telemetry_prefix ++ [:nodes_added]
      exit_sig = @telemetry_prefix ++ [:exit]
      events = [started, stopped, joined, nodes_added, exit_sig]

      node = node()
      nodes = PartitionedCache.nodes()

      with_telemetry_handler __MODULE__, events, fn ->
        assert node in nodes

        true =
          [name, RingMonitor]
          |> Utils.camelize_and_concat()
          |> Process.whereis()
          |> Process.exit(:stop)

        assert_receive {^exit_sig, %{system_time: _}, %{reason: :stop}}, 5000
        assert_receive {^stopped, %{system_time: _}, %{reason: :stop, node: ^node}}, 5000

        assert_receive {^started, %{system_time: _}, %{}}, 5000
        assert_receive {^joined, %{system_time: _}, %{node: ^node}}, 5000
        assert_receive {^nodes_added, %{system_time: _}, %{nodes: _}}, 5000

        assert node in PartitionedCache.nodes()
      end
    end
  end

  describe "rpc" do
    test "error: multicall returns errros" do
      msg =
        "RPC multicall get_all got unexpected errors.\n\n" <>
          "Node #{node()}:\n    \n    ** (Nebulex.Error) " <>
          "command failed with reason: :error"

      assert_raise Nebulex.Error, ~r"#{Regex.escape(msg)}", fn ->
        PartitionedNilCache.get_all!([],
          after_return: &PartitionedNilCache.return_error/1
        )
      end
    end

    test "error: timeout " do
      msg =
        PartitionedNilCache.find_node!(1234)
        |> rpc_error(PartitionedNilCache, :fetch, 2, :timeout)

      assert_raise Nebulex.Error, msg, fn ->
        PartitionedNilCache.get!(1234, nil, timeout: 0, before: fn -> Process.sleep(1000) end)
      end
    end

    test "error: multicall timeout" do
      assert PartitionedCache.put_all!(for(x <- 1..100_000, do: {x, x}), timeout: 60_000) == :ok
      assert PartitionedCache.get!(1, timeout: 1000) == 1

      assert_raise Nebulex.Error, ~r"RPC multicall get_all got unexpected errors", fn ->
        PartitionedCache.get_all!([], timeout: 0)
      end

      assert_raise Nebulex.Error, ~r"RPC operation failed with reason: :timeout", fn ->
        PartitionedCache.get_all!([in: [1, 2, 3]], timeout: 0)
      end

      assert_raise Nebulex.Error, ~r"RPC operation failed with reason: :timeout", fn ->
        PartitionedCache.stream!([in: [1, 2, 3]], timeout: 0)
        |> Enum.to_list()
      end
    end

    test "error: raises exception" do
      assert_raise RuntimeError, ~r"\*\* \(ArgumentError\) error", fn ->
        PartitionedNilCache.fetch!(1234, before: &PartitionedNilCache.raise_error/0)
      end

      assert_raise RuntimeError, ~r"\*\* \(ArgumentError\) error", fn ->
        PartitionedNilCache.count_all!([], before: &PartitionedNilCache.raise_error/0)
      end
    end

    test "error: exit signal" do
      p1 = "the process that executed the command exited with reason"
      p2 = "RuntimeError"
      p3 = Regex.escape("Node:\n\n#{inspect(PartitionedNilCache.find_node!(1234))}")

      regex = ~r/(#{p1})(.*)(#{p2})(.*)(#{p3})/s

      assert_raise Nebulex.Error, regex, fn ->
        PartitionedNilCache.fetch!(1234, before: &PartitionedNilCache.exit_signal/0)
      end

      assert_raise Nebulex.Error, regex, fn ->
        PartitionedNilCache.put_new_all!(%{1234 => 1234},
          before: &PartitionedNilCache.exit_signal/0
        )
      end

      for op <- [:get_all!, :count_all!] do
        p4 = "RPC multicall #{String.replace("#{op}", "!", "")} got unexpected errors"
        p5 = Regex.escape("\n\nNode")

        regex = ~r/(#{p4})(.*)(#{p5})(.*)(#{p1})(.*)(#{p2})(.*)/s

        assert_raise Nebulex.Error, regex, fn ->
          apply(PartitionedNilCache, op, [[], [before: &PartitionedNilCache.exit_signal/0]])
        end
      end
    end

    test "error: exit exception" do
      p1 = "the applied function exited with reason: \"bye\""
      p2 = Regex.escape("Cache command:\n\n")
      p3 = Regex.escape("PartitionedNilCache.")
      p4 = Regex.escape("Node:\n\n#{inspect(PartitionedNilCache.find_node!(1234))}")

      regex = ~r/(#{p1})(.*)(#{p2})(.*)(#{p3})(.*)(#{p4})/s

      assert_raise Nebulex.Error, regex, fn ->
        PartitionedNilCache.fetch!(1234, before: &PartitionedNilCache.exit/0)
      end

      assert_raise Nebulex.Error, regex, fn ->
        PartitionedNilCache.put_new_all!(%{1234 => 1234},
          before: &PartitionedNilCache.exit/0
        )
      end

      p5 = "RPC multicall get_all got unexpected errors"
      p6 = Regex.escape("\n\nNode")

      regex = ~r/(#{p5})(.*)(#{p6})(.*)(#{p1})(.*)/s

      assert_raise Nebulex.Error, regex, fn ->
        PartitionedNilCache.get_all!([], before: &PartitionedNilCache.exit/0)
      end
    end

    test "error: erpc fails" do
      node = :"invalid@127.0.0.1"

      Nebulex.Distributed.Cluster
      |> stub(:ring_nodes, fn _ -> [node] end)
      |> stub(:find_node, fn _, _ -> {:ok, node} end)

      assert_raise Nebulex.Error,
                   ~r"#{rpc_error(node, PartitionedNilCache, :fetch, 2, :noconnection)}",
                   fn ->
                     PartitionedNilCache.get!(1)
                   end

      assert_raise Nebulex.Error,
                   ~r"#{rpc_error(node, PartitionedNilCache, :put_new_all, 2, :noconnection)}",
                   fn ->
                     PartitionedNilCache.put_new_all!(%{a: 1, b: 2, c: 3})
                   end

      p1 = "RPC multicall get_all got unexpected errors"
      p2 = Regex.escape("\n\nNode")
      p3 = "the RPC operation failed with reason: :noconnection"

      assert_raise Nebulex.Error, ~r/(#{p1})(.*)(#{p2})(.*)(#{p3})/s, fn ->
        PartitionedNilCache.get_all!()
      end
    end
  end

  ## Private Functions

  defp remote_keys(cache, count), do: keys_owned_by(cache, count, &(&1 != node()))
  defp local_keys(cache, count), do: keys_owned_by(cache, count, &(&1 == node()))

  defp keys_owned_by(cache, count, owner_filter) do
    1..1000
    |> Stream.filter(&owner_filter.(cache.find_node!(&1)))
    |> Enum.take(count)
  end

  defp teardown_cache(key) do
    node = PartitionedCache.find_node!(key)
    remote_pid = :rpc.call(node, Process, :whereis, [@cache_name])
    :ok = :rpc.call(node, Supervisor, :stop, [remote_pid])

    node
  end

  defp rpc_error(node, cache, op, arity, reason) do
    """
    the RPC operation failed with reason: #{inspect(reason)}.

    Cache command:

    #{inspect(cache)}.#{op}/#{arity}

    Node:

    #{inspect(node)}
    """
  end
end
