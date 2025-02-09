defmodule Nebulex.Adapters.PartitionedCacheTest do
  use Nebulex.NodeCase
  use Mimic

  use Nebulex.CacheTestCase,
    except: [Nebulex.Cache.QueryableQueryErrorTest, Nebulex.Cache.ObservableTest]

  use Nebulex.DistributedTest
  use Nebulex.DistributedInfoTest
  use Nebulex.Adapters.PartitionedInfoStatsTest

  import Nebulex.CacheCase

  alias Nebulex.Adapter
  alias Nebulex.Distributed.TestCache.{PartitionedCache, PartitionedNilCache}
  alias Nebulex.Utils

  @moduletag capture_log: true

  @primary :"primary@127.0.0.1"
  @cache_name :partitioned_cache

  setup do
    cluster = :lists.usort([@primary | Application.get_env(:nebulex_distributed, :nodes, [])])

    node_pid_list =
      start_caches(
        [node() | Node.list()],
        [
          {PartitionedCache, [name: @cache_name, join_timeout: 2000]},
          {PartitionedNilCache, [join_timeout: 2000]}
        ]
      )

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

    test "fails because unloaded keyslot module" do
      _ = Process.flag(:trap_exit, true)

      assert {:error, {%NimbleOptions.ValidationError{message: msg}, _}} =
               PartitionedCache.start_link(
                 name: :unloaded_keyslot,
                 keyslot: UnloadedKeyslot
               )

      assert Regex.match?(~r"module UnloadedKeyslot was not compiled", msg)
    end

    test "fails because keyslot module does not implement expected behaviour" do
      _ = Process.flag(:trap_exit, true)

      assert {:error, {%NimbleOptions.ValidationError{message: msg}, _}} =
               PartitionedCache.start_link(
                 name: :invalid_keyslot,
                 keyslot: __MODULE__
               )

      mod = inspect(__MODULE__)
      behaviour = "Nebulex.Adapter.Keyslot"

      expected = "the adapter expects the option value #{mod} to list #{behaviour} as a behaviour"

      assert Regex.match?(~r/#{expected}/, msg)
    end

    test "fails because invalid keyslot option" do
      _ = Process.flag(:trap_exit, true)

      assert {:error, {%NimbleOptions.ValidationError{message: msg}, _}} =
               PartitionedCache.start_link(
                 name: :invalid_keyslot,
                 keyslot: "invalid"
               )

      assert Regex.match?(
               ~r"invalid value for :keyslot option: expected a module, got: \"invalid\"",
               msg
             )
    end
  end

  describe "partitioned cache" do
    test "custom keyslot" do
      defmodule Keyslot do
        @behaviour Nebulex.Adapter.Keyslot

        @impl true
        def hash_slot(key, range) do
          key
          |> :erlang.phash2()
          |> rem(range)
        end
      end

      test_with_dynamic_cache(PartitionedCache, [name: :custom_keyslot, keyslot: Keyslot], fn ->
        refute PartitionedCache.get!("foo")
        assert PartitionedCache.put("foo", "bar") == :ok
        assert PartitionedCache.get!("foo") == "bar"
      end)
    end

    test "custom keyslot supports two item tuple keys for get_all" do
      defmodule TupleKeyslot do
        @behaviour Nebulex.Adapter.Keyslot

        @impl true
        def hash_slot({_, _} = key, range) do
          key
          |> :erlang.phash2()
          |> rem(range)
        end
      end

      test_with_dynamic_cache(
        PartitionedCache,
        [name: :custom_keyslot_with_tuple_keys, keyslot: TupleKeyslot],
        fn ->
          assert PartitionedCache.put_all!([{{"foo", 1}, "bar"}]) == :ok
          assert PartitionedCache.get_all!(in: [{"foo", 1}]) |> Map.new() == %{{"foo", 1} => "bar"}
        end
      )
    end

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
  end

  describe "cluster scenario:" do
    test "node leaves and then rejoins", %{name: name, cluster: cluster} do
      assert node() == @primary
      assert :lists.usort(Node.list()) == cluster -- [node()]
      assert PartitionedCache.nodes() == cluster

      PartitionedCache.with_dynamic_cache(name, fn ->
        :ok = PartitionedCache.leave_cluster()

        assert PartitionedCache.nodes() == cluster -- [node()]
      end)

      PartitionedCache.with_dynamic_cache(name, fn ->
        :ok = PartitionedCache.join_cluster()

        assert PartitionedCache.nodes() == cluster
      end)
    end

    test "teardown cache node", %{cluster: cluster} do
      assert PartitionedCache.nodes() == cluster

      assert PartitionedCache.put(1, 1) == :ok
      assert PartitionedCache.get!(1) == 1

      node = teardown_cache(1)

      wait_until fn ->
        assert PartitionedCache.nodes() == cluster -- [node]
      end

      refute PartitionedCache.get!(1)

      assert :ok == PartitionedCache.put_all([{4, 44}, {2, 2}, {1, 1}])

      assert PartitionedCache.get!(4) == 44
      assert PartitionedCache.get!(2) == 2
      assert PartitionedCache.get!(1) == 1
    end

    test "bootstrap leaves cache from the cluster when terminated and then rejoins when restarted",
         %{name: name} do
      prefix = [:nebulex, :cache, :bootstrap]
      started = prefix ++ [:started]
      stopped = prefix ++ [:stopped]
      joined = prefix ++ [:joined]
      exit_sig = prefix ++ [:exit]

      with_telemetry_handler __MODULE__, [started, stopped, joined, exit_sig], fn ->
        assert node() in PartitionedCache.nodes()

        true =
          [name, Bootstrap]
          |> Utils.camelize_and_concat()
          |> Process.whereis()
          |> Process.exit(:stop)

        assert_receive {^exit_sig, %{system_time: _}, %{reason: :stop}}, 5000
        assert_receive {^stopped, %{system_time: _}, %{reason: :stop, cluster_nodes: nodes}}, 5000

        refute node() in nodes

        assert_receive {^started, %{system_time: _}, %{}}, 5000
        assert_receive {^joined, %{system_time: _}, %{cluster_nodes: nodes}}, 5000

        assert node() in nodes

        wait_until fn ->
          assert nodes -- PartitionedCache.nodes() == []
        end

        assert_receive {^joined, %{system_time: _}, %{cluster_nodes: nodes}}, 5000
        assert node() in nodes
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
        PartitionedNilCache.get_node(1)
        |> rpc_error(PartitionedNilCache, :fetch, 2, :timeout)

      assert_raise Nebulex.Error, msg, fn ->
        PartitionedNilCache.get!(1, nil, timeout: 0, before: fn -> Process.sleep(1000) end)
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
        PartitionedNilCache.fetch!(1, before: &PartitionedNilCache.raise_error/0)
      end

      assert_raise RuntimeError, ~r"\*\* \(ArgumentError\) error", fn ->
        PartitionedNilCache.count_all!([], before: &PartitionedNilCache.raise_error/0)
      end
    end

    test "error: exit signal" do
      p1 = "the process that executed the command exited with reason"
      p2 = "RuntimeError"
      p3 = Regex.escape("Node:\n\n#{inspect(PartitionedNilCache.get_node(1))}")

      regex = ~r/(#{p1})(.*)(#{p2})(.*)(#{p3})/s

      assert_raise Nebulex.Error, regex, fn ->
        PartitionedNilCache.fetch!(1, before: &PartitionedNilCache.exit_signal/0)
      end

      assert_raise Nebulex.Error, regex, fn ->
        PartitionedNilCache.put_new_all!(%{"1" => "1"},
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
      p4 = Regex.escape("Node:\n\n#{inspect(PartitionedNilCache.get_node(1))}")

      regex = ~r/(#{p1})(.*)(#{p2})(.*)(#{p3})(.*)(#{p4})/s

      assert_raise Nebulex.Error, regex, fn ->
        PartitionedNilCache.fetch!(1, before: &PartitionedNilCache.exit/0)
      end

      assert_raise Nebulex.Error, regex, fn ->
        PartitionedNilCache.put_new_all!(%{"1" => "1"},
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
      |> stub(:get_nodes, fn _ -> [node] end)
      |> stub(:get_node, fn _, _, _ -> node end)

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

  defp teardown_cache(key) do
    node = PartitionedCache.get_node(key)
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
