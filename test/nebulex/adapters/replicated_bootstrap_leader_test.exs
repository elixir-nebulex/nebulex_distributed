defmodule Nebulex.Adapters.ReplicatedBootstrapLeaderTest do
  # Covers how `Nebulex.Adapters.Replicated.ClusterMonitor` chooses the bootstrap pusher
  # and keeps its cluster view when a push fails.
  #
  # The pusher is elected among the nodes `:pg` knows at the time of the join, not from
  # the monitor's own view, so a stale view cannot elect extra pushers. The view is
  # updated before the pushes run, and each push is rescued on its own, so a push that
  # raises (for example the target raising inside `apply_bootstrap_entries/2`) is reported
  # through the `:bootstrap` telemetry span and never drops a joined node from the view.
  use Nebulex.NodeCase, async: false

  import Mimic, only: [call_original: 3, stub: 3]
  import Nebulex.CacheCase, only: [wait_until: 1, with_telemetry_handler: 3]

  alias Nebulex.Adapter
  alias Nebulex.Adapters.Replicated
  alias Nebulex.Adapters.Replicated.{ClusterMonitor, Replicator}
  alias Nebulex.Distributed.{Cluster, RPC}
  alias Nebulex.Distributed.TestCache.ReplicatedCache
  alias Nebulex.Telemetry
  alias Nebulex.TestTelemetryRelay

  @moduletag capture_log: true

  @cache_name :replicated_leader_cache
  @telemetry_prefix Telemetry.default_prefix(ReplicatedCache)
  @bootstrap_start @telemetry_prefix ++ [:bootstrap, :start]
  @bootstrap_stop @telemetry_prefix ++ [:bootstrap, :stop]
  @bootstrap_exception @telemetry_prefix ++ [:bootstrap, :exception]

  # Erlang term order on node names: node1 < node2 < node3 < node4 < primary.
  @node1 :"node1@127.0.0.1"
  @node2 :"node2@127.0.0.1"
  @node3 :"node3@127.0.0.1"
  @node4 :"node4@127.0.0.1"

  # The real Replicator with only `apply_bootstrap_entries/2` replaced by a raise. Stands in
  # for any exception on the target during bootstrap, such as an ETS table deleted while its
  # cache restarts. `RPC.call/5` re-raises remote exceptions on the pusher. Everything else
  # (inbox/outbox processing) keeps working.
  @replicator_source File.read!("lib/nebulex/adapters/replicated/replicator.ex")
  @raising_replicator String.replace(
                        @replicator_source,
                        "def apply_bootstrap_entries(adapter_meta, entries) do",
                        "def apply_bootstrap_entries(_adapter_meta, _entries) do\n" <>
                          "    raise \"target raised during bootstrap\"\n" <>
                          "  end\n\n" <>
                          "  def __real_apply_bootstrap_entries(adapter_meta, entries) do"
                      )

  true = @raising_replicator != @replicator_source

  setup do
    nodes = [node() | Node.list()]

    _ = start_caches(nodes, [{ReplicatedCache, name: @cache_name, replication: [interval: 100]}])

    # A monitor adds a joined node to its view only after pushing to it, so
    # once every view holds every node, the startup pushes are done and none
    # of them reaches the raising Replicator compiled on node4 below.
    wait_until(fn ->
      for node <- nodes do
        assert monitor_state(node).cluster_nodes == MapSet.new(nodes)
      end
    end)

    default_dynamic_cache = ReplicatedCache.get_dynamic_cache()
    _ = ReplicatedCache.put_dynamic_cache(@cache_name)

    on_exit(fn ->
      _ = ReplicatedCache.put_dynamic_cache(default_dynamic_cache)

      for node <- nodes, suffix <- [:start, :exception] do
        _ = rpc(node, :telemetry, :detach, [{__MODULE__, node, suffix}])
      end

      for node <- nodes do
        case rpc(node, Process, :whereis, [@cache_name]) do
          pid when is_pid(pid) -> rpc(node, Supervisor, :stop, [pid, :normal, @timeout])
          _ -> :ok
        end
      end

      _ = rpc(@node4, :code, :purge, [Replicator])
      _ = rpc(@node4, :code, :delete, [Replicator])
    end)

    # Give the pushers something to push, and wait for replication to land on them.
    :ok = ReplicatedCache.put(:k, "v")

    wait_until(fn ->
      assert primary_count(@node1) >= 1
      assert primary_count(@node2) >= 1
    end)

    [{Replicator, _binary}] = rpc(@node4, Code, :compile_string, [@raising_replicator])

    {:ok, doomed_target: monitor_pid(@node4)}
  end

  describe "handle_info/2 on a :join event" do
    test "keeps every node in the batch in the view when a push raises", %{
      doomed_target: doomed_target
    } do
      node2_member = monitor_pid(@node2)
      candidates = [@node1, @node3, node()]

      for node <- candidates, do: watch(node, :exception)

      # Nodes 2 and 4 restart: their leaves are applied, then both rejoins arrive in one
      # :pg batch on every other node. The push to node4 raises.
      for node <- candidates do
        %{pg_ref: pg_ref, adapter_meta: %{pg_group: pg_group}} = monitor_state(node)

        send(monitor_pid(node), {pg_ref, :leave, pg_group, [doomed_target, node2_member]})
        send(monitor_pid(node), {pg_ref, :join, pg_group, [doomed_target, node2_member]})
      end

      # The state is read with a call, so it is answered after both messages were handled.
      for node <- candidates do
        %{cluster_nodes: view} = monitor_state(node)

        assert MapSet.member?(view, @node2) == true
        assert MapSet.member?(view, @node4) == true
      end

      # The failure is reported, not swallowed, by the one node that pushes to node4.
      assert_receive {@bootstrap_exception, _measurements, %{node: pusher, peer: @node4}},
                     @timeout

      assert pusher in candidates
      refute_receive {@bootstrap_exception, _measurements, %{peer: @node4}}, 500

      # The monitor is still alive and serving.
      assert @node2 in rpc(pusher, Cluster, :pg_nodes, [monitor_state(pusher).pg_group])
    end

    test "keeps pushing to the other joiners when one push raises", %{
      doomed_target: doomed_target
    } do
      monitor = monitor_pid(node())
      %{pg_ref: pg_ref, adapter_meta: %{pg_group: pg_group}} = monitor_state(node())
      others = for node <- [@node1, @node2, @node3], do: monitor_pid(node)
      joiners = [doomed_target | others]

      with_telemetry_handler __MODULE__, [@bootstrap_stop, @bootstrap_exception], fn ->
        # Every peer restarts at once, so this node is the only pusher left.
        send(monitor, {pg_ref, :leave, pg_group, joiners})
        send(monitor, {pg_ref, :join, pg_group, joiners})

        %{cluster_nodes: view} = monitor_state(node())

        for node <- [@node1, @node2, @node3, @node4] do
          assert MapSet.member?(view, node) == true
        end

        assert_receive {@bootstrap_exception, _measurements, %{peer: @node4}}, @timeout

        for node <- [@node1, @node2, @node3] do
          assert_receive {@bootstrap_stop, _measurements, %{peer: ^node}}, @timeout
        end

        assert monitor_pid(node()) == monitor
      end
    end

    test "one join gets exactly one pusher, and different joiners get different ones" do
      for node <- [node() | Node.list()], do: watch(node, :start)

      # The smallest name (node1) would serve both of these joiners.
      pusher2 = restart_and_wait_for_pusher(@node2)
      pusher3 = restart_and_wait_for_pusher(@node3)

      assert pusher2 != pusher3
    end
  end

  describe "push_to_new_node/2" do
    setup do
      for i <- 1..4, do: :ok = ReplicatedCache.put({:push, i}, i)

      {:ok, adapter_meta: Adapter.lookup_meta(@cache_name)}
    end

    test "stops at the first chunk that fails", %{adapter_meta: adapter_meta} do
      fake_node = :"nonexistent@127.0.0.1"
      test_pid = self()

      stub(RPC, :call, fn node, mod, fun, args, timeout ->
        send(test_pid, {:rpc_call, fun})

        call_original(RPC, :call, [node, mod, fun, args, timeout])
      end)

      adapter_meta = %{adapter_meta | bootstrap_chunk_size: 2}

      with_telemetry_handler __MODULE__, [@bootstrap_stop], fn ->
        ClusterMonitor.push_to_new_node(fake_node, adapter_meta)

        assert_receive {@bootstrap_stop, _measurements,
                        %{peer: ^fake_node, total: 0, errors: [{{:error, _}, ^fake_node}]}}
      end

      assert_received {:rpc_call, :apply_bootstrap_entries}
      refute_received {:rpc_call, :apply_bootstrap_entries}
    end

    test "reports the delivered entries and whether anti-entropy is on", %{
      adapter_meta: adapter_meta
    } do
      total = ReplicatedCache.count_all!()

      with_telemetry_handler __MODULE__, [@bootstrap_stop], fn ->
        ClusterMonitor.push_to_new_node(@node1, %{adapter_meta | bootstrap_chunk_size: 2})

        assert_receive {@bootstrap_stop, _measurements,
                        %{peer: @node1, total: ^total, errors: [], anti_entropy: true}}

        ClusterMonitor.push_to_new_node(@node1, %{adapter_meta | anti_entropy_interval: false})

        assert_receive {@bootstrap_stop, _measurements, %{peer: @node1, anti_entropy: false}}
      end
    end
  end

  ## Helpers

  # Restarts the cache on `node`, waits for the bootstrap push to it, and
  # checks that no other node pushes.
  defp restart_and_wait_for_pusher(node) do
    :ok =
      rpc(node, Supervisor, :stop, [rpc(node, Process, :whereis, [@cache_name]), :normal, @timeout])

    {:ok, _pid} =
      start_cache(node, ReplicatedCache, name: @cache_name, replication: [interval: 100])

    assert_receive {@bootstrap_start, _measurements, %{node: pusher, peer: ^node}}, @timeout
    refute_receive {@bootstrap_start, _measurements, %{peer: ^node}}, 500

    pusher
  end

  defp watch(node, suffix) do
    :ok =
      rpc(node, :telemetry, :attach, [
        {__MODULE__, node, suffix},
        @telemetry_prefix ++ [:bootstrap, suffix],
        &TestTelemetryRelay.handle/4,
        %{pid: self()}
      ])
  end

  defp primary_count(node) do
    %{adapter_meta: adapter_meta} = monitor_state(node)

    rpc(node, Replicated, :with_dynamic_cache, [adapter_meta, :count_all!, [[], []]])
  end

  defp monitor_pid(node) do
    cache_pid = rpc(node, Process, :whereis, [@cache_name])
    pid = find_child(node, cache_pid, ClusterMonitor)

    assert is_pid(pid)

    pid
  end

  defp monitor_state(node) do
    rpc(node, :sys, :get_state, [monitor_pid(node)])
  end

  defp find_child(node, supervisor, id) do
    node
    |> rpc(Supervisor, :which_children, [supervisor])
    |> Enum.find_value(fn
      {^id, pid, _type, _modules} when is_pid(pid) -> pid
      {_id, pid, :supervisor, _modules} when is_pid(pid) -> find_child(node, pid, id)
      _child -> nil
    end)
  end
end
