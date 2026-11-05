defmodule Nebulex.Adapters.ReplicatedBootstrapLeaderTest do
  # Covers how `Nebulex.Adapters.Replicated.ClusterMonitor` chooses the bootstrap pusher, how
  # it runs the pushes, and how it behaves when a push fails.
  #
  # The pusher is elected among the `:pg` members whose own bootstrap has completed, at the
  # time of the join, not from the monitor's own view, so a stale view or a half-filled cache
  # cannot elect a pusher. A cluster with no bootstrapped member falls back to every member and
  # the pusher seeds the group. The view is updated before the pushes run, and the pushes run
  # one at a time in unlinked tasks, so a push that raises (for example the target raising
  # inside `apply_bootstrap_entries/2`) is reported through the `:bootstrap` telemetry span,
  # never drops a joined node from the view, and never stops the pushes to the other joiners.
  # When a bootstrapped node leaves, the node that leads afterwards picks up the members that
  # are not bootstrapped, so a pusher restarting mid-rollout strands nobody.
  use Nebulex.NodeCase, async: false

  import Nebulex.CacheCase, only: [wait_until: 1]

  alias Nebulex.Adapters.Replicated
  alias Nebulex.Adapters.Replicated.{ClusterMonitor, Replicator}
  alias Nebulex.Distributed.Cluster
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

  # The real Replicator with only `apply_bootstrap_entries/2` replaced by a short pause and a
  # raise. Stands in for any exception on the target during bootstrap, such as an ETS table
  # deleted while its cache restarts. `RPC.call/5` re-raises remote exceptions on the pusher.
  # Everything else (inbox/outbox processing) keeps working.
  @replicator_source File.read!("lib/nebulex/adapters/replicated/replicator.ex")
  @raising_replicator String.replace(
                        @replicator_source,
                        "def apply_bootstrap_entries(adapter_meta, entries) do",
                        "def apply_bootstrap_entries(_adapter_meta, _entries) do\n" <>
                          "    Process.sleep(300)\n" <>
                          "    raise \"target raised during bootstrap\"\n" <>
                          "  end\n\n" <>
                          "  def __real_apply_bootstrap_entries(adapter_meta, entries) do"
                      )

  true = @raising_replicator != @replicator_source

  setup do
    nodes = [node() | Node.list()]

    _ = start_caches(nodes, [{ReplicatedCache, name: @cache_name, replication: [interval: 100]}])

    :ok = Process.sleep(200)

    default_dynamic_cache = ReplicatedCache.get_dynamic_cache()
    _ = ReplicatedCache.put_dynamic_cache(@cache_name)

    on_exit(fn ->
      _ = ReplicatedCache.put_dynamic_cache(default_dynamic_cache)

      for node <- nodes, suffix <- [:start, :stop, :exception] do
        _ = rpc(node, :telemetry, :detach, [{__MODULE__, node, suffix}])
      end

      for node <- nodes do
        case rpc(node, Process, :whereis, [@cache_name]) do
          pid when is_pid(pid) -> rpc(node, Supervisor, :stop, [pid, :normal, @timeout])
          _ -> :ok
        end

        _ = rpc(node, :code, :purge, [Replicator])
        _ = rpc(node, :code, :delete, [Replicator])
      end
    end)

    # Give the pushers something to push, and wait for replication to land on them.
    :ok = ReplicatedCache.put(:k, "v")

    wait_until(fn ->
      assert primary_count(@node1) >= 1
      assert primary_count(@node2) >= 1
    end)

    # Every node finished its own bootstrap and is a pusher candidate: the first push in the
    # cluster's life ran as the fallback and seeded the group, the rest were pushed to.
    %{adapter_meta: adapter_meta} = monitor_state(node())

    wait_until(fn ->
      assert Enum.sort(Cluster.pg_nodes(adapter_meta.ready_pg_group)) == Enum.sort(nodes)
    end)

    {:ok, adapter_meta: adapter_meta}
  end

  describe "handle_info/2 on a :join event" do
    test "keeps every node in the batch in the view when a push raises" do
      break_bootstrap_on(@node4)

      monitor = monitor_pid(@node1)
      %{pg_ref: pg_ref, pg_group: pg_group} = monitor_state(@node1)
      node2_member = monitor_pid(@node2)
      node4_member = monitor_pid(@node4)

      watch(@node1, :stop)
      watch(@node1, :exception)

      # Nodes 2 and 4 restart: their leaves are applied, then both rejoins arrive in one
      # :pg batch. The push to node4 raises.
      send(monitor, {pg_ref, :leave, pg_group, [node4_member, node2_member]})
      send(monitor, {pg_ref, :join, pg_group, [node4_member, node2_member]})

      # The state is read with a call, so it is answered after both messages were handled.
      %{cluster_nodes: view} = monitor_state(@node1)

      assert MapSet.member?(view, @node2) == true
      assert MapSet.member?(view, @node4) == true

      # The other push in the batch completes, the failure is reported, not swallowed.
      assert_receive {@bootstrap_stop, _measurements, %{node: @node1, peer: @node2}}, @timeout

      assert_receive {@bootstrap_exception, _measurements, %{node: @node1, peer: @node4}},
                     @timeout

      # The monitor is still alive and serving.
      assert monitor_pid(@node1) == monitor
      assert @node2 in rpc(@node1, Cluster, :pg_nodes, [pg_group])
    end

    test "elects exactly one pusher from live membership even when a view is stale" do
      # node2 forgets node1 in its own view, the way a lost join batch would leave it.
      monitor = monitor_pid(@node2)
      %{pg_ref: pg_ref, pg_group: pg_group} = monitor_state(@node2)

      send(monitor, {pg_ref, :leave, pg_group, [monitor_pid(@node1)]})

      %{cluster_nodes: view} = monitor_state(@node2)
      assert MapSet.member?(view, @node1) == false
      assert Enum.min(view) == @node2

      watch(@node1, :start)
      watch(@node2, :start)

      restart_cache(@node3)

      # The real leader pushes, node2 does not.
      assert_receive {@bootstrap_start, _measurements, %{node: @node1, peer: @node3}}, @timeout
      refute_receive {@bootstrap_start, _measurements, %{node: @node2, peer: @node3}}, 500

      # And node2's view has caught up through the join it just saw.
      %{cluster_nodes: view} = monitor_state(@node2)
      assert MapSet.member?(view, @node3) == true
    end

    test "does not elect a node whose own bootstrap has not completed", %{
      adapter_meta: adapter_meta
    } do
      # node1 rejoins, but the push to it raises, so it never completes its bootstrap.
      break_bootstrap_on(@node1)
      watch(@node2, :exception)

      restart_cache(@node1)

      assert_receive {@bootstrap_exception, _measurements, %{node: @node2, peer: @node1}},
                     @timeout

      assert @node1 in Cluster.pg_nodes(adapter_meta.pg_group)
      assert @node1 in Cluster.pg_nodes(adapter_meta.ready_pg_group) == false

      # node1 sorts first, but node2 is the lowest bootstrapped node, so node2 pushes.
      watch(@node1, :start)
      watch(@node2, :start)

      restart_cache(@node3)

      assert_receive {@bootstrap_start, _measurements, %{node: @node2, peer: @node3}}, @timeout
      refute_receive {@bootstrap_start, _measurements, %{node: @node1, peer: @node3}}, 500

      # node3 completed its bootstrap and is a candidate again.
      wait_until(fn -> assert @node3 in Cluster.pg_nodes(adapter_meta.ready_pg_group) end)
    end

    test "falls back to every member when no bootstrapped member is left", %{
      adapter_meta: adapter_meta
    } do
      # Every node forgets it was bootstrapped.
      for node <- [node() | Node.list()] do
        :ok =
          rpc(node, :pg, :leave, [
            Cluster.pg_scope(),
            adapter_meta.ready_pg_group,
            monitor_pid(node)
          ])
      end

      # The remote leaves reach the local `:pg` asynchronously.
      wait_until(fn -> assert Cluster.pg_nodes(adapter_meta.ready_pg_group) == [] end)

      watch(@node1, :start)

      restart_cache(@node3)

      # The lowest member pushes rather than nobody.
      assert_receive {@bootstrap_start, _measurements, %{node: @node1, peer: @node3}}, @timeout
    end

    test "does not push when it connects to a cluster that has bootstrapped members", %{
      adapter_meta: adapter_meta
    } do
      me = node()
      monitor = monitor_pid(me)
      %{pg_ref: pg_ref, pg_group: pg_group} = monitor_state(me)

      # The local node is not bootstrapped (it started before it could connect).
      :ok = :pg.leave(Cluster.pg_scope(), adapter_meta.ready_pg_group, monitor)

      watch(me, :start)

      # The connection comes up: every peer appears as a joiner at once.
      peers = Enum.map(Node.list(), &monitor_pid/1)
      send(monitor, {pg_ref, :leave, pg_group, peers})
      send(monitor, {pg_ref, :join, pg_group, peers})

      # The peers are bootstrapped, so there is nobody to push to, and the local node is
      # not elected.
      refute_receive {@bootstrap_start, _measurements, %{node: ^me}}, 500

      assert %{in_flight: nil} = monitor_state(me)
    end

    test "becomes bootstrapped itself after pushing as the fallback", %{
      adapter_meta: adapter_meta
    } do
      me = node()
      monitor = monitor_pid(me)
      %{pg_ref: pg_ref, pg_group: pg_group} = monitor_state(me)

      # No node is bootstrapped.
      for node <- [me | Node.list()] do
        :ok =
          rpc(node, :pg, :leave, [
            Cluster.pg_scope(),
            adapter_meta.ready_pg_group,
            monitor_pid(node)
          ])
      end

      wait_until(fn -> assert Cluster.pg_nodes(adapter_meta.ready_pg_group) == [] end)

      _ = :sys.replace_state(monitor, &%{&1 | ready?: false})

      watch(me, :stop)

      peers = Enum.map(Node.list(), &monitor_pid/1)
      send(monitor, {pg_ref, :leave, pg_group, peers})
      send(monitor, {pg_ref, :join, pg_group, peers})

      # The local node is the only candidate, pushes to every peer, and is bootstrapped from
      # the first completed push on.
      for peer <- Node.list() do
        assert_receive {@bootstrap_stop, _measurements, %{node: ^me, peer: ^peer}}, @timeout
      end

      assert %{ready?: true} = monitor_state(me)
      assert me in Cluster.pg_nodes(adapter_meta.ready_pg_group)
    end
  end

  describe "handle_info/2 on a :leave event of the bootstrapped group" do
    test "queues the members that are not bootstrapped when it leads afterwards", %{
      adapter_meta: adapter_meta
    } do
      me = node()
      peers = Node.list()

      # The peers stop reacting, so only the local node can pick up the pushes.
      for peer <- peers, do: :ok = rpc(peer, :sys, :suspend, [monitor_pid(peer)])

      on_exit(fn ->
        for peer <- peers, pid = rpc(peer, Process, :whereis, [@cache_name]), is_pid(pid) do
          _ = rpc(peer, :sys, :resume, [monitor_pid(peer)])
        end
      end)

      watch(me, :stop)

      # The bootstrapped peers leave the group one by one, the last of them the pusher.
      for peer <- peers do
        :ok =
          rpc(peer, :pg, :leave, [
            Cluster.pg_scope(),
            adapter_meta.ready_pg_group,
            monitor_pid(peer)
          ])
      end

      # The local node leads now and pushes to every member that is not bootstrapped.
      for peer <- peers do
        assert_receive {@bootstrap_stop, _measurements, %{node: ^me, peer: ^peer}}, @timeout
      end

      refute_receive {@bootstrap_stop, _measurements, %{node: ^me}}, 500
    end
  end

  describe "bootstrap_completed/1" do
    test "makes a restarted node a candidate again once its bootstrap finishes", %{
      adapter_meta: adapter_meta
    } do
      watch(@node1, :stop)

      # The local node restarts its cache: it is bootstrapped by the leader and only then
      # rejoins the candidates.
      :ok = ReplicatedCache.stop()
      {:ok, _pid} = ReplicatedCache.start_link(name: @cache_name, replication: [interval: 100])

      assert_receive {@bootstrap_stop, _measurements, %{node: @node1, peer: peer}}, @timeout
      assert peer == node()

      wait_until(fn ->
        assert monitor_state(node()).ready? == true
        assert node() in Cluster.pg_nodes(adapter_meta.ready_pg_group)
      end)

      # A repeated completion changes nothing.
      :ok = ClusterMonitor.bootstrap_completed(adapter_meta)

      assert monitor_state(node()).ready? == true
    end
  end

  describe "the push queue" do
    test "pushes one joiner at a time, isolates failures, and skips nodes that left", %{
      adapter_meta: adapter_meta
    } do
      break_bootstrap_on(@node4)

      me = node()
      monitor = monitor_pid(me)
      %{pg_ref: pg_ref, pg_group: pg_group} = monitor_state(me)

      [node2_member, node3_member, node4_member] =
        Enum.map([@node2, @node3, @node4], &monitor_pid/1)

      # Only the local node stays bootstrapped, so it is the pusher for every join below.
      for node <- Node.list() do
        :ok =
          rpc(node, :pg, :leave, [
            Cluster.pg_scope(),
            adapter_meta.ready_pg_group,
            monitor_pid(node)
          ])
      end

      wait_until(fn -> assert Cluster.pg_nodes(adapter_meta.ready_pg_group) == [me] end)

      # node2 is gone for good before its join is handled.
      :ok =
        rpc(@node2, Supervisor, :stop, [
          rpc(@node2, Process, :whereis, [@cache_name]),
          :normal,
          @timeout
        ])

      wait_until(fn -> assert @node2 in Cluster.pg_nodes(pg_group) == false end)

      # Becoming the leader made the local node push to the peers that are not bootstrapped;
      # let that round finish first.
      wait_until(fn ->
        assert %{in_flight: nil, queue: queue} = monitor_state(me)
        assert :queue.is_empty(queue)
      end)

      watch(me, :start)
      watch(me, :stop)
      watch(me, :exception)

      # node4 raises after a pause and restarts while that push is in flight, so it is pushed
      # once more; node3 and node2 join meanwhile and wait their turn; node3 restarts while
      # still queued, which changes nothing.
      send(monitor, {pg_ref, :leave, pg_group, [node3_member, node4_member]})
      send(monitor, {pg_ref, :join, pg_group, [node4_member]})
      send(monitor, {pg_ref, :leave, pg_group, [node4_member]})
      send(monitor, {pg_ref, :join, pg_group, [node4_member]})
      send(monitor, {pg_ref, :join, pg_group, [node3_member, node2_member]})
      send(monitor, {pg_ref, :leave, pg_group, [node3_member]})
      send(monitor, {pg_ref, :join, pg_group, [node3_member]})

      assert_receive {@bootstrap_start, _measurements, %{node: ^me, peer: @node4}}, @timeout
      refute_received {@bootstrap_start, _measurements, %{node: ^me, peer: @node3}}

      assert_receive {@bootstrap_exception, _measurements, %{node: ^me, peer: @node4}},
                     @timeout

      assert_receive {@bootstrap_start, _measurements, %{node: ^me, peer: @node4}}, @timeout
      refute_received {@bootstrap_start, _measurements, %{node: ^me, peer: @node3}}

      assert_receive {@bootstrap_exception, _measurements, %{node: ^me, peer: @node4}},
                     @timeout

      assert_receive {@bootstrap_start, _measurements, %{node: ^me, peer: @node3}}, @timeout
      assert_receive {@bootstrap_stop, _measurements, %{node: ^me, peer: @node3}}, @timeout
      refute_receive {@bootstrap_start, _measurements, %{node: ^me, peer: @node3}}, 500
      refute_received {@bootstrap_start, _measurements, %{node: ^me, peer: @node4}}
      refute_received {@bootstrap_start, _measurements, %{node: ^me, peer: @node2}}

      # Unrelated messages are ignored.
      send(monitor, :unrelated)

      assert %{in_flight: nil} = :sys.get_state(monitor)
    end
  end

  ## Helpers

  defp break_bootstrap_on(node) do
    [{Replicator, _binary}] = rpc(node, Code, :compile_string, [@raising_replicator])

    :ok
  end

  defp restart_cache(node) do
    :ok =
      rpc(node, Supervisor, :stop, [rpc(node, Process, :whereis, [@cache_name]), :normal, @timeout])

    {:ok, _pid} =
      start_cache(node, ReplicatedCache, name: @cache_name, replication: [interval: 100])

    :ok
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
