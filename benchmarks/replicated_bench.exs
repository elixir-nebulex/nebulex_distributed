## Benchmarks

_ = Application.start(:telemetry)

# Load test support modules
Code.require_file("test/support/test_cache.ex")
Code.require_file("test/support/test_cluster.ex")
Code.require_file("test/support/node_case.ex")

# Spawn remote nodes for realistic replication
nodes = [:"node1@127.0.0.1", :"node2@127.0.0.1", :"node3@127.0.0.1"]
Nebulex.TestCluster.spawn(nodes)

# Load test cache module on remote nodes
test_cache_path = Path.expand("test/support/test_cache.ex")

for node <- Node.list() do
  :rpc.block_call(node, Code, :require_file, [test_cache_path])
end

alias Nebulex.Distributed.TestCache.ReplicatedCache
alias Nebulex.NodeCase

# Cache options
cache_opts = [
  primary: [gc_interval: :timer.hours(1)],
  replication: [interval: 100]
]

# Start cache on the primary node and remote nodes
{:ok, local} = ReplicatedCache.start_link(cache_opts)
node_pid_list = NodeCase.start_caches(Node.list(), [{ReplicatedCache, cache_opts}])

# Wait for cluster formation and bootstrap
:ok = Process.sleep(500)

IO.puts("Cluster nodes: #{inspect(ReplicatedCache.nodes())}")

# samples
keys = Enum.to_list(1..10_000)
bulk = for x <- 1..100, do: {x, x}

# init caches
Enum.each(1..5000, &ReplicatedCache.put(&1, &1))

# Wait for initial replication
:ok = Process.sleep(1000)

inputs = %{
  "Replicated Cache" => ReplicatedCache
}

benchmarks = %{
  "fetch" => fn {cache, random} ->
    cache.fetch(random)
  end,
  "put!" => fn {cache, random} ->
    cache.put!(random, random)
  end,
  "put_new!" => fn {cache, random} ->
    cache.put_new!(random, random)
  end,
  "replace!" => fn {cache, random} ->
    cache.replace!(random, random)
  end,
  "put_all!" => fn {cache, _random} ->
    cache.put_all!(bulk)
  end,
  "delete!" => fn {cache, random} ->
    cache.delete!(random)
  end,
  "take" => fn {cache, random} ->
    cache.take(random)
  end,
  "has_key?" => fn {cache, random} ->
    cache.has_key?(random)
  end,
  "ttl" => fn {cache, random} ->
    cache.ttl(random)
  end,
  "expire!" => fn {cache, random} ->
    cache.expire!(random, 1000)
  end,
  "incr!" => fn {cache, _random} ->
    cache.incr!(:counter, 1)
  end,
  "get_all!" => fn {cache, _random} ->
    cache.get_all!(in: [1, 2, 3, 4, 5, 6, 7, 8, 9])
  end,
  "count_all!" => fn {cache, _random} ->
    cache.count_all!()
  end,
  "delete_all!" => fn {cache, _random} ->
    cache.delete_all!(in: [1, 2, 3])
  end
}

Benchee.run(
  benchmarks,
  inputs: inputs,
  before_scenario: fn cache ->
    {cache, Enum.random(keys)}
  end,
  formatters: [
    {Benchee.Formatters.Console, comparison: false, extended_statistics: true},
    {Benchee.Formatters.HTML, extended_statistics: true, auto_open: false}
  ],
  print: [
    fast_warning: false
  ]
)

# Stop caches
NodeCase.stop_caches(node_pid_list)
if Process.alive?(local), do: Supervisor.stop(local)
