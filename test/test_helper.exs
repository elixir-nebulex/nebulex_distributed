# Nebulex dependency path
nbx_dep_path = Mix.Project.deps_paths()[:nebulex]

Code.require_file("#{nbx_dep_path}/test/support/test_adapter.exs", __DIR__)
Code.require_file("#{nbx_dep_path}/test/support/fake_adapter.exs", __DIR__)
Code.require_file("#{nbx_dep_path}/test/support/cache_case.exs", __DIR__)

for file <- File.ls!("#{nbx_dep_path}/test/shared/cache") do
  Code.require_file("#{nbx_dep_path}/test/shared/cache/" <> file, __DIR__)
end

for file <- File.ls!("#{nbx_dep_path}/test/shared"), file != "cache" do
  Code.require_file("#{nbx_dep_path}/test/shared/" <> file, __DIR__)
end

for file <- File.ls!("test/shared"), not File.dir?("test/shared/" <> file) do
  Code.require_file("./shared/" <> file, __DIR__)
end

# Mocks
[
  ExHashRing.Ring,
  Nebulex.Distributed.Cluster,
  Nebulex.Distributed.TestCache.Multilevel.L1,
  Nebulex.Distributed.TestCache.Multilevel.L2,
  Nebulex.Distributed.TestCache.Multilevel.L3,
  Nebulex.Distributed.TestCache.ReplicatedCache.Primary,
  PartitionedBuffer.Map
]
|> Enum.each(&Mimic.copy/1)

# Start Telemetry
_ = Application.start(:telemetry)

# Set nodes
nodes = [:"node1@127.0.0.1", :"node2@127.0.0.1", :"node3@127.0.0.1", :"node4@127.0.0.1"]
:ok = Application.put_env(:nebulex_distributed, :nodes, nodes)

# Spawn remote nodes
unless :clustered in Keyword.get(ExUnit.configuration(), :exclude, []) do
  Nebulex.TestCluster.spawn(nodes)
end

# For tasks/generators testing
Mix.start()
Mix.shell(Mix.Shell.Process)

# Start ExUnit
ExUnit.start()
