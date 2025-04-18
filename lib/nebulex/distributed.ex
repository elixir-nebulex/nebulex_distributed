defmodule Nebulex.Distributed do
  @moduledoc """
  Distributed caching functionality for Nebulex.

  This project provides the core functionality for distributed caching in
  Nebulex, primarily through the following adapter components:

    * `Nebulex.Adapters.Partitioned` - The Partitioned adapter implements a
      distributed cache where data is partitioned across multiple nodes in a
      cluster. Each node is responsible for a subset of the cache data.
    * `Nebulex.Adapters.Multilevel` - The Multilevel adapter implements a cache
      hierarchy with multiple levels, where each level can use a different cache
      adapter. This is particularly useful for implementing near-cache
      topologies.
    * `Nebulex.Adapters.Replicated` - The Replicated adapter implements a
      distributed cache where data is replicated across all nodes in a cluster.
      Each node has an identical copy of the cache data. **WIP**.

  Check the adapters documentation for more information on how to configure and
  use them.
  """
end
