defmodule Nebulex.Distributed do
  @moduledoc """
  Distributed caching adapters for [Nebulex](https://hexdocs.pm/nebulex).

  This package provides adapters for building distributed cache topologies
  on top of Nebulex. Each adapter wraps a local cache adapter (e.g.,
  `Nebulex.Adapters.Local`) and adds a distributed layer on top of it.

  ## Adapters

    * `Nebulex.Adapters.Partitioned` - Partitioned cache topology where
      data is sharded across cluster nodes using consistent hashing.
      Each node owns a subset of the keyspace, providing linear
      scalability and single-hop point-to-point operations.

    * `Nebulex.Adapters.Multilevel` - Multi-level (near) cache topology
      with a hierarchy of cache layers (e.g., L1 local + L2 distributed).
      Reads check levels in order for fast local hits with fallback to
      slower shared layers. Writes use a write-through policy.

    * `Nebulex.Adapters.Coherent` - Local cache with distributed
      invalidation via `Nebulex.Streams`. Each node maintains its
      own independent local cache for maximum read performance;
      entries are only present on nodes that have fetched them.
      Writes broadcast invalidation events across the cluster so
      other nodes delete stale entries and fetch fresh data on
      the next read.

    * 🚧 `Nebulex.Adapters.Replicated` - Replicated cache topology
      where every node holds an identical copy of the entire cache.
      Writes are broadcast to all nodes so data is immediately
      available locally on every member of the cluster. **Planned
      for a future release.**

  ## Choosing an Adapter

  | Aspect              | Partitioned          | Multilevel            | Coherent               |
  |---------------------|----------------------|-----------------------|------------------------|
  | Data location       | Sharded across nodes | L1 local + L2 shared  | Independent per node   |
  | Read performance    | Network hop required | L1 fast, L2 slower    | Fastest (pure local)   |
  | Write behavior      | Remote write to owner| Write through levels  | Local + invalidation   |
  | Consistency         | Strong (single owner)| Varies by config      | Eventual               |
  | Network overhead    | Medium               | Medium to high        | Low (invalidations)    |
  | Best for            | Large datasets       | Tiered access patterns| Read-heavy workloads   |

  ## Installation

  Add `:nebulex_distributed` to your dependencies in `mix.exs`:

      def deps do
        [
          {:nebulex, "~> 3.0"},
          {:nebulex_distributed, "~> 3.0"},
          {:telemetry, "~> 0.4 or ~> 1.0"}, # For observability and monitoring
          {:decorator, "~> 1.4"},           # For declarative caching
        ]
      end

  The `:telemetry` (observability and monitoring cache operations) and
  `:decorator` (declarative caching) dependencies are optional but highly
  recommended.

  See each adapter's documentation for configuration and usage details.
  """
end
