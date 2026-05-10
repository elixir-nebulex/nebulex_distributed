# Changelog

All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Fixed

- [Nebulex.Adapters.Replicated] Fixed excessive heap usage during bootstrap
  and anti-entropy reconciliation that could OOM nodes with large caches on
  deploy. The push-based bootstrap previously materialised the entire local
  cache into a single list (and a single RPC payload) and emitted per-entry
  primary-cache telemetry spans, both of which scaled with the cache size.
  Bootstrap now streams entries chunk-by-chunk to the joining peer, and the
  internal `ttl`/`put_new`/`put` calls used by bootstrap and anti-entropy
  pass `telemetry: false` so they no longer emit per-entry spans or
  increment primary-cache stats counters. The chunk size is configurable
  via the new `:bootstrap_chunk_size` replication option (defaults to
  `1_000`). Per-cycle visibility is preserved through the existing
  `:bootstrap` and `:anti_entropy` span events.
  [#15](https://github.com/elixir-nebulex/nebulex_distributed/issues/15).

## [v3.2.1](https://github.com/elixir-nebulex/nebulex_distributed/tree/v3.2.1) (2026-03-28)
> [Full Changelog](https://github.com/elixir-nebulex/nebulex_distributed/compare/v3.2.0...v3.2.1)

### Fixed

- [Nebulex.Adapters.Replicated] Replaced pull-based bootstrap with push-based
  bootstrap. Existing nodes now detect new node joins via
  `:pg.monitor_scope/1` and push their cache entries using `:put_new` commands,
  avoiding the overwrite problem where the bootstrapping node's timestamp was
  always newer than prior write versions. A simple leader election (smallest
  node name) ensures exactly one node pushes, and anti-entropy repairs any
  gaps.
  [#14](https://github.com/elixir-nebulex/nebulex_distributed/issues/14).
- [Nebulex.Adapters.Replicated] Switched conflict resolution versioning from
  `System.monotonic_time()` to `System.system_time()`. Monotonic time has a
  per-node epoch and cannot be compared across nodes, causing incorrect
  "newer version wins" resolution when nodes have different uptimes.
  Wall-clock time (Unix epoch) is directly comparable across
  NTP-synchronised nodes.
  [#14](https://github.com/elixir-nebulex/nebulex_distributed/issues/14).

## [v3.2.0](https://github.com/elixir-nebulex/nebulex_distributed/tree/v3.2.0) (2026-03-27)
> [Full Changelog](https://github.com/elixir-nebulex/nebulex_distributed/compare/v3.1.0...v3.2.0)

### Enhancements

- [Nebulex.Adapters.Replicated] Added new push-based replicated cache adapter.
  Provides a replicated cache topology where each node maintains a full local
  copy of the cache. Writes are applied locally first, then batched and pushed
  to all peer nodes via RPC using double-buffered outbox/inbox
  (`PartitionedBuffer.Map`). Features include:
  - Zero-latency local reads — all data is replicated on every node.
  - "Newer version wins" conflict resolution via monotonic versioning.
  - Bootstrap on node join with TTL streaming from an existing peer, with
    fallback to the next peer on failure.
  - GC interval reset after bootstrap (when primary is
    `Nebulex.Adapters.Local`) to synchronize generation rotation.
  - Configurable replication options: `:interval`, `:batch_size`, `:timeout`,
    `:retries`, `:retry_delay`, `:partitions`.
  - Telemetry spans for replication and bootstrap events.
  [#4](https://github.com/elixir-nebulex/nebulex_distributed/issues/4).
- [Nebulex.Adapters.Replicated] Added optional anti-entropy reconciliation to
  detect and repair data drift between nodes after missed replication batches.
  Enabled via the `:anti_entropy_interval` replication option. The implementation
  follows Riak's Active Anti-Entropy (AAE) approach: keys are hashed into 1024
  fixed buckets with XOR fingerprints, a random peer is compared each cycle, and
  only divergent entries are repaired through the inbox preserving "newer version
  wins" semantics. Emits Telemetry span events for monitoring.
  [#12](https://github.com/elixir-nebulex/nebulex_distributed/issues/12).

## [v3.1.0](https://github.com/elixir-nebulex/nebulex_distributed/tree/v3.1.0) (2026-03-14)
> [Full Changelog](https://github.com/elixir-nebulex/nebulex_distributed/compare/v3.0.0...v3.1.0)

### Enhancements

- [Nebulex.Adapters.Partitioned] Added new `:node_filter` start option that
  controls which cluster nodes are added to the hash ring. Accepts a 1-arity
  function receiving a node name and returning a boolean. Excluded nodes remain
  part of the cache cluster and can use the cache transparently — operations
  route to ring nodes as usual. Useful for background job runners, test nodes,
  or admin consoles that should not cache data locally. By default, all nodes
  are added to the ring (full backward compatibility).
  [#7](https://github.com/elixir-nebulex/nebulex_distributed/issues/7).
- [Nebulex.Adapters.Partitioned] Added ASCII topology diagram to the module
  documentation.
- [Nebulex.Adapters.Multilevel] Added ASCII topology diagram to the module
  documentation.

## [v3.0.0](https://github.com/elixir-nebulex/nebulex_distributed/tree/v3.0.0) (2026-02-21)
> [Full Changelog](https://github.com/elixir-nebulex/nebulex_distributed/compare/v3.0.0-rc.2...v3.0.0)

### Enhancements

- [Nebulex.Adapters.Coherent] Added new coherent cache adapter that provides
  a "local cache with distributed invalidation" pattern. Each node maintains
  its own independent local cache, but writes trigger invalidation events
  across the cluster via `Nebulex.Streams`. Key features include:
  - Local storage with maximum read performance (pure local lookups).
  - Distributed invalidation via Phoenix.PubSub using `Nebulex.Streams`.
  - Write-invalidate protocol that only broadcasts invalidation events,
    not actual values, minimizing network overhead.
  - Eventual consistency model where invalidated entries are deleted from
    remote caches, forcing fresh fetches from the System-of-Record.
  - Configurable primary storage adapter (defaults to `Nebulex.Adapters.Local`).
  - Ideal for read-heavy workloads, configuration/reference data caching,
    and session caches.
  [#6](https://github.com/elixir-nebulex/nebulex_distributed/issues/6).
- [Nebulex.Distributed.Transaction] Implemented adapter-specific transaction
  support using `Nebulex.Distributed.Transaction`, which provides a distributed
  locking implementation based on Erlang's `:global` module. This module is now
  the canonical implementation for distributed transactions across
  `Nebulex.Adapters.Partitioned` and `Nebulex.Adapters.Multilevel` adapters.
  The implementation provides cluster-wide lock coordination with automatic node
  discovery for partitioned and multilevel cache topologies. This change aligns
  with the removal of the default transaction implementation from Nebulex core,
  allowing distributed adapters to provide transaction implementations optimized
  for multi-node scenarios. Both adapters automatically coordinate locks across
  all cluster nodes without requiring manual `:nodes` configuration.
  [#5](https://github.com/elixir-nebulex/nebulex_distributed/issues/5).
- [Nebulex.Distributed.RingMonitor] Moved the ring monitor from
  `Nebulex.Adapters.Partitioned.RingMonitor` to
  `Nebulex.Distributed.RingMonitor` to establish it as a shared distributed
  utility. This keeps the implementation reusable by other distributed adapters
  (including future replicated/invalidation-based adapters) while preserving
  the existing partitioned adapter behavior and telemetry events.

## [v3.0.0-rc.2](https://github.com/elixir-nebulex/nebulex_distributed/tree/v3.0.0-rc.2) (2025-12-07)
> [Full Changelog](https://github.com/elixir-nebulex/nebulex_distributed/compare/v3.0.0-rc.1...v3.0.0-rc.2)

### Enhancements

- [Nebulex.Adapters.Partitioned] Improved documentation with comprehensive
  sections on consistent hashing, key distribution, and cluster membership
  management.
- [Nebulex.Adapters.Multilevel] Added comprehensive "How Multi-Level Caches Work"
  section covering cache lookup, write-through policy, replication modes, and
  TTL handling. Additionally, a "Near cache topology example" section with
  practical L1 (Local) + L2 (Redis) configuration and usage was added.
- [Nebulex.Adapters.Multilevel.Options] Improved documentation for the supported
  options.

### Backwards incompatible changes

- [Nebulex.Adapters.Partitioned] The `Nebulex.Adapters.Partitioned.Bootstraper`
  module has been renamed to `Nebulex.Adapters.Partitioned.RingMonitor` to
  better reflect its monitoring responsibilities.
- [Nebulex.Adapters.Partitioned.Options] The `:join_timeout` option has been
  renamed to `:rejoin_interval` for clarity. It represents the periodic interval
  at which the ring monitor rejoins the cluster group to force ring
  synchronization.

### Fixed

- Fixed typo in README.md ("GtiHub" → "GitHub").

## [v3.0.0-rc.1](https://github.com/elixir-nebulex/nebulex_distributed/tree/v3.0.0-rc.1) (2025-06-07)
> [Full Changelog](https://github.com/elixir-nebulex/nebulex_distributed/compare/2c6188ebb9a482cd75a97b6abdb2feddcfb189c9...v3.0.0-rc.1)

### Enhancements

- [Nebulex.Adapters.Multilevel] The adapter implements the new Nebulex v3 API.
- [Nebulex.Adapters.Multilevel] The adapter supports the `:timeout` option from
  `Nebulex.Cache`.
- [Nebulex.Adapters.Partitioned] The adapter implements the new Nebulex v3 API.
- [Nebulex.Adapters.Partitioned] The adapter supports the `:timeout` option from
  `Nebulex.Cache`.
- [Nebulex.Adapters.Partitioned] The adapter uses `ExHashRing` for distributing
  the keys across the cluster members.
- [Nebulex.Adapters.Partitioned] he adapter supports the `:hash_ring` option to
  configute `ExHashRing`.

### Backwards incompatible changes

- [Nebulex.Distributed] `Nebulex.Distributed` requires Erlang/OTP 25 or later.
- [Nebulex.Distributed.RPC] The usage of async tasks for the RPC calls has been
  removed (for OTP < 23). The new `Nebulex.Distributed.RPC` module uses `:erpc`.
- [Nebulex.Adapters.Multilevel] Option `:model` is no longer supported. Please
  use the option `:inclusion_policy` instead.
- [Nebulex.Adapters.Multilevel] The previous extended function `model/0,1` has
  been removed. Please use `inclusion_policy/0,1` instead.
- [Nebulex.Adapters.Partitioned] The option `:keyslot` is no longer supported,
  so the `Nebulex.Adapter.Keyslot` behaviour has been removed. The partitioned
  adapter uses `ExHashRing` for key distribution under-the-hood.
- [Nebulex.Adapters.Partitioned] The option `:join_timeout` is no longer
  supported.

### Closed issues

- `Nebulex.Adapters.Multilevel` implements Nebulex v3
  [#3](https://github.com/elixir-nebulex/nebulex_distributed/issues/3)
- `Nebulex.Adapters.Partitioned` implements Nebulex v3
  [#2](https://github.com/elixir-nebulex/nebulex_distributed/issues/2)
- Upgrade to Nebulex v3.0.0
  [#1](https://github.com/elixir-nebulex/nebulex_distributed/issues/1)
