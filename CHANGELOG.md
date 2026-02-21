# Changelog

All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
