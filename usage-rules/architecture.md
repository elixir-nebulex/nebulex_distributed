# Architecture

This guide covers what the code and the module docs do not: how the modules
split responsibility and the non-negotiable rules for every change. Read the
"Non-Negotiables" section at session start; read the rest before structural
changes.

For how the adapters work and how to configure them, read the module docs:

- `Nebulex.Distributed` - the package overview and the available adapters.
- `Nebulex.Adapters.Partitioned`, `Nebulex.Adapters.Replicated`,
  `Nebulex.Adapters.Coherent`, and `Nebulex.Adapters.Multilevel` - each
  topology, its options, and its telemetry events.
- `Nebulex.Distributed.Transaction` - distributed locking with `:global`.

For the broader Nebulex context (the adapter pattern, behaviour contracts,
and shared non-negotiables), read the
[core Nebulex architecture guide][core-arch].

[core-arch]: https://hexdocs.pm/nebulex/architecture.html

---

## Purpose

The core `nebulex` package defines the caching abstraction, and
`nebulex_local` provides the local in-memory cache. `nebulex_distributed`
builds the distributed topologies on top of a local cache: each adapter wraps
a primary storage adapter (`Nebulex.Adapters.Local` by default) and adds a
distributed layer. Bugs here usually show up only with several nodes, under
concurrent writes, or when nodes join and leave.

---

## Module Boundaries

This section covers how the modules split responsibility; the file layout
is in `lib/` and `test/`.

- Each adapter (`Partitioned`, `Replicated`, `Coherent`, `Multilevel`) is an
  entry point that implements the behaviours and delegates storage to its
  primary (or level) caches. Its `Options` module holds the NimbleOptions
  schema, and its `Supervisor` starts the primary storage and the adapter's
  processes.
- `Nebulex.Distributed.Cluster` manages cluster membership through `:pg` and
  the consistent hash ring (`ExHashRing`). `RingMonitor` keeps the ring in
  sync with the cluster for the partitioned adapter.
- `Nebulex.Distributed.RPC` wraps the remote calls the adapters make.
- `Nebulex.Distributed.Transaction` is the default transaction
  implementation for the distributed adapters.
- In `Replicated`, `Replicator` processes the outbox/inbox batches and
  streams the bootstrap entries, `ClusterMonitor` watches node joins,
  elects the bootstrap leader and queues its pushes one joiner at a time,
  and `AntiEntropy` runs the reconciliation cycle.

---

## Non-Negotiables

These rules apply in addition to the shared non-negotiables in the core
Nebulex architecture guide.

### 1. Test with real peer nodes

Changes to cluster membership, routing, replication, bootstrap, or
anti-entropy must be covered by the clustered tests, which run on peer
nodes. Tests that mock the cluster do not catch ordering or timing bugs
between nodes.

### 2. Keep the adapters consistent

All four adapters implement `Nebulex.Adapter.CompositeKV`, which Nebulex
core requires and checks at compile time. When a behaviour or a cross-cutting
option changes, update every adapter that it applies to.

### 3. Optional deps must remain optional

`:telemetry` is optional. No code path may raise or fail when it is absent.

### 4. Keep this document up to date

After any structural change (changing the module boundaries, adding an
optional dependency, or adding a non-negotiable), review this document and
update it if needed. When you add, move, or remove a file, check that this
section still holds. Do not copy here what the module docs already say;
link to them. A stale architecture guide is worse than no guide.

### 5. `mix precommit` must pass

```bash
NEBULEX_PATH=nebulex mix precommit
```

### 6. Read why before changing subtle behavior

The reasons behind the subtle behavior (composite commands on the owner
node, replication and versioning, bootstrap, anti-entropy, and `:key_hasher`)
are in the module docs, the code comments, the CHANGELOG, and the tests. Read
them, and the history (`git log -S`), before changing that code. Do not reverse
such behavior without a test that covers the original problem.

---

## Further Reading

- [Core Nebulex Architecture](https://hexdocs.pm/nebulex/architecture.html)
- [Creating a New Adapter](https://hexdocs.pm/nebulex/creating-new-adapter.html)
- [nebulex_local Architecture](https://github.com/elixir-nebulex/nebulex_local/blob/main/usage-rules/architecture.md)
- [nebulex_distributed HexDocs](https://hexdocs.pm/nebulex_distributed)
