# Architecture

This guide covers what the code and the module docs do not: how the modules
split responsibility, the design decisions behind the subtle parts, and the
non-negotiable rules for every change. Read the "Non-Negotiables" section at
session start; read the rest before structural changes.

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

The per-file map lives in the "Local Key Files" section of `AGENTS.md`.
This section covers only how the modules split responsibility:

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
  streams the bootstrap entries, `ClusterMonitor` watches node joins and
  elects the bootstrap leader, and `AntiEntropy` runs the reconciliation
  cycle.

---

## Design Decisions

Each of these fixed a real bug or design problem. Do not reverse one without
a documented reason and a test that covers the original problem.

### Partitioned composite commands run on the owner node

`get_and_update`, `update`, `fetch_or_store`, and `get_or_store` send the
whole command to the node that owns the key in a single RPC, and the
function runs there against the primary storage. Running it on the calling
node broke the documented semantics (#19). Because of this, the function
must exist on every node: use a capture of a named function, not an
anonymous function, in docs and tests.

### Replicated writes are local first, then pushed in batches

A write is applied to the local primary first and then pushed to the peers
through the outbox/inbox buffers (`Tidefall`), in batches. If a write lands
while the cache is shutting down and the buffer is already gone, the write
stays local, the adapter emits `[:replication, :discarded]`, and the
callback still returns `:ok` (#18). Do not let buffer errors escape the
write callbacks.

### Versions use wall-clock time

Conflicts resolve as "newer version wins", with versions from
`System.system_time()`. `System.monotonic_time()` has a per-node epoch and
cannot be compared across nodes (#14). Do not switch back.

### Bootstrap is push-based, streamed, and silent

When a node joins, the existing node with the smallest name (Erlang term
ordering) pushes its entries to it with `:put_new`, so the joining node
never overwrites newer writes (#14). Entries go out in chunks of
`:bootstrap_chunk_size`, and the internal calls used by bootstrap and
anti-entropy pass `telemetry: false`. Loading the whole cache into one list
and emitting a span per entry ran nodes with large caches out of memory on
deploy (#15).

### Anti-entropy repairs through the inbox

Anti-entropy hashes keys into 1024 fixed buckets with XOR fingerprints,
compares with one random peer per cycle, and repairs only the divergent
entries. Repairs go through the inbox, so they keep the "newer version wins"
semantics.

### Keys must be valid in a match-spec head, or hashed

The replication buffers match on keys, so keys that are not valid in an ETS
match-spec head (for example maps) need the `:key_hasher` option. Without
it, the second write to such a key crashed the buffer and the write was
lost. Every node must use the same hasher.

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
optional dependency, or reversing or adding a design decision above),
review this document and update it if needed. When you add, move, or
remove a file, update the "Local Key Files" list in `AGENTS.md`. Do not
copy here what the module docs already say; link to them. A stale
architecture guide is worse than no guide.

### 5. `mix precommit` must pass

```bash
NEBULEX_PATH=nebulex mix precommit
```

---

## Further Reading

- [Core Nebulex Architecture](https://hexdocs.pm/nebulex/architecture.html)
- [Creating a New Adapter](https://hexdocs.pm/nebulex/creating-new-adapter.html)
- [nebulex_local Architecture](https://github.com/elixir-nebulex/nebulex_local/blob/main/usage-rules/architecture.md)
- [nebulex_distributed HexDocs](https://hexdocs.pm/nebulex_distributed)
