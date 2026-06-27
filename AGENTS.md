## Local Scope First (`nebulex_distributed`)

This repository is `nebulex_distributed` (the distributed adapters package),
not Nebulex core. When imported Nebulex sections reference missing
`usage-rules/*.md` paths or Nebulex-core files, treat them as upstream
guidance and prioritize this repository's local files and modules.

### Local Rule Precedence (for this repo)

When rules conflict, apply them in this order. Items 2–4 refer to the
rule files referenced via `@deps/…` at the bottom of this file; load
them as part of session bootstrap.

1. This local preface.
2. `nebulex:workflow`.
3. `nebulex:nebulex` (as framework guidance).
4. `nebulex:elixir-style` and `nebulex:elixir`.

### Local Key Files

> Keep this list current when modules are added, moved, or removed.
> A brief one-line description per file is enough.

- `lib/nebulex/adapters/partitioned.ex` - Partitioned adapter implementation.
- `lib/nebulex/adapters/multilevel.ex` - Multilevel adapter implementation.
- `lib/nebulex/adapters/coherent.ex` - Coherent adapter implementation.
- `lib/nebulex/adapters/replicated.ex` - Replicated adapter implementation.
- `lib/nebulex/adapters/replicated/replicator.ex` - Bootstrap and inbox/outbox apply logic.
- `lib/nebulex/adapters/replicated/cluster_monitor.ex` - PG monitor and bootstrap leader election.
- `lib/nebulex/adapters/replicated/anti_entropy.ex` - Merkle-style reconciliation cycle.
- `lib/nebulex/adapters/*/options.ex` - Adapter option definitions/docs.
- `lib/nebulex/adapters/*/supervisor.ex` - Adapter supervision trees.
- `lib/nebulex/distributed.ex` - Package umbrella module and docs entry point.
- `lib/nebulex/distributed/application.ex` - OTP application entry point.
- `lib/nebulex/distributed/cluster.ex` - Cluster membership and node discovery.
- `lib/nebulex/distributed/rpc.ex` - Remote procedure call helpers.
- `lib/nebulex/distributed/transaction.ex` - Distributed transaction coordination (and its `transaction/options.ex`).
- `lib/nebulex/distributed/helpers.ex` - Shared distributed utilities.
- `lib/nebulex/distributed/ring_monitor.ex` - Hash ring monitoring.
- `test/nebulex/adapters/partitioned_*.exs` - Partitioned adapter tests (incl. error, node-filter).
- `test/nebulex/adapters/multilevel_*.exs` - Multilevel adapter tests (inclusive, exclusive, error).
- `test/nebulex/adapters/coherent_test.exs` - Coherent adapter tests.
- `test/nebulex/adapters/replicated_*.exs` - Replicated adapter tests (incl. buffer-race, key-hasher).
- `test/nebulex/distributed/*_test.exs` - Distributed utility tests (RPC, helpers).
- `test/shared/` - Shared test cases (distributed, multilevel, transaction, info).
- `README.md` - Public usage/configuration for this package.
- `CHANGELOG.md` - Package release history.

<!-- usage-rules-start -->
<!-- nebulex:workflow-start -->
## nebulex:workflow usage
@deps/nebulex/usage-rules/workflow.md
<!-- nebulex:workflow-end -->
<!-- nebulex:nebulex-start -->
## nebulex:nebulex usage
@deps/nebulex/usage-rules/nebulex.md
<!-- nebulex:nebulex-end -->
<!-- nebulex:elixir-style-start -->
## nebulex:elixir-style usage
@deps/nebulex/usage-rules/elixir-style.md
<!-- nebulex:elixir-style-end -->
<!-- nebulex:elixir-start -->
## nebulex:elixir usage
@deps/nebulex/usage-rules/elixir.md
<!-- nebulex:elixir-end -->
<!-- usage_rules-start -->
## usage_rules usage
_A config-driven dev tool for Elixir projects to manage AGENTS.md files and agent skills from dependencies_

@deps/usage_rules/usage-rules.md
<!-- usage_rules-end -->
<!-- usage_rules:otp-start -->
## usage_rules:otp usage
@deps/usage_rules/usage-rules/otp.md
<!-- usage_rules:otp-end -->
<!-- usage-rules-end -->
