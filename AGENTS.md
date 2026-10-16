## Local Scope First (`nebulex_distributed`)

This repository is `nebulex_distributed`, the distributed adapters package
for [Nebulex](https://github.com/elixir-nebulex/nebulex), not Nebulex core.
When imported Nebulex sections reference missing `usage-rules/*.md` paths or
Nebulex-core files, treat them as upstream guidance and prioritize this
repository's local files and modules.

Read `usage-rules/architecture.md` first (imported below). It covers the
module boundaries, the design decisions behind the subtle parts, and the
non-negotiable contribution rules. For how the adapters work, read the
module docs it links to.

@usage-rules/architecture.md

### Running Tests

Run `mix deps.get` first. It also fetches the rule files that the `@deps/…`
references at the bottom of this file point to; they do not exist in a fresh
clone.

The test suite reuses the shared test cases from the Nebulex core repository,
which are not part of the Hex package. `mix nbx.setup` clones the `main`
branch into `./nebulex`, which is what CI runs against. The tests start peer
nodes, so `epmd` must be running:

```bash
mix deps.get
mix nbx.setup
epmd -daemon
NEBULEX_PATH=nebulex mix test
```

To run against another checkout, point `NEBULEX_PATH` at it. To skip the
tests that need peer nodes, add `--exclude clustered`.

### Local Rule Precedence (for this repo)

When rules conflict, apply them in this order. Items 3–6 refer to the
rule files referenced via `@deps/…` at the bottom of this file; load
them as part of session bootstrap if your agent does not follow `@`
imports.

1. This local preface.
2. `usage-rules/architecture.md` (this repo's architecture and
   non-negotiables; it replaces the core architecture guide that
   `nebulex:workflow` refers to).
3. `nebulex:workflow`.
4. `nebulex:nebulex` (as framework guidance).
5. `nebulex:elixir-style` and `nebulex:elixir`.
6. `usage_rules` and `usage_rules:otp`.

### Style Rules Are Non-Negotiable

Every change in `lib/` and `test/` must follow `nebulex:elixir-style`. Do not
treat these rules as suggestions, and do not skip them because
`mix format` and `mix credo --strict` pass; the tools do not check most of
them. Before you finish a change, check the diff against the rules. Pay
attention to these rules, which are easy to miss:

- All clauses of a function use the same form: all single-line (`do:`) or
  all multiline (`do ... end`). Do not mix the two forms.
- A function body with more than one expression has a blank line before the
  final expression. This includes `fn` bodies and `case`/`cond` clause
  bodies.
- Add a blank line after a multiline assignment.
- A function that returns a pipe writes the pipe on multiple lines.
- Do not pipe once from a value that is not a function call.

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
- `test/support/` - Test caches and the peer-node cluster helpers.
- `usage-rules/architecture.md` - Module boundaries, design decisions, and non-negotiables.
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
