# Project Rules

This repository is `nebulex_distributed`, the distributed adapters package
for [Nebulex](https://github.com/elixir-nebulex/nebulex). The shared rules
come from the `nebulex` dependency, so run `mix deps.get` first; the
`@deps/…` files do not exist in a fresh clone. The workflow below says what
to read for each kind of task; this repo's architecture guide replaces the
core one.

@deps/nebulex/usage-rules/workflow.md

@usage-rules/architecture.md

## Running Tests

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

<!-- usage-rules-start -->
<!-- nebulex:nebulex-start -->
## nebulex:nebulex usage
[nebulex:nebulex usage rules](deps/nebulex/usage-rules/nebulex.md)
<!-- nebulex:nebulex-end -->
<!-- nebulex:elixir-style-start -->
## nebulex:elixir-style usage
[nebulex:elixir-style usage rules](deps/nebulex/usage-rules/elixir-style.md)
<!-- nebulex:elixir-style-end -->
<!-- usage_rules-start -->
## usage_rules usage
_A config-driven dev tool for Elixir projects to manage AGENTS.md files and agent skills from dependencies_

[usage_rules usage rules](deps/usage_rules/usage-rules.md)
<!-- usage_rules-end -->
<!-- usage-rules-end -->
