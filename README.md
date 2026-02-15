# Nebulex Distributed :spider_web:
> Distributed cache topologies adapters for [Nebulex][Nebulex].

[Nebulex]: https://github.com/elixir-nebulex/nebulex

![CI](https://github.com/elixir-nebulex/nebulex_distributed/workflows/CI/badge.svg)
[![Codecov](https://codecov.io/gh/elixir-nebulex/nebulex_distributed/graph/badge.svg)](https://codecov.io/gh/elixir-nebulex/nebulex_distributed)
[![Hex.pm](https://img.shields.io/hexpm/v/nebulex_distributed.svg)](https://hex.pm/packages/nebulex_distributed)
[![Documentation](https://img.shields.io/badge/Documentation-ff69b4)](https://hexdocs.pm/nebulex_distributed)

## About

One of the goals of Nebulex is also to provide the ability to set up distributed
cache topologies, but this feature will depend on the adapters. Here is where
**"Nebulex Distributed"** comes in. It provides the following adapters to set up
distributed topologies:

  * [`Nebulex.Adapters.Partitioned`][partitioned] - Partitioned cache topology.
  * [`Nebulex.Adapters.Multilevel`][multilevel] - Multi-level or near cache
    topology.
  * [`Nebulex.Adapters.Coherent`][coherent] - Local cache with distributed
    invalidation via `Nebulex.Streams`.
  * 🚧 `Nebulex.Adapters.Replicated` - Replicated cache topology (**WIP!**).

These adapters work more as wrappers for an existing local adapter and provide
the distributed topology on top of it. You can optionally set the adapter for
the primary cache storage with the option `:primary_storage_adapter`.

[partitioned]: https://hexdocs.pm/nebulex_distributed/Nebulex.Adapters.Partitioned.html
[multilevel]: https://hexdocs.pm/nebulex_distributed/Nebulex.Adapters.Multilevel.html
[coherent]: https://hexdocs.pm/nebulex_distributed/Nebulex.Adapters.Coherent.html

## Installation

`:nebulex_distributed` requires Erlang/OTP 25 or later. Then add
`:nebulex_distributed` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:nebulex_distributed, "~> 3.0"},
    {:telemetry, "~> 0.4 or ~> 1.0"},  # For observability/telemetry support
    {:decorator, "~> 1.4"},            # For declarative caching
  ]
end
```

The `:telemetry` (observability and monitoring cache operations) and
`:decorator` (declarative caching) dependencies are optional but highly
recommended.

See the [online documentation][online_docs] for more information.

[online_docs]: https://hexdocs.pm/nebulex_distributed

## Testing

Since this adapter uses support modules and shared tests from `Nebulex`,
but the test folder is not included in the Hex dependency, the following
steps are required to run the tests.

First of all, make sure you set the environment variable `NEBULEX_PATH`
to `nebulex`:

```
export NEBULEX_PATH=nebulex
```

Second, make sure you fetch `:nebulex` dependency directly from GitHub
by running:

```
mix nbx.setup
```

Third, fetch deps:

```
mix deps.get
```

Finally, you can run the tests:

```
mix test
```

Running tests with coverage:

```
mix coveralls.html
```

You will find the coverage report within `cover/excoveralls.html`.

## Benchmarks

The adapter provides a set of basic benchmark tests using the library
[benchee](https://github.com/PragTob/benchee), and they are located within
the directory [benchmarks](./benchmarks).

To run a benchmark test you have to run:

```
mix run benchmarks/BENCH_TEST_FILE
```

Where `BENCH_TEST_FILE` can be any of:

  * `partitioned_bench.exs`: benchmark for the partitioned adapter using
    the `Nebulex.Adapters.Local` as primary storage.
  * `multilevel_bench.exs`: benchmark for the multilevel adapter.

## Contributing

Contributions to Nebulex are very welcome and appreciated!

Use the [issue tracker](https://github.com/elixir-nebulex/nebulex_distributed/issues)
for bug reports or feature requests. Open a
[pull request](https://github.com/elixir-nebulex/nebulex_distributed/pulls)
when you are ready to contribute.

When submitting a pull request you should not update the
[CHANGELOG.md](CHANGELOG.md), and also make sure you test your changes
thoroughly, include unit tests alongside new or changed code.

Before to submit a PR it is highly recommended to run `mix test.ci` and ensure
all checks run successfully.

## Copyright and License

Copyright (c) 2024-2026 Carlos Andres Bolaños R.A.

`nebulex_distributed` source code is licensed under the [MIT License](LICENSE.md).
