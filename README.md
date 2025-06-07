# Nebulex Distributed :spider_web:
> Distributed cache topologies adapters for [Nebulex][Nebulex].

[Nebulex]: http://github.com/elixir-nebulex/nebulex

![CI](http://github.com/elixir-nebulex/nebulex_distributed/workflows/CI/badge.svg)
[![Codecov](http://codecov.io/gh/elixir-nebulex/nebulex_distributed/graph/badge.svg)](http://codecov.io/gh/elixir-nebulex/nebulex_distributed/graph/badge.svg)
[![Hex.pm](http://img.shields.io/hexpm/v/nebulex_distributed.svg)](http://hex.pm/packages/nebulex_distributed)
[![Documentation](http://img.shields.io/badge/Documentation-ff69b4)](http://hexdocs.pm/nebulex_distributed)

## About

One of the goals of Nebulex is also to provide the ability to set up distributed
cache topologies, but this feature will depend on the adapters. Here is where
**"Nebulex Distributed"** comes in. It provides the following adapters to set up
distributed topologies:

  * [`Nebulex.Adapters.Partitioned`][partitioned] - Partitioned cache topology.
  * [`Nebulex.Adapters.Multilevel`][multilevel] - Multi-level or near cache
    topology.
  * `Nebulex.Adapters.Replicated` - Replicated cache topology (**WIP!**).

These adapters work more as wrappers for an existing local adapter and provide
the distributed topology on top of it. You can optionally set the adapter for
the primary cache storage with the option `:primary_storage_adapter`.

[partitioned]: http://hexdocs.pm/nebulex_distributed/Nebulex.Adapters.Partitioned.html
[multilevel]: http://hexdocs.pm/nebulex_distributed/Nebulex.Adapters.Multilevel.html

## Installation

`:nebulex_distributed` requires Erlang/OTP 25 or later. Then add
`:nebulex_distributed` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:nebulex_distributed, "~> 3.0.0-rc.1"}
  ]
end
```

See the [online documentation][online_docs] for more information.

[online_docs]: http://hexdocs.pm/nebulex_distributed

## Partitioned cache topology example

You can define a partitioned cache as follows:

```elixir
defmodule MyApp.PartitionedCache do
  use Nebulex.Cache,
    otp_app: :my_app,
    adapter: Nebulex.Adapters.Partitioned
end
```

Where the configuration for the cache must be in your application
environment, usually defined in your `config/config.exs`:

```elixir
config :my_app, MyApp.PartitionedCache,
  primary: [
    gc_interval: :timer.hours(12),
    gc_memory_check_interval: :timer.seconds(10),
    max_size: 1_000_000,
    allocated_memory: 2_000_000_000
  ]
```

If your application was generated with a supervisor (by passing `--sup`
to `mix new`) you will have a `lib/my_app/application.ex` file containing
the application start callback that defines and starts your supervisor.
You just need to edit the `start/2` function to start the cache as a
supervisor on your application's supervisor:

```elixir
def start(_type, _args) do
  children = [
    {MyApp.PartitionedCache, []},
  ]

  ...
end
```

## Near cache topology example

To set up a near cache, you use the multilevel adapter, like so:

```elixir
defmodule MyApp.Multilevel do
  use Nebulex.Cache,
    otp_app: :nebulex,
    adapter: Nebulex.Adapters.Multilevel

  defmodule L1 do
    use Nebulex.Cache,
      otp_app: :nebulex,
      adapter: Nebulex.Adapters.Local
  end

  defmodule L2 do
    use Nebulex.Cache,
      otp_app: :nebulex,
      adapter: Nebulex.Adapters.Partitioned
  end
end
```

Where the configuration for the cache and its levels must be in your
application environment, usually defined in your `config/config.exs`:

```elixir
config :my_app, MyApp.Multilevel,
  levels: [
    {
      MyApp.Multilevel.L1,
      gc_interval: :timer.hours(12),
      backend: :shards
    },
    {
      MyApp.Multilevel.L2,
      primary: [
        gc_interval: :timer.hours(12),
        backend: :shards
      ]
    }
  ]
```

If your application was generated with a supervisor (by passing `--sup`
to `mix new`) you will have a `lib/my_app/application.ex` file containing
the application start callback that defines and starts your supervisor.
You just need to edit the `start/2` function to start the cache as a
supervisor on your application's supervisor:

```elixir
def start(_type, _args) do
  children = [
    {MyApp.Multilevel, []},
    ...
  ]
```

## Testing

Since this adapter uses support modules and shared tests from `Nebulex`,
but the test folder is not included in the Hex dependency, the following
steps are required to run the tests.

First of all, make sure you set the environment variable `NEBULEX_PATH`
to `nebulex`:

```
export NEBULEX_PATH=nebulex
```

Second, make sure you fetch `:nebulex` dependency directly from GtiHub
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
[benchee](http://github.com/PragTob/benchee), and they are located within
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

Use the [issue tracker](http://github.com/elixir-nebulex/nebulex_distributed/issues)
for bug reports or feature requests. Open a
[pull request](http://github.com/elixir-nebulex/nebulex_distributed/pulls)
when you are ready to contribute.

When submitting a pull request you should not update the
[CHANGELOG.md](CHANGELOG.md), and also make sure you test your changes
thoroughly, include unit tests alongside new or changed code.

Before to submit a PR it is highly recommended to run `mix test.ci` and ensure
all checks run successfully.

## Copyright and License

Copyright (c) 2024 Carlos Andres Bolaños R.A.

`nebulex_distributed` source code is licensed under the [MIT License](LICENSE).
