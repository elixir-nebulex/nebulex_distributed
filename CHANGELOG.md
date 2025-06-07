# Changelog

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
