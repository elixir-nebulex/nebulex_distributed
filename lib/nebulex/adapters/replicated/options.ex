defmodule Nebulex.Adapters.Replicated.Options do
  @moduledoc false

  alias Nebulex.Adapters.Coherent.Options, as: CoherentOptions

  # Common runtime options
  common_runtime_opts = [
    timeout: [
      type: :timeout,
      required: false,
      default: 5000,
      doc: """
      The time in **milliseconds** to wait for an RPC call to a peer node
      to finish when pulling data on a cache miss.

      This timeout applies to RPC calls made to peer nodes during the
      lazy-pull replication. Set to `:infinity` to wait indefinitely.
      """
    ]
  ]

  # Nebulex common options
  @nbx_start_opts Nebulex.Cache.Options.__compile_opts__() ++ Nebulex.Cache.Options.__start_opts__()

  # Compilation time option schema
  @compile_opts_schema CoherentOptions.shared_compile_opts() |> NimbleOptions.new!()

  # Start options schema
  @start_opts_schema CoherentOptions.shared_start_opts() |> NimbleOptions.new!()

  # Common runtime options schema
  @common_runtime_opts_schema NimbleOptions.new!(common_runtime_opts)

  ## Docs API

  # coveralls-ignore-start

  @spec compile_options_docs() :: binary()
  def compile_options_docs do
    NimbleOptions.docs(@compile_opts_schema)
  end

  @spec start_options_docs() :: binary()
  def start_options_docs do
    NimbleOptions.docs(@start_opts_schema)
  end

  @spec common_runtime_options_docs() :: binary()
  def common_runtime_options_docs do
    NimbleOptions.docs(@common_runtime_opts_schema)
  end

  # coveralls-ignore-stop

  ## Validation API

  @spec validate_compile_opts!(keyword()) :: keyword()
  def validate_compile_opts!(opts) do
    NimbleOptions.validate!(opts, @compile_opts_schema)
  end

  @spec validate_start_opts!(keyword()) :: keyword()
  def validate_start_opts!(opts) do
    adapter_opts =
      opts
      |> Keyword.drop(@nbx_start_opts)
      |> NimbleOptions.validate!(@start_opts_schema)

    Keyword.merge(opts, adapter_opts)
  end

  @spec validate_common_runtime_opts!(keyword()) :: keyword()
  def validate_common_runtime_opts!(opts) do
    adapter_opts =
      opts
      |> Keyword.take([:timeout])
      |> NimbleOptions.validate!(@common_runtime_opts_schema)

    Keyword.merge(opts, adapter_opts)
  end
end
