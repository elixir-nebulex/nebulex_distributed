defmodule Nebulex.Adapters.Partitioned.Options do
  @moduledoc false

  # Compilation time options
  compile_opts = [
    primary_storage_adapter: [
      type: :atom,
      required: false,
      default: Nebulex.Adapters.Local,
      doc: """
      The adapter for the primary storage.
      """
    ]
  ]

  # Start options
  start_opts = [
    primary: [
      type: :keyword_list,
      required: false,
      default: [],
      doc: """
      Options for the adapter configured via the `:primary_storage_adapter`
      option. The options will vary depending on the adapter used.
      """
    ],
    hash_ring: [
      type: :keyword_list,
      required: false,
      default: [],
      doc: """
      Options for the hash ring.
      See [`ExHashRing.Ring.start_link/2`][ex_hash_ring] for more information.

      [ex_hash_ring]: https://hexdocs.pm/ex_hash_ring/ExHashRing.Ring.html#start_link/1
      """
    ]
  ]

  # Common runtime options
  common_runtime_opts = [
    timeout: [
      type: :timeout,
      required: false,
      default: 5000,
      doc: """
      The time in **milliseconds** to wait for a command to finish
      (`:infinity` to wait indefinitely).
      """
    ]
  ]

  # Stream options
  stream_opts = [
    on_error: [
      type: {:in, [:nothing, :raise]},
      type_doc: "`:raise` | `:nothing`",
      required: false,
      default: :raise,
      doc: """
      Indicates whether to raise an exception when an error occurs or do nothing
      (skip errors).

      When the stream is evaluated, the adapter attempts to execute the `stream`
      command on the different nodes. Still, the execution could fail due to an
      RPC error or the command explicitly returns an error. If the option is set
      to `:raise`, the command will raise an exception when an error occurs on
      the stream evaluation. On the other hand, if it is set to `:nothing`, the
      error is skipped.
      """
    ]
  ]

  # Nebulex common options
  @nbx_start_opts Nebulex.Cache.Options.__compile_opts__() ++ Nebulex.Cache.Options.__start_opts__()

  # Compilation time option schema
  @compile_opts_schema NimbleOptions.new!(compile_opts)

  # Start options schema
  @start_opts_schema NimbleOptions.new!(start_opts)

  # Common runtime options schema
  @common_runtime_opts_schema NimbleOptions.new!(common_runtime_opts)

  # Stream options schema
  @stream_opts_schema NimbleOptions.new!(stream_opts ++ common_runtime_opts)

  # Stream options docs schema
  @stream_opts_docs_schema NimbleOptions.new!(stream_opts)

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

  @spec stream_options_docs() :: binary()
  def stream_options_docs do
    NimbleOptions.docs(@stream_opts_docs_schema)
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

  @spec validate_stream_opts!(keyword()) :: keyword()
  def validate_stream_opts!(opts) do
    adapter_opts =
      opts
      |> Keyword.take([:timeout, :on_error])
      |> NimbleOptions.validate!(@stream_opts_schema)

    Keyword.merge(opts, adapter_opts)
  end
end
