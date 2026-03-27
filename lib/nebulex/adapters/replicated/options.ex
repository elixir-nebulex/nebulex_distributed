defmodule Nebulex.Adapters.Replicated.Options do
  @moduledoc false

  # Compilation time options
  compile_opts = [
    primary_storage_adapter: [
      type: :atom,
      required: false,
      default: Nebulex.Adapters.Local,
      doc: """
      The adapter module used for the primary (local) storage on each cluster
      node. The replicated adapter wraps this local adapter and adds push-based
      replication on top of it. This option allows you to choose which adapter
      to use for the local storage. The configuration for the primary adapter
      is specified via the `:primary` start option.
      """
    ]
  ]

  # Replication options (nested under :replication)
  replication_opts = [
    interval: [
      type: :pos_integer,
      required: false,
      default: :timer.seconds(1),
      doc: """
      How often (in milliseconds) the outbox and inbox buffers swap tables
      and run the processing cycle. Lower values mean faster replication
      but more frequent task spawning. Maps to the `:processing_interval_ms`
      option of `PartitionedBuffer.Map`.
      """
    ],
    batch_size: [
      type: :pos_integer,
      required: false,
      default: 1_000,
      doc: """
      Number of entries to read from ETS per batch when flushing the outbox
      and inbox buffers. The processor is called once per batch. Maps to
      the `:processing_batch_size` option of `PartitionedBuffer.Map`.
      """
    ],
    timeout: [
      type: :pos_integer,
      required: false,
      default: :timer.minutes(1),
      doc: """
      Timeout in milliseconds for the RPC multicall when replicating
      buffered commands to peer nodes.
      """
    ],
    retries: [
      type: :non_neg_integer,
      required: false,
      default: 3,
      doc: """
      Number of times to retry replicating to a failed peer node before
      giving up.
      """
    ],
    retry_delay: [
      type: :pos_integer,
      required: false,
      default: 100,
      doc: """
      Delay in milliseconds between replication retry attempts.
      """
    ],
    partitions: [
      type: :pos_integer,
      required: false,
      doc: """
      Number of partitions for the inbox and outbox buffers. More
      partitions reduce contention under high write concurrency.
      Maps to the `:partitions` option of `PartitionedBuffer.Map`.
      Defaults to `System.schedulers_online()`.
      """
    ],
    anti_entropy_interval: [
      type: :pos_integer,
      required: false,
      doc: """
      Interval in milliseconds between anti-entropy reconciliation
      cycles. When set, a background process periodically picks a
      random peer, compares bucket-hashed Merkle digests of the local
      and remote caches, and repairs only the divergent keys by
      writing them through the inbox (preserving "newer version wins"
      semantics).

      Disabled by default (not present). Set to a positive integer
      to enable, e.g., `:timer.minutes(1)`.
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
      Configuration options passed to the primary storage adapter specified via
      `:primary_storage_adapter`. The available options depend on which adapter
      you choose. Refer to the documentation of your chosen primary storage
      adapter for the complete list of supported options.
      """
    ],
    replication: [
      type: :keyword_list,
      required: false,
      default: [],
      keys: replication_opts,
      doc: """
      Configuration options for the push-based replication layer. Controls
      how often buffered commands are flushed, batch sizes, RPC timeouts,
      and retry behavior.
      """
    ]
  ]

  # Nebulex common options
  @nbx_start_opts Nebulex.Cache.Options.__compile_opts__() ++ Nebulex.Cache.Options.__start_opts__()

  # Compilation time option schema
  @compile_opts_schema NimbleOptions.new!(compile_opts)

  # Start options schema
  @start_opts_schema NimbleOptions.new!(start_opts)

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
end
