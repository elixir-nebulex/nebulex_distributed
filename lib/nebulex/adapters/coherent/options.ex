defmodule Nebulex.Adapters.Coherent.Options do
  @moduledoc false

  alias Nebulex.Streams

  # Compilation time options
  compile_opts = [
    primary_storage_adapter: [
      type: :atom,
      required: false,
      default: Nebulex.Adapters.Local,
      doc: """
      The adapter module used for the primary (local) storage on each cluster
      node. The coherent adapter wraps this local adapter and adds distributed
      invalidation on top of it using `Nebulex.Streams`. This option allows you
      to choose which adapter to use for the local storage. The configuration
      for the primary adapter is specified via the `:primary` start option.
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
    stream_opts: [
      type: :keyword_list,
      required: false,
      default: [],
      doc: """
      Configuration options for the event stream used for distributed
      invalidation. The stream broadcasts cache events (inserts, updates,
      deletes) to all nodes in the cluster, enabling automatic invalidation
      of stale entries.

      **Note:** The following options are set automatically by the adapter
      and cannot be overridden:

        * `:cache` - Derived from the cache module.
        * `:name` - Derived from the cache instance name.
        * `:broadcast_fun` - Set to `:broadcast_from` to avoid self-invalidation.
      """,
      keys: [
        pubsub: [
          type: :atom,
          required: false,
          default: Nebulex.Streams.PubSub,
          doc: """
          The `Phoenix.PubSub` instance to use for event broadcasting.

          Defaults to `Nebulex.Streams.PubSub`. You can provide a custom PubSub
          instance if you want to use your application's existing PubSub. The
          specified PubSub must be started in your supervision tree.
          """
        ],
        backoff_initial: [
          type: :non_neg_integer,
          required: false,
          default: :timer.seconds(1),
          doc: """
          Initial backoff time in milliseconds for listener re-registration.

          When the stream server fails to register the event listener, it will
          wait this amount of time before retrying. The backoff time increases
          exponentially up to `:backoff_max`.
          """
        ],
        backoff_max: [
          type: :timeout,
          required: false,
          default: :timer.seconds(30),
          doc: """
          Maximum backoff time in milliseconds for listener re-registration.

          When retrying failed listener registration, the backoff time will not
          exceed this value.
          """
        ],
        partitions: [
          type: :pos_integer,
          required: false,
          doc: """
          Number of partitions for parallel event processing.

          When provided, events are divided into this many independent
          sub-streams, allowing multiple invalidator workers to process events
          in parallel. Each partition has its own topic and worker.

          Typical values:

          - Omit or 1: Low event volume (single worker handles all events).
          - `System.schedulers_online()`: CPU-bound event processing.
          - `System.schedulers_online() * 2`: I/O-bound event processing.
          """
        ],
        hash: [
          type: {:fun, 1},
          type_doc: "`t:Nebulex.Streams.hash/0`",
          required: false,
          default: &Streams.default_hash/1,
          doc: """
          Custom hash function for routing events to partitions.

          This function receives a `Nebulex.Event.CacheEntryEvent` and returns
          either:

          - A partition number (0 to partitions-1): routes the event to that
            partition.
          - `:none`: discards the event entirely.

          Defaults to `Nebulex.Streams.default_hash/1` which uses `phash2` for
          even distribution.

          The hash function is only used when `:partitions` is configured.
          """
        ]
      ]
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
