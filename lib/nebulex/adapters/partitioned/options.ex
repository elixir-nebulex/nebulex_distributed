defmodule Nebulex.Adapters.Partitioned.Options do
  @moduledoc false

  # Compilation time options
  compile_opts = [
    primary_storage_adapter: [
      type: :atom,
      required: false,
      default: Nebulex.Adapters.Local,
      doc: """
      The adapter module used for the primary (local) storage on each cluster
      node. The partitioned adapter is a distributed wrapper that routes
      requests to the appropriate node based on consistent hashing. The actual
      data storage is handled by the primary storage adapter on each node.
      This option allows you to choose which adapter to use for this local
      storage. The configuration for the primary adapter is specified via the
      `:primary` start option.
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
    hash_ring: [
      type: :keyword_list,
      required: false,
      default: [],
      doc: """
      Configuration options for the consistent hash ring used to distribute keys
      across cluster nodes. The hash ring maps each key to a node using virtual
      nodes (vnodes), which enables:

        * Minimal key redistribution when nodes join or leave.
        * Even distribution of keys across nodes.
        * Efficient node lookup for cache operations.

      See [`ExHashRing.Ring.start_link/2`][ex_hash_ring] for the complete list
      of supported options.

      [ex_hash_ring]: https://hexdocs.pm/ex_hash_ring/ExHashRing.Ring.html#start_link/1
      """
    ],
    rejoin_interval: [
      type: :timeout,
      required: false,
      default: :timer.seconds(30),
      doc: """
      The interval in **milliseconds** at which the `RingMonitor` periodically
      rejoins the `:pg` group to force ring synchronization across all cluster
      nodes.

      **Purpose:** This mechanism helps handle race conditions during concurrent
      node startups by ensuring all nodes eventually have a consistent view of
      the hash ring. Even if some join events are missed during initial cluster
      formation, each rejoin triggers new notifications that force ring updates.

      **Trade-offs:**

        * **Shorter intervals** (e.g., 10 seconds):
          - Faster consistency convergence.
          - More overhead from frequent rejoin events and notifications.
          - Better for highly dynamic clusters with frequent node changes.

        * **Longer intervals** (e.g., 60 seconds):
          - Lower overhead and reduced network traffic.
          - Slower eventual consistency.
          - Fine for stable clusters that don't change frequently.

      **Default (30 seconds):** Works well for most use cases, balancing
      consistency and overhead.
      """
    ],
    node_filter: [
      type: {:fun, 1},
      required: false,
      doc: """
      An optional 1-arity function that filters which cluster nodes are
      added to the hash ring. The function receives a node name
      (`t:node/0`) and must return `true` to include the node in the
      ring, or `false` to exclude it. Only nodes present in the ring
      will be selected to cache data.

      Excluded nodes are still part of the cache cluster (`:pg` group),
      so the cache remains fully usable from them — reads, writes, and
      all other operations work transparently, routing to ring nodes
      as usual.

      By default, all nodes that join the cluster are added to the
      hash ring.

      > #### Function Captures {: .info}
      >
      > Due to how anonymous functions are implemented in the Erlang VM,
      > it is best to use function captures (`&Mod.fun/1`) as node filters.
      > In other words, avoid using literal anonymous functions
      > (`fn ... -> ... end`) or local function captures (`&my_filter/1`)
      > as they cannot be serialized across distributed nodes.

      See the ["Node Filter"](`Nebulex.Adapters.Partitioned#module-node-filter`)
      section for more information and examples.
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
      The time in **milliseconds** to wait for a cache command to finish.

      This timeout applies to RPC calls made to remote nodes during partitioned
      cache operations. Since the partitioned adapter routes requests across
      cluster nodes, network latency and node load affect execution time.

      Set to `:infinity` to wait indefinitely. If a timeout occurs, the
      operation fails with an error. Note that the underlying cache operation
      may still complete on the remote node asynchronously.
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
      Controls error handling during stream evaluation across cluster nodes.

      When streaming entries from a partitioned cache, the adapter evaluates the
      stream on each cluster node. Since this involves RPC calls to remote
      nodes, failures can occur due to:

        * Network issues or RPC timeouts.
        * Errors on the remote node.
        * Temporary node unavailability.

      **Options:**

        * `:raise` (default) - Raise an exception immediately when an error
          occurs on any node. The stream evaluation stops, and no further nodes
          are queried.

        * `:nothing` - Skip errors silently and continue. Returns only
          successful results from nodes that responded without errors.
          Useful for resilience in environments where temporary node
          failures are acceptable.

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
