defmodule Nebulex.Adapters.Multilevel.Options do
  @moduledoc false

  # Start options
  start_opts = [
    stats: [
      type: :boolean,
      required: false,
      default: true,
      doc: """
      Enables or disables cache statistics collection (default: enabled).

      When enabled, collects hit/miss/write statistics available via the
      `info()` command. Statistics are collected per-level and aggregated at
      the multi-level cache level. See the ["Info API"](#module-info-api)
      section for details on how to access cache statistics.
      """
    ],
    levels: [
      type: :non_empty_keyword_list,
      required: true,
      doc: """
      Defines the cache hierarchy as a non-empty keyword list of cache levels.

      Each element must be a tuple of `{cache_module, opts}` where:

        * `cache_module` - The cache module to use for this level
          (e.g., `MyApp.Multilevel.L1`, `MyApp.Multilevel.L2`).
        * `opts` - Keyword list of options passed to that cache's
          `start_link/1`.

      **Level Ordering**: The order of elements determines the hierarchy:
        - First element = L1 (fastest, checked first)
        - Second element = L2 (slower, larger capacity)
        - Nth element = LN (slowest, largest capacity)

      **Example:**

          levels: [
            {MyApp.Multilevel.L1, gc_interval: :timer.hours(12)},
            {MyApp.Multilevel.L2, primary: [gc_interval: :timer.hours(12)]}
          ]

      This option is **required**. If not set or empty, the adapter raises an
      exception. Each level must be a different cache module instance.
      """
    ],
    inclusion_policy: [
      type: {:in, [:inclusive, :exclusive]},
      required: false,
      default: :inclusive,
      doc: """
      Specifies whether the same data can exist in multiple cache levels
      simultaneously (default: inclusive).

      `:inclusive` - Same key can exist in L1, L2, L3, etc. simultaneously.
      On read, if found in L2 but not L1, automatically replicate back to L1
      for faster future reads. Trade-off: Uses more memory (data duplicated
      in multiple levels). The `get_all` operation is slower because each entry
      requires per-entry replication. Use the `:replicate` option to skip
      replication if needed.

      `:exclusive` - Same key can exist in only one level at a time. On read,
      return value WITHOUT replicating to L1. Trade-off: Reads after L1
      eviction must fetch from slower levels. Good for large datasets or
      strict memory constraints.

      See the ["How Multi-Level Caches Work"](#module-how-multi-level-caches-work)
      section for detailed examples and workflow diagrams.
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
      The time in milliseconds to wait for a command to complete. Set to
      `:infinity` to wait indefinitely.

      **Note**: The timeout applies to each level independently.
      """
    ],
    level: [
      type: :pos_integer,
      required: false,
      doc: """
      An integer greater than 0 that specifies the cache level to execute the
      operation on.

      > #### WARNING {: .warning}
      >
      > Using this option **breaks the multi-level cache semantics**
      > and is **not recommended** for normal operations. It's primarily useful
      > for:
      > - Debugging and testing.
      > - Administrative tasks.
      > - Advanced use cases where you need direct level access.
      """
    ]
  ]

  # Queryable options
  queryable_opts = [
    replicate: [
      type: :boolean,
      required: false,
      default: true,
      doc: """
      Controls whether entries are replicated backward during `get_all`
      operations (default: replicate).

      Applies only to `get_all` when using `:inclusive` inclusion policy.

      When enabled, entries found in L2 are automatically replicated back to L1
      for faster future reads. Trade-off: Each entry requires a replication
      operation. When disabled, entries are returned without replicating to L1,
      which is faster for bulk reads where replication is unnecessary.

      Example - Fast bulk read without L1 replication:

          MyCache.get_all(:user_ids, replicate: false)

      This only affects `get_all`. Regular `get` always respects the inclusion
      policy. Ignored when using `:exclusive` policy (no replication occurs).
      """
    ],
    on_error: [
      type: {:in, [:nothing, :raise]},
      type_doc: "`:raise` | `:nothing`",
      required: false,
      default: :raise,
      doc: """
      Controls error handling during queryable operations (`get_all`,
      `count_all`, `delete_all`, `stream`).

      `:raise` - Raise an exception if any error occurs on any level. Fail-fast
      with no partial results. Use for correctness-critical operations where you
      need guarantee of success or explicit failure.

      `:nothing` - Skip errors silently and continue processing. Returns partial
      results from levels that succeeded. Use for large bulk reads, analytics,
      or best-effort operations where partial results are acceptable.

      Errors can occur from network issues (RPC timeout), level failures,
      unavailability, or data corruption.
      """
    ]
  ]

  # Nebulex common options
  @nbx_start_opts Nebulex.Cache.Options.__compile_opts__() ++ Nebulex.Cache.Options.__start_opts__()

  # Start options schema
  @start_opts_schema NimbleOptions.new!(start_opts)

  # Common runtime options schema
  @common_runtime_opts_schema NimbleOptions.new!(common_runtime_opts)

  # Queryable options schema
  @queryable_opts_schema NimbleOptions.new!(common_runtime_opts ++ queryable_opts)

  # Queryable options schema only for docs
  @queryable_opts_schema_docs NimbleOptions.new!(queryable_opts)

  ## Docs API

  # coveralls-ignore-start

  @spec start_options_docs() :: binary()
  def start_options_docs do
    NimbleOptions.docs(@start_opts_schema)
  end

  @spec common_runtime_options_docs() :: binary()
  def common_runtime_options_docs do
    NimbleOptions.docs(@common_runtime_opts_schema)
  end

  @spec queryable_options_docs() :: binary()
  def queryable_options_docs do
    NimbleOptions.docs(@queryable_opts_schema_docs)
  end

  # coveralls-ignore-stop

  ## Validation API

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

  @spec validate_queryable_opts!(keyword()) :: keyword()
  def validate_queryable_opts!(opts) do
    adapter_opts =
      opts
      |> Keyword.take([:replicate, :on_error])
      |> NimbleOptions.validate!(@queryable_opts_schema)

    Keyword.merge(opts, adapter_opts)
  end
end
