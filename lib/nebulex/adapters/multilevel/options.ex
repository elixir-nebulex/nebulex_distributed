defmodule Nebulex.Adapters.Multilevel.Options do
  @moduledoc false

  # Start options
  start_opts = [
    stats: [
      type: :boolean,
      required: false,
      default: true,
      doc: """
      A flag to determine whether to collect cache stats.
      """
    ],
    levels: [
      type: :non_empty_keyword_list,
      required: true,
      doc: """
      This option is to define the levels, a list of tuples in the shape
      `{cache_level :: Nebulex.Cache.t(), opts :: keyword()}`, where the first
      element is the module that defines the cache for that level, and the
      second one is the options given to that level in the `start_link/1`.
      The multi-level cache adapter relies on the list order to determine the
      level hierarchy. For example, the first element in the list will be the
      L1 cache (level 1), and so on; the Nth element will be the LN cache.
      This option is required; if it is not set or empty, the adapter raises
      an exception.
      """
    ],
    inclusion_policy: [
      type: {:in, [:inclusive, :exclusive]},
      required: false,
      default: :inclusive,
      doc: """
      Specifies the cache inclusion policy: `:inclusive` or `:exclusive`.

      For an "inclusive" cache, the same data can be present in all cache
      levels. On the other hand, in an "exclusive" cache, the data can be
      present in only one cache level; the key cannot exist in the rest of the
      levels at the same time. This option applies to the callback `get` only;
      if the cache inclusion policy is `:inclusive`, when the key does exist in a level
      N, that entry is duplicated backward (to all previous levels: 1..N-1).
      However, when the mode is `:inclusive`, the `get_all` operation is
      translated into multiple `get` calls underneath (which may be a
      significant performance penalty) since it requires replicating the entries
      properly with their current TTLs. It is possible to skip the replication
      when calling `get_all` using the option `:replicate`.
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
    ],
    level: [
      type: :pos_integer,
      required: false,
      doc: """
      Dictates the level where the cache command will take place. The evaluation
      is performed by default throughout the cache hierarchy (all levels).
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
      This option applies only to the `get_all` callback when using the
      inclusive policy. Determines whether the entries should be replicated
      to the backward levels or not.
      """
    ],
    on_error: [
      type: {:in, [:nothing, :raise]},
      type_doc: "`:raise` | `:nothing`",
      required: false,
      default: :raise,
      doc: """
      Indicates whether to raise an exception when an error occurs or do nothing
      (skip errors).

      When the stream is evaluated, the adapter attempts to execute the `stream`
      command on the different cache levels. Still, the execution could fail at
      any of the cache levels. If the option is set to `:raise`, the command
      will raise an exception when an error occurs on the stream evaluation.
      On the other hand, if it is set to `:nothing`, the error is skipped.
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
