defmodule Nebulex.Adapters.Multilevel do
  @moduledoc """
  Adapter module for the multi-level cache topology.

  The Multi-level adapter is a simple layer that works on top of a local or
  distributed cache implementation, enabling a cache hierarchy by levels.
  Multi-level caches generally operate by checking the fastest,
  level 1 (L1) cache first; if it hits, the adapter proceeds at
  high speed. If that first cache misses, the next fastest cache
  (level 2, L2) is checked, and so on, before accessing external
  memory (that can be handled by a `cacheable` decorator).

  For write functions, the "Write Through" policy is applied by default;
  this policy ensures that the data is stored safely as it is written
  throughout the hierarchy. However, it is possible to force the write
  operation in a specific level (although it is not recommended) via
  `:level` option, where the value is a positive integer greater than 0.

  ## How Multi-Level Caches Work

  A multi-level cache is organized in a hierarchy of cache layers, where each
  layer is typically faster but smaller than the next. The most common pattern
  is a **near-cache topology** with two levels:

    * **L1 (Level 1)**: Local cache - Fast, in-process, limited size. Examples:
      `Nebulex.Adapters.Local`.
    * **L2 (Level 2)**: Distributed/remote cache - Slower, shared across nodes,
      larger capacity. Examples: `Nebulex.Adapters.Partitioned`, Redis, etc.

  Multi-level caches provide **performance improvement** by reducing latency
  for frequently accessed data (served from L1) while maintaining large capacity
  through the L2 layer.

  ### Cache Lookup (Read Operations)

  When you perform a **read operation** (e.g., `get`, `fetch`), the adapter
  checks the cache levels in order (L1 → L2 → L3, etc.) and returns the value
  from the **first level that contains it**:

    1. Check L1 (fast, local cache).
    2. If found → Return immediately.
    3. If not found → Check L2.
    4. If found in L2 → Return and replicate to L1 (if inclusive mode).
    5. If not found → Check L3, L4, etc.
    6. If not found in any level → Return error or miss.

  This **single-hop lookup** pattern ensures you always get data from the
  fastest available level while maintaining a fallback to slower (but larger)
  levels.

  **Example:**
  - Request for key "user:123"
  - L1 miss (not in local cache)
  - L2 hit (found in distributed cache)
  - Value returned to client
  - In inclusive mode: value is replicated back to L1 for future requests

  ### Write-Through Policy

  The multi-level adapter uses a **Write-Through** policy for write operations.
  This means data is written to **all levels synchronously** before returning
  to the caller:

    1. Write to L1 (local cache).
    2. Write to L2 (distributed cache).
    3. Write to L3, L4, etc. (if any)
    4. Return to caller.

  This ensures:

    * **Consistency**: Data is immediately available in all levels.
    * **Safety**: Data survives L1 failures (protected in L2).
    * **Simplicity**: No complex cache invalidation logic needed.

  **Trade-off**: Write operations are **slower** because they must complete on
  all levels before returning. However, this is typically acceptable when:
    - Read operations are usually more frequent.
    - Distributed writes are less latency-sensitive than reads.
    - Consistency guarantees are worth the cost.

  **Failure Handling**: If a write fails at any level, the operation stops
  (fail-fast). Partially written data at earlier levels is **not rolled back**
  (atomic writes are not guaranteed across levels).

  ### Replication in Inclusive Mode

  When using the **`:inclusive` policy** (default), data can exist in multiple
  levels simultaneously. The adapter automatically **replicates data backward**
  (from slower to faster levels) during reads:

    * **Inclusive mode**: Same key can exist in L1, L2, and L3 simultaneously.
    * **Exclusive mode**: Same key can exist in only one level at a time.

  #### Inclusive Mode Workflow

    1. **Read (inclusive mode)**:
       - Check L1, L2, L3 in order.
       - If found in L2 (not in L1) → Replicate to L1.
       - Future requests hit L1 (faster).

    2. **Write (inclusive mode)**:
       - Write to L1, L2, L3 (all levels).
       - Future reads hit L1 (fastest).

    3. **Eviction (inclusive mode)**:
       - L1 evicts a key (e.g., due to size limit).
       - Key still exists in L2.
       - Next read finds it in L2 and replicates it back to L1.

  #### Exclusive Mode Workflow

    1. **Read (exclusive mode)**:
       - Find key in L2.
       - Return value (do NOT replicate to L1).

    2. **Write (exclusive mode)**:
       - Write to L1, L2, L3 (write-through still applies).
       - Data exists in all levels.

    3. **Eviction (exclusive mode)**:
       - L1 evicts a key.
       - L2 still has it.
       - Next read must go to L2 (no replication).

  #### Choosing the Right Mode

    * **Inclusive (default)**: Good for hot-spot data patterns.
      - Pro: Faster subsequent reads (hot data stays in L1).
      - Con: More memory usage (duplicated in multiple levels).
      - Con: `get_all` is slower (requires per-entry replication).

    * **Exclusive**: Good for large data sets or strict memory limits.
      - Pro: Less memory usage (data exists once).
      - Con: Slower reads after eviction (must fetch from L2).
      - Con: More complex consistency management.

  ### TTL (Time-To-Live) Handling

  TTL values are **per-level and independent**:

    * Each level has its own TTL for the same key.
    * When writing with TTL, the same TTL is applied to all levels.
    * Each level independently expires the entry.
    * In inclusive mode: If L1 expires but L2 still has it, next read
      replicates from L2 back to L1 with remaining TTL.

  #### Why TTL is Per-Level

  Each cache level uses a **different adapter with its own eviction policy**.
  For example:

    * **L1 (`Nebulex.Adapters.Local`)**: TTL is evaluated **on-demand** during
      reads and via a **garbage collector** that periodically removes expired
      entries and creates new generations. Behavior is controlled by
      `gc_interval` option.

    * **L2 (`Nebulex.Adapters.Partitioned` or Redis)**: TTL is handled by the
      underlying distributed cache. Redis, for example, uses its own TTL
      expiration mechanism which may differ from the local adapter's approach.

  Because each adapter manages TTL differently, the **expiration timing and
  behavior can vary significantly across levels**:

    - L1 may expire entries within seconds (controlled by GC interval).
    - L2 may expire entries at slightly different times (depends on Redis TTL).
    - A key can exist in L2 but be expired in L1.
    - The remaining TTL is automatically synchronized when data is replicated
      during reads.

  This independence is **intentional and necessary** because:
    - Different adapters have different TTL mechanisms.
    - Forcing synchronized TTL across levels would require complex coordination.
    - Allowing independent TTL gives each adapter control over its eviction.
    - The cost is acceptable because reads automatically sync TTL through
      replication.

  This means:
    - Data can be available in L2 even after L1 expires it.
    - Evictions are independent per level and controlled by each adapter.
    - TTL synchronization happens automatically during reads
      (in inclusive mode).

  We can define a multi-level cache as follows:

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

  Where the configuration for the cache and its levels must be in your
  application environment, usually defined in your `config/config.exs`:

      config :my_app, MyApp.Multilevel,
        inclusion_policy: :inclusive,
        levels: [
          {
            MyApp.Multilevel.L1,
            gc_interval: :timer.hours(12)
          },
          {
            MyApp.Multilevel.L2,
            primary: [
              gc_interval: :timer.hours(12)
            ]
          }
        ]

  If your application was generated with a supervisor (by passing `--sup`
  to `mix new`) you will have a `lib/my_app/application.ex` file containing
  the application start callback that defines and starts your supervisor.
  You just need to edit the `start/2` function to start the cache as a
  supervisor on your application's supervisor:

      def start(_type, _args) do
        children = [
          {MyApp.Multilevel, []},
          ...
        ]

  See `Nebulex.Cache` for more information.

  ## Options

  This adapter supports the following options and all of them can be given via
  the cache configuration:

  #{Nebulex.Adapters.Multilevel.Options.start_options_docs()}

  ## Shared options

  Almost all of the cache functions outlined in `Nebulex.Cache` module
  accept the following options:

  #{Nebulex.Adapters.Multilevel.Options.common_runtime_options_docs()}

  ### Queryable API options

  The following options apply to `get_all`, `count_all`, `delete_all`,
  and `stream` commands:

  #{Nebulex.Adapters.Multilevel.Options.queryable_options_docs()}

  ## Telemetry events

  The multi-level adapter emits Telemetry events for itself and for each
  configured cache level. By default, each level gets a unique telemetry
  prefix derived from its module name, making events naturally distinguishable.

  ### Default Behavior

  Each cache level is a separate module (e.g., `MyApp.Multilevel.L1`,
  `MyApp.Multilevel.L2`), so each gets a unique telemetry prefix by default:

      defmodule MyApp.Multilevel do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: Nebulex.Adapters.Multilevel

        defmodule L1 do
          use Nebulex.Cache,
            otp_app: :my_app,
            adapter: Nebulex.Adapters.Local
        end

        defmodule L2 do
          use Nebulex.Cache,
            otp_app: :my_app,
            adapter: Nebulex.Adapters.Partitioned
        end
      end

  With this setup, events are naturally separated by module:

    * Multilevel adapter: `[:my_app, :multilevel, :command, :start]`
    * L1 cache (Local): `[:my_app, :multilevel, :l1, :command, :start]`
    * L2 cache (Partitioned): `[:my_app, :multilevel, :l2, :command, :start]`
    * L2 primary storage: `[:my_app, :multilevel, :l2, :primary, :command, :start]`

  This default behavior is already good for distinguishing events based on
  which cache level emitted them.

  ### Custom Telemetry Prefixes (Optional)

  If you want to override the default prefix for a level, use the optional
  `:telemetry_prefix` option:

      config :my_app, MyApp.Multilevel,
        levels: [
          {MyApp.Multilevel.L1, telemetry_prefix: [:my_app, :cache, :l1]},
          {MyApp.Multilevel.L2, telemetry_prefix: [:my_app, :cache, :l2]}
        ]

  This is useful if you want:
    * A different naming convention for your telemetry events.
    * To aggregate events from multiple caches under a common prefix.
    * To match an existing telemetry structure in your application.

  ### Events for Distributed L2 Adapters

  If L2 uses a distributed adapter like `Nebulex.Adapters.Partitioned`, you
  also get events from its primary storage:

      MyApp.Multilevel.get(key)
      # Emits (with default prefixes):
      #   [:my_app, :multilevel, :command, :start]        # Multilevel wrapper
      #   [:my_app, :multilevel, :l1, :command, :start]   # L1
      #   [:my_app, :multilevel, :l2, :command, :start]   # L2 wrapper
      #   [:my_app, :multilevel, :l2, :primary, :command, :start]  # L2 primary

  Refer to the [Telemetry guide](http://hexdocs.pm/nebulex/telemetry.html)
  for complete information on Nebulex Telemetry events and how to attach
  handlers.

  ## Info API

  As explained above, the multi-level adapter uses the configured cache levels.
  Therefore, the information provided by the `info` command will depend on the
  adapters configured for each level. The Nebulex built-in adapters support the
  recommended keys `:server`, `:memory`, and `:stats`. Additionally, the
  multi-level adapter supports:

    * `:levels_info` - A list with the info map for each cache level.

  For example, the info for `MyApp.Multilevel` may look like this:

      iex> MyApp.Multilevel.info!()
      %{
        memory: %{total: nil, used: 206760},
        server: %{
          cache_module: MyApp.Multilevel,
          cache_name: :multilevel_inclusive,
          cache_adapter: Nebulex.Adapters.Multilevel,
          cache_pid: #PID<0.998.0>,
          nbx_version: "3.0.0"
        },
        stats: %{
          hits: 0,
          misses: 0,
          writes: 0,
          evictions: 0,
          expirations: 0,
          deletions: 0,
          updates: 0
        },
        levels_info: [
          %{
            memory: %{total: nil, used: 68920},
            server: %{
              cache_module: MyApp.Multilevel.L1,
              cache_name: MyApp.Multilevel.L1,
              cache_adapter: Nebulex.Adapters.Local,
              cache_pid: #PID<0.1000.0>,
              nbx_version: "3.0.0"
            },
            stats: %{
              hits: 0,
              misses: 0,
              writes: 0,
              evictions: 0,
              expirations: 0,
              deletions: 0,
              updates: 0
            }
          },
          %{
            memory: %{total: nil, used: 68920},
            nodes: [:"node1@127.0.0.1"],
            server: %{
              cache_module: MyApp.Multilevel.L2,
              cache_name: MyApp.Multilevel.L2,
              cache_adapter: Nebulex.Adapters.Partitioned,
              cache_pid: #PID<0.1015.0>,
              nbx_version: "3.0.0"
            },
            stats: %{
              hits: 0,
              misses: 0,
              writes: 0,
              evictions: 0,
              expirations: 0,
              deletions: 0,
              updates: 0
            },
            nodes_info: %{
              "node1@127.0.0.1": %{
                memory: %{total: nil, used: 68920},
                server: %{
                  cache_module: MyApp.Multilevel.L2.Primary,
                  cache_name: MyApp.Multilevel.L2.Primary,
                  cache_adapter: Nebulex.Adapters.Local,
                  cache_pid: #PID<0.1017.0>,
                  nbx_version: "3.0.0"
                },
                stats: %{
                  hits: 0,
                  misses: 0,
                  writes: 0,
                  evictions: 0,
                  expirations: 0,
                  deletions: 0,
                  updates: 0
                }
              }
            }
          }
        ]
      }

  ## Extended API

  This adapter provides some additional convenience functions to the
  `Nebulex.Cache` API.

  ### `inclusion_policy/0,1`

  Returns the inclusion policy of the cache.

      iex> MyCache.inclusion_policy()
      :inclusive

  ## Near cache topology example

  The multi-level adapter can be used to implement a **near-cache topology**
  with different types of cache backends. The most common pattern is L1 (local,
  fast) + L2 (distributed, larger capacity).

  ### L1 (Local) + L2 (Redis or External Cache)

  Instead of using `Nebulex.Adapters.Partitioned` for L2, you can use any
  external cache system via its adapter. For example, with Redis:

      defmodule MyApp.NearCache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: Nebulex.Adapters.Multilevel

        defmodule L1 do
          use Nebulex.Cache,
            otp_app: :my_app,
            adapter: Nebulex.Adapters.Local
        end

        defmodule L2 do
          use Nebulex.Cache,
            otp_app: :my_app,
            adapter: Nebulex.Adapters.Redis
        end
      end

  Configuration:

      config :my_app, MyApp.NearCache,
        levels: [
          {MyApp.NearCache.L1, gc_interval: :timer.hours(1), max_size: 10_000},
          {MyApp.NearCache.L2, conn_opts: [host: "localhost", port: 6379]}
        ]

  This topology provides:

    * **L1 (Local)**: In-process cache with fast micro-second latency.
    * **L2 (Redis)**: Shared across nodes, larger capacity, multi-millisecond
      latency.

  **Benefits:**
    - Hot data is served from L1 (very fast).
    - Cold data fetches from Redis L2, then cached in L1 on next access.
    - Data survives node restarts (in Redis).
    - Works across multiple nodes with a shared Redis instance.

  **Use case**: Web applications where you want blazing-fast L1 performance for
  frequently accessed data while using Redis for distributed, shared storage.

  ## Transactions

  The multilevel adapter supports distributed transactions via
  `Nebulex.Distributed.Transaction`, which uses Erlang's `:global` module for
  cluster-wide lock coordination.

  **Automatic Node Detection**: When using transactions with multilevel caches,
  the adapter automatically detects distributed levels (e.g., partitioned or
  replicated caches) and ensures locks are acquired across all relevant cluster
  nodes. This means you don't need to manually specify nodes when using
  distributed levels.

  **Example**:

      # Multilevel cache with local L1 and partitioned L2
      MyApp.Multilevel.transaction(fn ->
        # Locks are automatically set across all partitioned cache nodes
        counter = MyApp.Multilevel.get!(:counter, default: 0)
        MyApp.Multilevel.put!(:counter, counter + 1)
      end, keys: [:counter])

  **Recommendation**: Always specify the `:keys` option to enable fine-grained
  locking and maximize concurrency. Without keys, a global lock is used which
  serializes all transactions.

  See `Nebulex.Distributed.Transaction` for more information about transaction
  options, behavior, and performance considerations.

  ## CAVEATS

  Because this adapter reuses other existing/configured adapters, it inherits
  all their limitations too. Therefore, it is highly recommended to check the
  documentation of the adapters to use.
  """

  # Provide Cache Implementation
  @behaviour Nebulex.Adapter
  @behaviour Nebulex.Adapter.KV
  @behaviour Nebulex.Adapter.Queryable
  @behaviour Nebulex.Adapter.Info

  # Inherit distributed transaction implementation using `:global`
  use Nebulex.Distributed.Transaction

  # Inherit default observable implementation
  use Nebulex.Adapter.Observable

  import Nebulex.Adapter
  import Nebulex.Utils

  alias __MODULE__.Options
  alias Nebulex.Adapters.Common.Info, as: I
  alias Nebulex.Distributed.Cluster
  alias Nebulex.Distributed.Helpers, as: H

  ## Nebulex.Adapter

  @impl true
  defmacro __before_compile__(_env) do
    quote do
      @doc """
      A convenience function to get the cache inclusion policy.
      """
      def inclusion_policy(name \\ __MODULE__) do
        name
        |> lookup_meta()
        |> Map.fetch!(:inclusion_policy)
      end
    end
  end

  @impl true
  def init(opts) do
    # Common options
    {telemetry_prefix, opts} = Keyword.pop!(opts, :telemetry_prefix)
    {telemetry, opts} = Keyword.pop!(opts, :telemetry)
    {cache, opts} = Keyword.pop!(opts, :cache)

    # Validate options
    opts = Options.validate_start_opts!(opts)

    # Get adapter options
    name = opts[:name] || cache
    levels = Keyword.fetch!(opts, :levels)
    inclusion_policy = Keyword.fetch!(opts, :inclusion_policy)

    # Build multi-level specs
    {children, meta_list} = children(levels, telemetry_prefix, telemetry)

    # Build adapter spec
    child_spec =
      Supervisor.child_spec(
        {Nebulex.Adapters.Multilevel.Supervisor,
         {camelize_and_concat([name, Supervisor]), children}},
        id: {__MODULE__, name}
      )

    adapter_meta = %{
      telemetry_prefix: telemetry_prefix,
      telemetry: telemetry,
      name: name,
      levels: meta_list,
      inclusion_policy: inclusion_policy,
      started_at: DateTime.utc_now()
    }

    {:ok, child_spec, adapter_meta}
  end

  defp children(levels, telemetry_prefix, telemetry) do
    levels
    |> Enum.reverse()
    |> Enum.reduce({[], []}, fn {l_cache, l_opts}, {child_acc, meta_acc} ->
      l_opts = Keyword.merge([telemetry_prefix: telemetry_prefix, telemetry: telemetry], l_opts)

      meta = %{cache: l_cache, name: l_opts[:name]}

      {[{l_cache, l_opts} | child_acc], [meta | meta_acc]}
    end)
  end

  ## Nebulex.Adapter.KV

  @impl true
  def fetch(%{levels: levels, inclusion_policy: policy}, key, opts) do
    {ml_opts, opts} = validate_ml_opts!(opts)

    levels
    |> levels(ml_opts)
    |> Enum.reduce_while({nil, []}, fn level, {_, prev} ->
      case with_dynamic_cache(level, :fetch, [key, opts]) do
        {:error, %Nebulex.KeyError{key: ^key}} = error ->
          {:cont, {error, [level | prev]}}

        other ->
          {:halt, {other, [level | prev]}}
      end
    end)
    |> maybe_replicate(key, policy)
  end

  @impl true
  def put(%{levels: levels}, key, value, on_write, ttl, keep_ttl?, opts) do
    opts = Keyword.merge(opts, ttl: ttl, keep_ttl: keep_ttl?)

    case on_write do
      :put ->
        while_error(levels, :put, [key, value], {:ok, true}, opts)

      :put_new ->
        while_error(levels, :put_new, [key, value], {:ok, true}, opts)

      :replace ->
        while_error(levels, :replace, [key, value], {:ok, true}, opts)
    end
  end

  @impl true
  def put_all(%{levels: levels}, entries, on_write, ttl, opts) do
    {ml_opts, opts} = validate_ml_opts!(opts)

    opts = Keyword.put(opts, :ttl, ttl)
    action = if on_write == :put_new, do: :put_new_all, else: :put_all

    reducer = fn level, {_, level_acc} ->
      case with_dynamic_cache(level, action, [entries, opts]) do
        :ok ->
          {:cont, {{:ok, true}, [level | level_acc]}}

        {:ok, true} ->
          {:cont, {{:ok, true}, [level | level_acc]}}

        other ->
          _ = delete_from_levels(level_acc, entries)

          {:halt, {other, level_acc}}
      end
    end

    levels
    |> levels(ml_opts)
    |> Enum.reduce_while({{:ok, true}, []}, reducer)
    |> elem(0)
  end

  @impl true
  def delete(%{levels: levels}, key, opts) do
    while_error(levels, :delete, [key], :ok, Keyword.put(opts, :reverse, true))
  end

  @impl true
  def take(%{levels: levels}, key, opts) do
    init = wrap_error Nebulex.KeyError, key: key, reason: :not_found

    levels
    |> levels(opts)
    |> do_take(init, key, opts)
  end

  defp do_take([], result, _key, _opts) do
    result
  end

  defp do_take([l_meta | rest], {:error, %Nebulex.KeyError{}}, key, opts) do
    result = with_dynamic_cache(l_meta, :take, [key, opts])

    do_take(rest, result, key, opts)
  end

  defp do_take(levels, result, key, _opts) do
    _ = while_error(levels, :delete, [key], :ok, reverse: true)

    result
  end

  @impl true
  def has_key?(%{levels: levels}, key, opts) do
    while_ok(levels, :has_key?, [key], {:ok, false}, opts)
  end

  @impl true
  def ttl(%{levels: levels}, key, opts) do
    init = wrap_error Nebulex.KeyError, key: key, reason: :not_found

    while_ok(levels, :ttl, [key], init, opts)
  end

  @impl true
  def expire(%{levels: levels}, key, ttl, opts) do
    with_bool(levels, :expire, [key, ttl], {:ok, false}, opts)
  end

  @impl true
  def touch(%{levels: levels}, key, opts) do
    with_bool(levels, :touch, [key], {:ok, false}, opts)
  end

  @impl true
  def update_counter(%{levels: levels}, key, amount, default, ttl, opts) do
    while_error(levels, :incr, [key, amount], nil, [ttl: ttl, default: default] ++ opts)
  end

  ## Nebulex.Adapter.Queryable

  @impl true
  def execute(adapter_meta, query_spec, opts) do
    do_execute(
      adapter_meta,
      query_spec,
      validate_ml_opts!(opts, &Options.validate_queryable_opts!/1, [:replicate, :on_error])
    )
  end

  defp do_execute(_adapter_meta, %{op: :get_all, query: {:in, []}}, _opts) do
    {:ok, []}
  end

  defp do_execute(_adapter_meta, %{op: op, query: {:in, []}}, _opts)
       when op in [:count_all, :delete_all] do
    {:ok, 0}
  end

  defp do_execute(
         %{inclusion_policy: :inclusive} = adapter_meta,
         %{op: :get_all, query: {:in, keys}, select: select} = query,
         {ml_opts, opts}
       ) do
    replicate? = Keyword.fetch!(ml_opts, :replicate)
    level = Keyword.get(ml_opts, :level)

    if replicate? do
      fetch_keys(adapter_meta, keys, select, [level: level] ++ opts)
    else
      do_execute(%{adapter_meta | inclusion_policy: :exclusive}, query, {ml_opts, opts})
    end
  end

  defp do_execute(
         %{levels: levels},
         %{op: :get_all, query: {:in, keys}, select: select} = query,
         {ml_opts, opts}
       ) do
    reducer = query_reducer(:in)

    levels
    |> levels(ml_opts)
    |> Enum.reduce_while({{:ok, %{}}, keys}, fn level, {_, k_acc} = acc ->
      query = build_query(%{query | query: {:in, k_acc}, select: {:key, :value}})

      level
      |> with_dynamic_cache(:get_all, [query, opts])
      |> reducer.(acc)
    end)
    |> elem(0)
    |> select(select)
  end

  defp do_execute(%{levels: levels}, %{op: op, select: select} = query, {ml_opts, opts}) do
    ml_opts = if op == :delete_all, do: Keyword.put(ml_opts, :reverse, true), else: ml_opts
    acc_in = if op == :get_all, do: %{}, else: 0

    query = build_query(%{query | select: {:key, :value}})
    reducer = query_reducer(:q)

    levels
    |> levels(ml_opts)
    |> Enum.reduce_while({:ok, acc_in}, fn level, acc ->
      level
      |> with_dynamic_cache(op, [query, opts])
      |> reducer.(acc)
    end)
    |> select(select)
  end

  @impl true
  def stream(adapter_meta, query, opts) do
    {ml_opts, opts} =
      validate_ml_opts!(opts, &Options.validate_queryable_opts!/1, [:replicate, :on_error])

    # The multi-level adapter is a wrapper adapter, it doesn't implement any
    # cache storage, it depends on other cache adapters to do so. There are no
    # entries to stream from the multi-level adapter itself. Therefore, this
    # is a workaround to create a stream to trigger the multi-level evaluation
    # lazily.
    stream = fn _, _ ->
      on_error = Keyword.fetch!(ml_opts, :on_error)

      case do_execute(adapter_meta, %{query | op: :get_all}, {ml_opts, opts}) do
        {:ok, results} ->
          {:halt, [results]}

        {:error, _} when on_error == :nothing ->
          {:halt, []}

        {:error, reason} when on_error == :raise ->
          stacktrace =
            Process.info(self(), :current_stacktrace)
            |> elem(1)
            |> tl()

          reraise reason, stacktrace
      end
    end

    {:ok, Stream.flat_map(stream, & &1)}
  end

  ## Nebulex.Adapter.Transaction

  @impl true
  def transaction(%{levels: levels} = adapter_meta, fun, opts) do
    {ml_opts, opts} = validate_ml_opts!(opts)

    # Perhaps one of the levels is a distributed adapter,
    # then ensure the lock is set on the cluster nodes.
    nodes =
      levels
      |> levels(ml_opts)
      |> Enum.reduce([node()], fn %{name: name, cache: cache}, acc ->
        if cache.__adapter__() in [Nebulex.Adapters.Partitioned, Nebulex.Adapters.Replicated] do
          Cluster.pg_nodes(name || cache) ++ acc
        else
          acc
        end
      end)
      |> Enum.uniq()

    super(adapter_meta, fun, Keyword.put(opts, :nodes, nodes))
  end

  @impl true
  def in_transaction?(adapter_meta, opts) do
    super(adapter_meta, opts)
  end

  ## Nebulex.Adapter.Info

  @impl true
  def info(adapter_meta, spec, opts) do
    {ml_opts, opts} = validate_ml_opts!(opts)

    info(adapter_meta, spec, opts, ml_opts)
  end

  defp info(adapter_meta, :all, opts, ml_opts) do
    with {:ok, levels_info} <- fetch_levels_info(adapter_meta, :all, opts, ml_opts) do
      levels_info
      |> info_agg()
      |> Map.take([:server, :memory, :stats, :levels_info])
      |> Map.merge(%{server: I.info(adapter_meta, :server), levels_info: levels_info})
      |> wrap_ok()
    end
  end

  defp info(adapter_meta, :server, _opts, _ml_opts) do
    {:ok, I.info(adapter_meta, :server)}
  end

  defp info(adapter_meta, :levels_info, opts, ml_opts) do
    fetch_levels_info(adapter_meta, :all, opts, ml_opts)
  end

  defp info(_adapter_meta, [], _opts, _ml_opts) do
    {:ok, %{}}
  end

  defp info(adapter_meta, spec, opts, ml_opts) when is_list(spec) do
    server =
      if Enum.member?(spec, :server) do
        %{server: I.info(adapter_meta, :server)}
      else
        %{}
      end

    with {:ok, levels_info} <-
           fetch_levels_info(
             adapter_meta,
             Enum.filter(spec, &(&1 != :levels_info)),
             opts,
             ml_opts
           ) do
      info =
        if Enum.member?(spec, :levels_info) do
          %{levels_info: levels_info}
        else
          %{}
        end

      levels_info
      |> info_agg()
      |> Map.merge(info)
      |> Map.merge(server)
      |> wrap_ok()
    end
  end

  defp info(adapter_meta, spec, opts, ml_opts) do
    with {:ok, levels_info} <- fetch_levels_info(adapter_meta, spec, opts, ml_opts) do
      {:ok, info_agg(levels_info)}
    end
  end

  defp fetch_levels_info(%{levels: levels}, spec, opts, ml_opts) do
    levels
    |> levels(ml_opts)
    |> Enum.reverse()
    |> Enum.reduce_while({:ok, []}, fn level_meta, {:ok, acc} ->
      case with_dynamic_cache(level_meta, :info, [spec, opts]) do
        {:ok, info} ->
          {:cont, {:ok, [info | acc]}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  defp info_agg(info) do
    Enum.reduce(info, %{}, &H.merge_info_maps(&2, Map.delete(&1, :server)))
  end

  ## Private functions

  defp with_dynamic_cache(%{cache: cache, name: nil}, action, args) do
    apply(cache, action, args)
  end

  defp with_dynamic_cache(%{cache: cache, name: name}, action, args) do
    cache.with_dynamic_cache(name, fn ->
      apply(cache, action, args)
    end)
  end

  defp validate_ml_opts!(opts, fun \\ &Options.validate_common_runtime_opts!/1, keys \\ []) do
    opts
    |> fun.()
    |> Keyword.split([:level, :reverse | keys])
  end

  defp levels(levels, opts) do
    levels =
      if level = Keyword.get(opts, :level) do
        [Enum.at(levels, level - 1)]
      else
        levels
      end

    if Keyword.get(opts, :reverse) do
      Enum.reverse(levels)
    else
      levels
    end
  end

  defp while_error(levels, fun, args, acc, opts) do
    {ml_opts, opts} = validate_ml_opts!(opts)

    levels
    |> levels(ml_opts)
    |> do_while_error(fun, args ++ [opts], acc)
  end

  defp do_while_error([], _fun, _args, acc) do
    acc
  end

  defp do_while_error([l | levels], fun, args, acc) do
    case {with_dynamic_cache(l, fun, args), acc} do
      {:ok, value} ->
        do_while_error(levels, fun, args, value)

      {{:ok, bool}, {:ok, acc_bool}} when is_boolean(bool) ->
        do_while_error(levels, fun, args, {:ok, bool and acc_bool})

      {{:ok, value}, nil} ->
        do_while_error(levels, fun, args, {:ok, value})

      {{:ok, _}, {:ok, _} = acc} ->
        do_while_error(levels, fun, args, acc)

      {{:error, _} = error, _acc} ->
        error
    end
  end

  defp while_ok(levels, fun, args, init, opts) do
    {ml_opts, opts} = validate_ml_opts!(opts)
    args = args ++ [opts]

    levels
    |> levels(ml_opts)
    |> Enum.reduce_while(init, fn level_meta, acc ->
      case with_dynamic_cache(level_meta, fun, args) do
        {:error, %Nebulex.KeyError{}} ->
          {:cont, acc}

        {:ok, false} ->
          {:cont, acc}

        return ->
          {:halt, return}
      end
    end)
  end

  defp with_bool(levels, fun, args, acc, opts) do
    {ml_opts, opts} = validate_ml_opts!(opts)

    levels
    |> levels(ml_opts)
    |> with_bool(fun, args ++ [opts], acc)
  end

  defp with_bool([], _fun, _args, acc) do
    acc
  end

  defp with_bool([l | levels], fun, args, {:ok, acc}) do
    with {:ok, value} <- with_dynamic_cache(l, fun, args) do
      with_bool(levels, fun, args, {:ok, value or acc})
    end
  end

  defp delete_from_levels(levels, entries) do
    for level_meta <- levels, {key, _} <- entries do
      with_dynamic_cache(level_meta, :delete, [key, []])
    end
  end

  defp maybe_replicate({{:ok, value}, [level_meta | [_ | _] = levels]}, key, :inclusive) do
    case with_dynamic_cache(level_meta, :ttl, [key]) do
      {:ok, ttl} ->
        :ok =
          Enum.each(levels, fn l ->
            with {:error, _} = error <- with_dynamic_cache(l, :put, [key, value, [ttl: ttl]]) do
              throw({:return, error})
            end
          end)

        {:ok, value}

      {:error, %Nebulex.KeyError{key: ^key}} ->
        # The cache entry expired between the `fetch` and `ttl` calls,
        # don't replicate the entry
        {:ok, value}

      {:error, _} = error ->
        error
    end
  catch
    {:return, result} -> result
  end

  defp maybe_replicate({value, _levels}, _key, _inclusion_policy) do
    value
  end

  defp build_query(%{select: select, query: query}) do
    query = with {:q, q} <- query, do: {:query, q}

    [query, select: select]
  end

  defp select({:ok, map}, select) when is_map(map) do
    case select do
      :key -> Map.keys(map)
      :value -> Map.values(map)
      _else -> Map.to_list(map)
    end
    |> wrap_ok()
  end

  defp select(other, _select) do
    other
  end

  defp fetch_keys(adapter_meta, keys, select, opts) do
    Enum.reduce_while(keys, {:ok, []}, fn k, {:ok, acc} ->
      case {fetch(adapter_meta, k, opts), select} do
        {{:ok, _v}, :key} ->
          {:cont, {:ok, [k | acc]}}

        {{:ok, v}, :value} ->
          {:cont, {:ok, [v | acc]}}

        {{:ok, v}, _} ->
          {:cont, {:ok, [{k, v} | acc]}}

        {{:error, %Nebulex.KeyError{}}, _} ->
          {:cont, {:ok, acc}}

        {error, _} ->
          {:halt, error}
      end
    end)
  end

  defp query_reducer(:in) do
    fn
      {:ok, res}, {{:ok, acc}, keys_acc} ->
        # Ensure no duplicates
        {acc, res_keys} =
          Enum.reduce(res, {acc, []}, fn {k, v}, {acc, k_acc} ->
            {Map.put_new(acc, k, v), [k | k_acc]}
          end)

        case keys_acc -- res_keys do
          [] ->
            {:halt, {{:ok, acc}, []}}

          keys_acc ->
            {:cont, {{:ok, acc}, keys_acc}}
        end

      {:error, _} = error, _ ->
        {:halt, {error, []}}
    end
  end

  defp query_reducer(:q) do
    fn
      {:ok, res}, {:ok, acc} when is_list(res) ->
        # Ensure no duplicates
        acc = Enum.reduce(res, acc, &Map.put_new(&2, elem(&1, 0), elem(&1, 1)))

        {:cont, {:ok, acc}}

      {:ok, res}, {:ok, acc} when is_integer(res) ->
        {:cont, {:ok, acc + res}}

      {:error, _} = error, _ ->
        {:halt, error}
    end
  end
end
