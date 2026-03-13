defmodule Nebulex.Adapters.Coherent do
  @moduledoc """
  Adapter module for the coherent cache topology.

  ## Features

    * Local cache with distributed invalidation across cluster nodes.
    * Automatic cache invalidation via `Nebulex.Streams`.
    * Configurable primary storage adapter.
    * Maximum read performance (pure local lookups).

  ## Coherent Cache Topology

  The coherent adapter provides a "local cache with distributed invalidation"
  pattern. Each node maintains its own independent local cache, but writes
  trigger invalidation events across the cluster via `Nebulex.Streams`.

  Key characteristics:

    * _**Local Storage**_: Each node has a full local cache. All read operations
      are served directly from the local cache with no network overhead.

    * _**Distributed Invalidation**_: When a cache entry is modified (inserted,
      updated, or deleted), an event is broadcast to all nodes in the cluster.
      Other nodes invalidate (delete) that entry from their local caches.

    * _**Eventual Consistency**_: After invalidation, the next read on other
      nodes results in a cache miss, forcing a fresh fetch from the
      System-of-Record (SoR).

    * _**Write-Invalidate Protocol**_: Only invalidation events are broadcast,
      not the actual values. This minimizes network overhead.

  ## How It Works

  ```asciidoc
  Node A                          Node B                          Node C
  ┌──────────────┐               ┌──────────────┐               ┌──────────────┐
  │ Local Cache  │               │ Local Cache  │               │ Local Cache  │
  └──────┬───────┘               └──────┬───────┘               └──────┬───────┘
         │                              │                              │
         └──────────────┬───────────────┴──────────────┬───────────────┘
                        │                              │
                 ┌──────┴──────┐                ┌──────┴──────┐
                 │   Streams   │◄──────────────►│ Invalidator │
                 │  (PubSub)   │                │  (Workers)  │
                 └─────────────┘                └─────────────┘
  ```

  The process:

    1. Node A modifies a cache entry (e.g., `Cache.put("key", value)`).
    2. The local cache stores the value and emits a cache event.
    3. `Nebulex.Streams` broadcasts the event via Phoenix.PubSub.
    4. The `Nebulex.Streams.Invalidator` on Nodes B and C receives the event.
    5. The Invalidator deletes "key" from the local caches on B and C.
    6. Next read on B or C: cache miss → fetch fresh from SoR.

  ## When to Use

  The coherent adapter is ideal for:

    * _**Read-Heavy Workloads**_: Maximum read performance since all reads are
      local.
    * _**Configuration/Reference Data**_: Data that rarely changes but must be
      consistent when it does.
    * _**Session Caches**_: When each node primarily serves its own sessions
      but needs consistency for shared data.
    * _**Simple Distributed Caching**_: When you want the simplicity of local
      caching with basic distributed consistency.

  ## Comparison with Other Adapters

  | Aspect | Coherent | Partitioned | Multilevel |
  |--------|----------|-------------|------------|
  | Data Location | Independent per node | Sharded across nodes | L1 local + L2 shared |
  | Read Performance | Fastest (local) | Network hop required | L1 fast, L2 slower |
  | Write Behavior | Local + invalidation broadcast | Remote write to owner | Write through levels |
  | Consistency | Eventual (after invalidation) | Strong (single owner) | Varies by config |
  | Network Overhead | Low (only invalidations) | Medium (data transfer) | Medium to High |

  ## Primary Storage Adapter

  This adapter depends on a local cache adapter (primary storage), adding a
  distributed invalidation layer on top of it. You don't need to manually
  define the primary storage cache; the adapter initializes it automatically
  as part of the supervision tree.

  The `:primary_storage_adapter` option (defaults to `Nebulex.Adapters.Local`)
  configures which adapter to use for the local storage. Options for the
  primary adapter can be specified via the `:primary` configuration option.

  ## Usage

  The cache expects the `:otp_app` and `:adapter` as options when used.
  The `:otp_app` should point to an OTP application with the cache
  configuration. Optionally, you can configure the desired primary storage
  adapter with the option `:primary_storage_adapter` (defaults to
  `Nebulex.Adapters.Local`). See the compile time options for more information:

  #{Nebulex.Adapters.Coherent.Options.compile_options_docs()}

  For example:

      defmodule MyApp.CoherentCache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: Nebulex.Adapters.Coherent
      end

  Providing a custom `:primary_storage_adapter`:

      defmodule MyApp.CoherentCache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: Nebulex.Adapters.Coherent,
          adapter_opts: [primary_storage_adapter: Nebulex.Adapters.Local]
      end

  Configuration in `config/config.exs`:

      config :my_app, MyApp.CoherentCache,
        primary: [
          gc_interval: :timer.hours(12),
          max_size: 1_000_000
        ],
        stream_opts: [
          partitions: System.schedulers_online()
        ]

  Add the cache to your supervision tree:

      def start(_type, _args) do
        children = [
          {MyApp.CoherentCache, []},
          ...
        ]

        opts = [strategy: :one_for_one, name: MyApp.Supervisor]
        Supervisor.start_link(children, opts)
      end

  See `Nebulex.Cache` for more information.

  ## Configuration Options

  This adapter supports the following configuration options:

  #{Nebulex.Adapters.Coherent.Options.start_options_docs()}

  ## Telemetry Events

  Since the coherent adapter depends on the configured primary storage cache
  (which uses a local cache adapter), this one will also emit Telemetry events.
  Additionally, `Nebulex.Streams` and `Nebulex.Streams.Invalidator` emit their
  own telemetry events for monitoring the invalidation process.

  For example, the cache defined before `MyApp.CoherentCache` will emit:

    * `[:my_app, :coherent_cache, :command, :start]`
    * `[:my_app, :coherent_cache, :primary, :command, :start]`
    * `[:my_app, :coherent_cache, :command, :stop]`
    * `[:my_app, :coherent_cache, :primary, :command, :stop]`

  Additionally, stream and invalidator events:

    * `[:nebulex, :streams, :listener_registered]`
    * `[:nebulex, :streams, :broadcast]`
    * `[:nebulex, :streams, :invalidator, :started]`
    * `[:nebulex, :streams, :invalidator, :invalidate, :start]`
    * `[:nebulex, :streams, :invalidator, :invalidate, :stop]`

  See the [Telemetry guide](https://hexdocs.pm/nebulex/telemetry.html) and
  `Nebulex.Streams` documentation for more information.

  ## Extended API

  This adapter provides some additional convenience functions to the
  `Nebulex.Cache` API.

  Retrieving the primary storage or local cache module:

      MyCache.__primary__()

  ## Best Practices

  1. **Use for read-heavy workloads**: The coherent adapter excels when reads
     far outnumber writes.

  2. **Configure partitions for high write volumes**: If you have many
     concurrent writes, use `:partitions` in `:stream_opts` to parallelize
     invalidation processing.

  3. **Ensure PubSub connectivity**: The adapter relies on Phoenix.PubSub for
     event distribution. Ensure your cluster nodes can communicate via PubSub.

  4. **Handle cache misses gracefully**: After invalidation, reads result in
     cache misses. Ensure your application can fetch fresh data from the SoR.

  5. **Monitor invalidation latency**: Use telemetry events to monitor how
     quickly invalidations propagate across the cluster.

  ## Caveats

    * _**Eventual Consistency Window**_: There is a latency between when a write
      occurs on one node and when the invalidation is processed on other nodes.
      During this window, other nodes may serve stale data. The duration depends
      on network latency and PubSub message delivery. For most use cases this is
      negligible, but time-sensitive applications should account for this.

    * _**Memory Usage**_: Each node maintains its own independent local cache.
      This is not full data replication; nodes only cache data they have locally
      accessed or written. Memory usage depends on each node's access patterns
      and the primary adapter's configuration (e.g., `:max_size`, `:gc_interval`).

  """

  # Provide Cache Implementation
  @behaviour Nebulex.Adapter
  @behaviour Nebulex.Adapter.KV
  @behaviour Nebulex.Adapter.Queryable
  @behaviour Nebulex.Adapter.Transaction
  @behaviour Nebulex.Adapter.Info

  # Inherit default observable implementation
  use Nebulex.Adapter.Observable

  import Nebulex.Utils

  alias __MODULE__.Options

  ## Nebulex.Adapter

  @impl true
  defmacro __before_compile__(env) do
    otp_app = Module.get_attribute(env.module, :otp_app)
    opts = Module.get_attribute(env.module, :opts)
    adapter_opts = Keyword.fetch!(opts, :adapter_opts)

    adapter_opts = Options.validate_compile_opts!(adapter_opts)
    primary = Keyword.fetch!(adapter_opts, :primary_storage_adapter)

    quote do
      defmodule Primary do
        @moduledoc """
        This is the cache for the primary storage.
        """
        use Nebulex.Cache,
          otp_app: unquote(otp_app),
          adapter: unquote(primary)

        use Nebulex.Streams
      end

      @doc """
      A convenience function for getting the primary storage cache.
      """
      def __primary__, do: Primary
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

    # Get the cache name (required)
    name = opts[:name] || cache

    # Primary cache options
    primary_opts =
      Keyword.merge(
        [telemetry_prefix: telemetry_prefix ++ [:primary], telemetry: telemetry],
        Keyword.fetch!(opts, :primary)
      )

    # Maybe put a name to primary storage
    primary_opts =
      if opts[:name],
        do: [name: camelize_and_concat([name, Primary])] ++ primary_opts,
        else: primary_opts

    # Stream options
    stream_opts = Keyword.fetch!(opts, :stream_opts)

    # Prepare metadata
    adapter_meta = %{
      telemetry_prefix: telemetry_prefix,
      telemetry: telemetry,
      primary_name: primary_opts[:name]
    }

    # Prepare child spec
    child_spec =
      Supervisor.child_spec(
        {Nebulex.Adapters.Coherent.Supervisor, {cache, primary_opts, stream_opts}},
        id: {__MODULE__, name}
      )

    {:ok, child_spec, adapter_meta}
  end

  ## Nebulex.Adapter.KV

  @impl true
  def fetch(adapter_meta, key, opts) do
    with_dynamic_cache(adapter_meta, :fetch, [key, opts])
  end

  @impl true
  def put(adapter_meta, key, value, on_write, ttl, keep_ttl?, opts) do
    primary_opts = [ttl: ttl, keep_ttl: keep_ttl?] ++ opts

    case on_write do
      :put ->
        with :ok <- with_dynamic_cache(adapter_meta, :put, [key, value, primary_opts]) do
          {:ok, true}
        end

      :put_new ->
        with_dynamic_cache(adapter_meta, :put_new, [key, value, primary_opts])

      :replace ->
        with_dynamic_cache(adapter_meta, :replace, [key, value, primary_opts])
    end
  end

  @impl true
  def put_all(adapter_meta, entries, on_write, ttl, opts) do
    primary_opts = [ttl: ttl] ++ opts

    case on_write do
      :put ->
        with :ok <- with_dynamic_cache(adapter_meta, :put_all, [entries, primary_opts]) do
          {:ok, true}
        end

      :put_new ->
        with_dynamic_cache(adapter_meta, :put_new_all, [entries, primary_opts])
    end
  end

  @impl true
  def delete(adapter_meta, key, opts) do
    with_dynamic_cache(adapter_meta, :delete, [key, opts])
  end

  @impl true
  def take(adapter_meta, key, opts) do
    with_dynamic_cache(adapter_meta, :take, [key, opts])
  end

  @impl true
  def has_key?(adapter_meta, key, opts) do
    with_dynamic_cache(adapter_meta, :has_key?, [key, opts])
  end

  @impl true
  def ttl(adapter_meta, key, opts) do
    with_dynamic_cache(adapter_meta, :ttl, [key, opts])
  end

  @impl true
  def expire(adapter_meta, key, ttl, opts) do
    with_dynamic_cache(adapter_meta, :expire, [key, ttl, opts])
  end

  @impl true
  def touch(adapter_meta, key, opts) do
    with_dynamic_cache(adapter_meta, :touch, [key, opts])
  end

  @impl true
  def update_counter(adapter_meta, key, amount, default, ttl, opts) do
    with_dynamic_cache(adapter_meta, :incr, [key, amount, [ttl: ttl, default: default] ++ opts])
  end

  ## Nebulex.Adapter.Queryable

  @impl true
  def execute(adapter_meta, %{op: op} = query, opts) do
    query = build_query(query)

    with_dynamic_cache(adapter_meta, op, [query, opts])
  end

  @impl true
  def stream(adapter_meta, query, opts) do
    query = build_query(query)

    with_dynamic_cache(adapter_meta, :stream, [query, opts])
  end

  ## Nebulex.Adapter.Transaction

  @impl true
  def transaction(adapter_meta, fun, opts) do
    with_dynamic_cache(adapter_meta, :transaction, [fun, opts])
  end

  @impl true
  def in_transaction?(adapter_meta, opts) do
    with_dynamic_cache(adapter_meta, :in_transaction?, [opts])
  end

  ## Nebulex.Adapter.Info

  @impl true
  def info(adapter_meta, spec, opts) do
    with_dynamic_cache(adapter_meta, :info, [spec, opts])
  end

  ## Private Functions

  defp build_query(%{select: select, query: query}) do
    query = with {:q, q} <- query, do: {:query, q}

    [query, select: select]
  end

  defp with_dynamic_cache(%{cache: cache, primary_name: nil}, action, args) do
    apply(cache.__primary__(), action, args)
  end

  defp with_dynamic_cache(%{cache: cache, primary_name: primary_name}, action, args) do
    cache.__primary__().with_dynamic_cache(primary_name, fn ->
      apply(cache.__primary__(), action, args)
    end)
  end
end
