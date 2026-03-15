defmodule Nebulex.Adapters.Replicated do
  @moduledoc """
  Adapter module for the replicated cache topology using lazy-pull replication.

  ## Features

    * Replicated cache topology with lazy-pull replication.
    * Zero-latency local reads for cached data.
    * Automatic cache invalidation via `Nebulex.Streams`.
    * On-demand data replication: cache misses pull from peer nodes
      transparently.
    * Trivial node joins: new nodes start with an empty cache and fill
      lazily as reads come in.
    * Configurable primary storage adapter.

  ## Replicated Cache Topology

  The replicated adapter provides a "local cache with distributed invalidation
  and lazy-pull replication" pattern. Each node maintains its own local cache.
  Writes trigger invalidation events across the cluster, and cache misses
  transparently pull data from peer nodes.

  Key characteristics:

    * _**Local Storage**_: Each node has a local cache. All read operations
      are served directly from the local cache when the data is available,
      with no network overhead.

    * _**Distributed Invalidation**_: When a cache entry is modified (inserted,
      updated, or deleted), an event is broadcast to all nodes in the cluster.
      Other nodes invalidate (delete) that entry from their local caches.

    * _**Lazy-Pull Replication**_: On a cache miss (after invalidation or on
      a new node), the adapter transparently pulls the data from a peer node
      that has it, caches it locally, and returns it. Over time, frequently
      accessed data naturally replicates across all nodes.

    * _**Eventual Consistency**_: After invalidation, the next read on other
      nodes triggers a pull from a peer, ensuring the latest value propagates
      across the cluster.

    * _**Write-Invalidate Protocol**_: Only invalidation events are broadcast
      on writes, not the actual values. Data is only transferred when needed
      (on cache misses), minimizing network overhead.

  ## How It Works

  ```
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

  ### Write flow

    1. Node A modifies a cache entry (e.g., `Cache.put("key", value)`).
    2. The local cache stores the value and emits a cache event.
    3. `Nebulex.Streams` broadcasts the event via Phoenix.PubSub.
    4. The `Nebulex.Streams.Invalidator` on Nodes B and C receives the event.
    5. The Invalidator deletes "key" from the local caches on B and C.

  ### Read flow

    1. Node B reads "key" from its local cache.
    2. If hit → return immediately (zero latency).
    3. If miss → pull from a peer node via RPC, cache locally, and return.
    4. If no peer has it → return cache miss (application handles via
       cache-aside pattern).

  ### Node join

    1. New node joins the `:pg` group, starts receiving invalidation events.
    2. Cache starts empty — data fills in lazily as reads come in.
    3. No bulk sync, no write blocking, no special join protocol.

  ## When to Use

  The replicated adapter is ideal for:

    * _**Read-Heavy Workloads**_: Maximum read performance since all reads
      are served locally after the first pull.
    * _**Clusters Where Most Nodes Read the Same Data**_: Lazy replication
      naturally distributes hot data to all nodes that need it.
    * _**Scenarios Where Node Joins Must Not Block Writes**_: New nodes
      start empty and fill on demand.
    * _**When Eventual Consistency Is Acceptable**_: It's a cache — stale
      data expires or gets invalidated.

  ## Comparison with Other Adapters

  | Aspect | Replicated | Coherent | Partitioned |
  |--------|-----------|----------|-------------|
  | Read (hit) | Local (zero latency) | Local (zero latency) | RPC to owner node |
  | Read (miss) | Pull from peer, then local | Miss → app fetches from SoR | RPC to owner node |
  | Write | Local + invalidation broadcast | Local + invalidation broadcast | RPC to owner node |
  | Node join | Empty, fills on demand | Empty, fills on demand | Full ring sync |
  | Data location | Replicated on demand | Independent per node | Sharded across nodes |
  | Consistency | Eventual | Eventual | Strong (single owner) |
  | Network overhead | Low (invalidation + pull on miss) | Low (only invalidations) | Medium (data transfer) |

  ## Primary Storage Adapter

  This adapter depends on a local cache adapter (primary storage), adding a
  distributed invalidation layer with lazy-pull replication on top of it. You
  don't need to manually define the primary storage cache; the adapter
  initializes it automatically as part of the supervision tree.

  The `:primary_storage_adapter` option (defaults to `Nebulex.Adapters.Local`)
  configures which adapter to use for the local storage. Options for the
  primary adapter can be specified via the `:primary` configuration option.

  ## Usage

  The cache expects the `:otp_app` and `:adapter` as options when used.
  The `:otp_app` should point to an OTP application with the cache
  configuration. Optionally, you can configure the desired primary storage
  adapter with the option `:primary_storage_adapter` (defaults to
  `Nebulex.Adapters.Local`). See the compile time options for more information:

  #{Nebulex.Adapters.Replicated.Options.compile_options_docs()}

  For example:

      defmodule MyApp.ReplicatedCache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: Nebulex.Adapters.Replicated
      end

  Providing a custom `:primary_storage_adapter`:

      defmodule MyApp.ReplicatedCache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: Nebulex.Adapters.Replicated,
          adapter_opts: [primary_storage_adapter: Nebulex.Adapters.Local]
      end

  Configuration in `config/config.exs`:

      config :my_app, MyApp.ReplicatedCache,
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
          {MyApp.ReplicatedCache, []},
          ...
        ]

        opts = [strategy: :one_for_one, name: MyApp.Supervisor]
        Supervisor.start_link(children, opts)
      end

  See `Nebulex.Cache` for more information.

  ## Configuration Options

  This adapter supports the following configuration options:

  #{Nebulex.Adapters.Replicated.Options.start_options_docs()}

  ## Shared runtime options

  When using the replicated adapter, all of the cache functions outlined in
  `Nebulex.Cache` accept the following options:

  #{Nebulex.Adapters.Replicated.Options.common_runtime_options_docs()}

  ## Telemetry Events

  Since the replicated adapter depends on the configured primary storage cache
  (which uses a local cache adapter), this one will also emit Telemetry events.
  Additionally, `Nebulex.Streams` and `Nebulex.Streams.Invalidator` emit their
  own telemetry events for monitoring the invalidation process.

  See the [Telemetry guide](https://hexdocs.pm/nebulex/telemetry.html) and
  `Nebulex.Streams` documentation for more information.

  ## Extended API

  This adapter provides some additional convenience functions to the
  `Nebulex.Cache` API.

  Retrieving the primary storage or local cache module:

      MyCache.__primary__()

  Retrieving the cluster nodes associated with the given cache `name`:

      MyCache.nodes()

  Joining the cache to the cluster:

      MyCache.join_cluster()

  Leaving the cluster (removes the cache from the cluster):

      MyCache.leave_cluster()

  ## Caveats

    * _**Eventual Consistency Window**_: There is a latency between when a write
      occurs on one node and when the invalidation is processed on other nodes.
      During this window, other nodes may serve stale data. The duration depends
      on network latency and PubSub message delivery. For most use cases this is
      negligible, but time-sensitive applications should account for this.

    * _**First Read After Invalidation**_: The first read on a remote node after
      invalidation incurs a network hop to pull the data from a peer. Subsequent
      reads are local. This trade-off enables trivial node joins and eliminates
      write-time replication overhead.

    * _**Memory Usage**_: Over time, frequently accessed data replicates to all
      nodes that read it. Memory usage depends on each node's access patterns
      and the primary adapter's configuration (e.g., `:max_size`, `:gc_interval`).

    * _**Queryable Operations**_: Only `get_all(in: [k1, k2, ...])` triggers
      lazy-pull replication for missing keys. General queries
      (`get_all(query: nil)`, `count_all`, `delete_all`, `stream`) operate on
      the local cache only — merging arbitrary query results from all peers is
      not practical.

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
  alias Nebulex.Adapter
  alias Nebulex.Distributed.{Cluster, RPC}

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

      @doc """
      A convenience function for getting the cluster nodes.
      """
      def nodes(name \\ get_dynamic_cache()) do
        name
        |> get_pg_group()
        |> Cluster.pg_nodes()
      end

      @doc """
      A convenience function for joining the cache to the cluster.
      """
      def join_cluster(name \\ get_dynamic_cache()) do
        name
        |> get_pg_group()
        |> Cluster.join()
      end

      @doc """
      A convenience function for removing the cache from the cluster.
      """
      def leave_cluster(name \\ get_dynamic_cache()) do
        name
        |> get_pg_group()
        |> Cluster.leave()
      end

      @doc """
      A convenience function for getting the PG group name.
      """
      def get_pg_group(name) do
        name
        |> Adapter.lookup_meta()
        |> Map.fetch!(:pg_group)
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

    # PG group name for cluster membership
    pg_group = camelize_and_concat([name, PG])

    # Prepare metadata
    adapter_meta = %{
      telemetry_prefix: telemetry_prefix,
      telemetry: telemetry,
      name: name,
      primary_name: primary_opts[:name],
      pg_group: pg_group
    }

    # Prepare child spec
    child_spec =
      Supervisor.child_spec(
        {Nebulex.Adapters.Replicated.Supervisor, {cache, adapter_meta, primary_opts, stream_opts}},
        id: {__MODULE__, name}
      )

    {:ok, child_spec, adapter_meta}
  end

  ## Nebulex.Adapter.KV — Read callbacks with pull-from-peers

  @impl true
  def fetch(adapter_meta, key, opts) do
    with {:error, %Nebulex.KeyError{}} = e <- with_dynamic_cache(adapter_meta, :fetch, [key, opts]),
         {:ok, {value, _ttl}} <- pull_and_cache(adapter_meta, key, opts, e) do
      {:ok, value}
    end
  end

  @impl true
  def take(adapter_meta, key, opts) do
    with {:error, %Nebulex.KeyError{}} = e <- with_dynamic_cache(adapter_meta, :take, [key, opts]),
         {:ok, {value, _ttl}} <- rpc_first_peer(adapter_meta, key, opts, e) do
      {:ok, value}
    end
  end

  @impl true
  def has_key?(adapter_meta, key, opts) do
    with {:ok, false} <- with_dynamic_cache(adapter_meta, :has_key?, [key, opts]) do
      init_acc = wrap_error Nebulex.KeyError, key: key

      case pull_and_cache(adapter_meta, key, opts, init_acc) do
        {:ok, _} -> {:ok, true}
        {:error, %Nebulex.KeyError{}} -> {:ok, false}
      end
    end
  end

  @impl true
  def ttl(adapter_meta, key, opts) do
    with {:error, %Nebulex.KeyError{}} = e <- with_dynamic_cache(adapter_meta, :ttl, [key, opts]),
         {:ok, {_value, ttl}} <- pull_and_cache(adapter_meta, key, opts, e) do
      {:ok, ttl}
    end
  end

  ## Nebulex.Adapter.KV — Write callbacks (delegate to local primary + invalidation)

  @impl true
  def put(adapter_meta, key, value, on_write, ttl, keep_ttl?, opts) do
    primary_opts = Keyword.merge(opts, ttl: ttl, keep_ttl: keep_ttl?)

    do_put(on_write, adapter_meta, key, value, primary_opts)
  end

  defp do_put(:put, adapter_meta, key, value, primary_opts) do
    with :ok <- with_dynamic_cache(adapter_meta, :put, [key, value, primary_opts]) do
      {:ok, true}
    end
  end

  defp do_put(:put_new, adapter_meta, key, value, primary_opts) do
    with_dynamic_cache(adapter_meta, :put_new, [key, value, primary_opts])
  end

  defp do_put(:replace, adapter_meta, key, value, primary_opts) do
    with_dynamic_cache(adapter_meta, :replace, [key, value, primary_opts])
  end

  @impl true
  def put_all(adapter_meta, entries, on_write, ttl, opts) do
    primary_opts = Keyword.put(opts, :ttl, ttl)

    do_put_all(on_write, adapter_meta, entries, primary_opts)
  end

  defp do_put_all(:put, adapter_meta, entries, primary_opts) do
    with :ok <- with_dynamic_cache(adapter_meta, :put_all, [entries, primary_opts]) do
      {:ok, true}
    end
  end

  defp do_put_all(:put_new, adapter_meta, entries, primary_opts) do
    with_dynamic_cache(adapter_meta, :put_new_all, [entries, primary_opts])
  end

  @impl true
  def delete(adapter_meta, key, opts) do
    with_dynamic_cache(adapter_meta, :delete, [key, opts])
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
  def execute(adapter_meta, query, opts)

  def execute(adapter_meta, %{op: :get_all, query: {:in, keys}, select: select}, opts)
      when is_list(keys) do
    # Always query locally with {:key, :value} to know which keys are present
    kv_query = [in: keys, select: {:key, :value}]

    with {:ok, local_kv} <- with_dynamic_cache(adapter_meta, :get_all, [kv_query, opts]) do
      local_keys = Enum.map(local_kv, &elem(&1, 0))
      missing_keys = keys -- local_keys

      pulled_kv = pull_missing_keys(adapter_meta, missing_keys, opts)
      all_kv = local_kv ++ pulled_kv

      {:ok, select_from_kv(all_kv, select)}
    end
  end

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

  ## Helpers

  @doc """
  Helper function to use dynamic cache for internal primary cache storage
  when needed.
  """
  def with_dynamic_cache(adapter_meta, action, args)

  def with_dynamic_cache(%{cache: cache, primary_name: nil}, action, args) do
    apply(cache.__primary__(), action, args)
  end

  def with_dynamic_cache(%{cache: cache, primary_name: primary_name}, action, args) do
    cache.__primary__().with_dynamic_cache(primary_name, fn ->
      apply(cache.__primary__(), action, args)
    end)
  end

  @doc """
  Fetches the value and TTL for a key from the local primary cache.

  Called via RPC from peer nodes during lazy-pull replication. Returns
  `{:ok, {value, ttl}}` on hit or `{:error, %Nebulex.KeyError{}}` on miss.
  """
  def fetch_with_ttl(adapter_meta, key, opts) do
    with {:ok, value} <- with_dynamic_cache(adapter_meta, :fetch, [key, opts]),
         {:ok, ttl} <- with_dynamic_cache(adapter_meta, :ttl, [key, opts]) do
      {:ok, {value, ttl}}
    end
  end

  ## Private Functions

  defp build_query(%{select: select, query: query}) do
    query = with {:q, q} <- query, do: {:query, q}

    [query, select: select]
  end

  defp pull_and_cache(adapter_meta, key, opts, init_acc) do
    with {:ok, {value, ttl}} <- rpc_first_peer(adapter_meta, key, opts, init_acc) do
      # Cache locally by writing directly to the primary adapter
      # (not through the write path) to avoid triggering
      # an invalidation broadcast. Use the original TTL from the peer.
      _ = with_dynamic_cache(adapter_meta, :put, [key, value, [ttl: ttl] ++ opts])

      {:ok, {value, ttl}}
    end
  end

  defp rpc_first_peer(%{pg_group: pg_group} = adapter_meta, key, opts, init_acc) do
    # Validate common runtime options
    opts = Options.validate_common_runtime_opts!(opts)
    timeout = Keyword.fetch!(opts, :timeout)

    pg_group
    # Get the list of cache nodes
    |> Cluster.pg_nodes()
    # Delete the current node from the list
    |> List.delete(node())
    # Shuffle the list of nodes
    |> Enum.shuffle()
    # Call peer nodes until one returns a hit
    |> Enum.reduce_while(init_acc, fn peer, acc ->
      RPC.call(
        peer,
        __MODULE__,
        :fetch_with_ttl,
        [adapter_meta, key, opts],
        timeout
      )
      |> case do
        {:ok, _} = hit ->
          {:halt, hit}

        _error ->
          {:cont, acc}
      end
    end)
  end

  defp pull_missing_keys(adapter_meta, keys, opts) do
    Enum.reduce(keys, [], fn key, acc ->
      init_acc = wrap_error Nebulex.KeyError, key: key

      case pull_and_cache(adapter_meta, key, opts, init_acc) do
        {:ok, {value, _ttl}} -> [{key, value} | acc]
        _ -> acc
      end
    end)
  end

  defp select_from_kv(kv_list, {:key, :value}), do: kv_list
  defp select_from_kv(kv_list, :key), do: Enum.map(kv_list, &elem(&1, 0))
  defp select_from_kv(kv_list, :value), do: Enum.map(kv_list, &elem(&1, 1))
end
