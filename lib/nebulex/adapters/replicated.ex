defmodule Nebulex.Adapters.Replicated do
  @moduledoc """
  Adapter module for the replicated cache topology using push-based replication.

  ## Features

    * Replicated cache topology with eager push-based replication.
    * Zero-latency local reads — all data is replicated on every node.
    * Writes are applied locally and replicated to all peers via buffered RPC.
    * Double-buffered outbox and inbox for high-throughput batched replication.
    * "Newer version wins" conflict resolution via monotonic versioning.
    * Configurable primary storage adapter.

  ## Replicated Cache Topology

  The replicated adapter provides an "eager push replication" pattern. Each
  node maintains its own local cache. Writes are applied locally first, then
  batched and pushed to all peer nodes via RPC. On the receiving side, an
  inbox buffer applies remote commands to the local primary cache using
  "newer version wins" semantics.

  Key characteristics:

    * _**Local Storage**_: Each node has a local cache. All read operations
      are served directly from the local cache with no network overhead.

    * _**Push-Based Replication**_: When a cache entry is modified, the
      change is buffered in an outbox and periodically pushed to all peer
      nodes in a single batched RPC call.

    * _**Conflict Resolution**_: Uses monotonic versioning with "newer
      version wins" semantics. Concurrent writes to the same key are
      resolved deterministically.

    * _**Double-Buffered I/O**_: Both outbox (sending) and inbox (receiving)
      use double-buffered ETS tables for zero-downtime processing — writes
      continue while the previous batch is being processed.

  ## How It Works

  ```ascii
          Node A                      Node B                        Node C
    ┌───────────────┐           ┌───────────────┐             ┌───────────────┐
    │  Local Cache  │           │  Local Cache  │             │  Local Cache  │
    │   (primary)   │           │   (primary)   │             │   (primary)   │
    └──┬─────────┬──┘           └──┬─────────┬──┘             └──┬─────────┬──┘
       │         │                 │         │                   │         │
       ▼         ▼                 ▼         ▼                   ▼         ▼
  ┌────────┐ ┌────────┐       ┌────────┐ ┌────────┐        ┌────────┐ ┌────────┐
  │ Inbox  │ │ Outbox │       │ Inbox  │ │ Outbox │        │ Inbox  │ │ Outbox │
  └────────┘ └───┬────┘       └───▲────┘ └────────┘        └───▲────┘ └────────┘
                 │                │                            │
                 │ replicate ->   │                            │
                 └────────────────┘────────────────────────────┘
                  Batched RPC from Node A Outbox to peer Inboxes

  Example: put on Node A

    Client ── put("k", "v") ──▶ Node A Local Cache (write locally)
                                     │
                                     ├──▶ Inbox (tagged :local, skip on process)
                                     └──▶ Outbox (buffered)
                                               │
                                          flush cycle
                                               │
                              ┌────────────────┼────────────────┐
                              ▼                                 ▼
                         Node B Inbox                      Node C Inbox
                        (tagged :remote)                  (tagged :remote)
                              │                                 │
                         process cycle                     process cycle
                              │                                 │
                              ▼                                 ▼
                        Node B Cache                       Node C Cache
                       put("k", "v")                      put("k", "v")
  ```

  ### Write flow

    1. Node A modifies a cache entry (e.g., `Cache.put("key", value)`).
    2. The value is written to the local primary cache immediately.
    3. The command is written to the inbox (tagged `:local`, for conflict
       resolution) and to the outbox.
    4. On the next outbox flush cycle, all buffered commands are sent to
       peer inbox buffers via a single `RPC.multicall` with `put_all`.
    5. On each peer, the inbox applies remote commands to the local
       primary cache (skipping `:local` entries).

  ### Read flow

    1. Node B reads "key" from its local cache.
    2. If hit → return immediately (zero latency).
    3. If miss → return cache miss (data hasn't been replicated yet or
       was evicted locally).

  ### Node join (bootstrap)

    1. New node joins the `:pg` group.
    2. The `ClusterMonitor` discovers existing peers and bootstraps data
       from one of them by streaming all entries (with their TTLs) into
       the local inbox.
    3. If bootstrapping from a peer fails, the next peer is tried until
       one succeeds or all peers are exhausted.
    4. After bootstrap, new writes propagate automatically via the
       normal replication flow.

  ## When to Use

  The replicated adapter is ideal for:

    * _**Read-Heavy Workloads**_: Maximum read performance since all reads
      are served locally.
    * _**Small to Medium Datasets**_: Data that fits in memory on every
      node.
    * _**Low-Latency Write Propagation**_: Writes are batched and pushed
      eagerly, minimizing the consistency window.
    * _**When Eventual Consistency Is Acceptable**_: There is a small
      window between a write and its replication to peers.

  ## Primary Storage Adapter

  This adapter depends on a local cache adapter (primary storage), adding a
  push-based replication layer on top of it. You don't need to manually
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
        replication: [
          interval: :timer.seconds(1),
          batch_size: 1_000
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

  ## Telemetry events

  Since the replicated adapter depends on the configured primary storage
  cache (which uses a local cache adapter), this one will also emit Telemetry
  events. Therefore, there will be events emitted by the replicated adapter
  as well as the primary storage cache. For example, the cache defined before
  `MyApp.ReplicatedCache` will emit the following events:

    * `[:my_app, :replicated_cache, :command, :start]`
    * `[:my_app, :replicated_cache, :primary, :command, :start]`
    * `[:my_app, :replicated_cache, :command, :stop]`
    * `[:my_app, :replicated_cache, :primary, :command, :stop]`
    * `[:my_app, :replicated_cache, :command, :exception]`
    * `[:my_app, :replicated_cache, :primary, :command, :exception]`

  As you may notice, the telemetry prefix by default for the cache is
  `[:my_app, :replicated_cache]`. However, you could specify the
  `:telemetry_prefix` for the primary storage within the `:primary` options
  (if you want to override the default). See the
  [Telemetry guide](https://hexdocs.pm/nebulex/telemetry.html)
  for more information and examples.

  ## Adapter-specific telemetry events

  The replication process emits the following Telemetry span events when
  flushing buffered commands to peer nodes:

    * `telemetry_prefix ++ [:replication, :start]` - Dispatched when a
      replication batch starts being sent to peer nodes.

      * Measurements: `%{system_time: non_neg_integer}`
      * Metadata:

        ```
        %{
          adapter_meta: %{optional(atom) => term},
          node: atom,
          peers: [atom]
        }
        ```

    * `telemetry_prefix ++ [:replication, :stop]` - Dispatched when a
      replication batch completes (successfully or with errors).

      * Measurements: `%{duration: non_neg_integer}`
      * Metadata:

        ```
        %{
          adapter_meta: %{optional(atom) => term},
          node: atom,
          peers: [atom],
          errors: [{term, atom}]
        }
        ```

    * `telemetry_prefix ++ [:replication, :exception]` - Dispatched when a
      replication batch raises an exception.

      * Measurements: `%{duration: non_neg_integer}`
      * Metadata:

        ```
        %{
          adapter_meta: %{optional(atom) => term},
          node: atom,
          peers: [atom],
          kind: :error | :exit | :throw,
          reason: term(),
          stacktrace: [term()]
        }
        ```

  The `:errors` field in the `:stop` metadata is a list of `{error, node}`
  tuples for each peer node that failed to receive the replication batch.
  An empty list indicates all peers were updated successfully. When errors
  occur, the replicator retries failed nodes up to `:retries` times with
  a `:retry_delay` between attempts (see `:replication` options).

  ### Bootstrap events

  When a node joins the cluster and bootstraps data from an existing peer,
  the following Telemetry span events are emitted:

    * `telemetry_prefix ++ [:bootstrap, :start]` - Dispatched when
      bootstrap starts copying entries from a peer.

      * Measurements: `%{system_time: non_neg_integer}`
      * Metadata:

        ```
        %{
          adapter_meta: %{optional(atom) => term},
          node: atom,
          peer: atom
        }
        ```

    * `telemetry_prefix ++ [:bootstrap, :stop]` - Dispatched when
      bootstrap completes successfully.

      * Measurements: `%{duration: non_neg_integer}`
      * Metadata:

        ```
        %{
          adapter_meta: %{optional(atom) => term},
          node: atom,
          peer: atom,
          total: non_neg_integer
        }
        ```

    * `telemetry_prefix ++ [:bootstrap, :exception]` - Dispatched when
      bootstrap from a peer raises an exception (the next peer will be
      tried).

      * Measurements: `%{duration: non_neg_integer}`
      * Metadata:

        ```
        %{
          adapter_meta: %{optional(atom) => term},
          node: atom,
          peer: atom,
          kind: :error | :exit | :throw,
          reason: term(),
          stacktrace: [term()]
        }
        ```

  ## Caveats

    * _**Replication Latency**_: There is a window (up to the
      `:interval` replication option) between when a write occurs on
      one node and when it is replicated to peers. During this window,
      peers may serve stale data.

    * _**Memory Usage**_: Every node holds a full copy of the cache. This
      topology is best suited for datasets that fit in memory on all nodes.

    * _**Queryable Operations**_: General queries (`get_all`, `count_all`,
      `delete_all`, `stream`) operate on the local cache only.

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
  alias Nebulex.Distributed.Cluster

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

    # Replication options
    replication_opts = Keyword.fetch!(opts, :replication)

    # Buffer options
    buffer_opts =
      replication_opts
      |> Keyword.take([:partitions])
      |> Keyword.merge(
        processing_interval_ms: Keyword.fetch!(replication_opts, :interval),
        processing_batch_size: Keyword.fetch!(replication_opts, :batch_size)
      )

    # PG group name for cluster membership
    pg_group = camelize_and_concat([name, PG])

    # Buffer names
    inbox = camelize_and_concat([name, Inbox])
    outbox = camelize_and_concat([name, Outbox])

    # Prepare metadata
    adapter_meta = %{
      telemetry_prefix: telemetry_prefix,
      telemetry: telemetry,
      cache: cache,
      name: name,
      primary_name: primary_opts[:name],
      pg_group: pg_group,
      inbox: inbox,
      outbox: outbox,
      replication_timeout: Keyword.fetch!(replication_opts, :timeout),
      replication_retries: Keyword.fetch!(replication_opts, :retries),
      replication_retry_delay: Keyword.fetch!(replication_opts, :retry_delay)
    }

    # Prepare child spec
    child_spec =
      Supervisor.child_spec(
        {__MODULE__.Supervisor, {cache, adapter_meta, primary_opts, buffer_opts}},
        id: {__MODULE__, name}
      )

    {:ok, child_spec, adapter_meta}
  end

  ## Nebulex.Adapter.KV — Read callbacks (local only)

  @impl true
  def fetch(adapter_meta, key, opts) do
    with_dynamic_cache(adapter_meta, :fetch, [key, opts])
  end

  @impl true
  def has_key?(adapter_meta, key, opts) do
    with_dynamic_cache(adapter_meta, :has_key?, [key, opts])
  end

  @impl true
  def ttl(adapter_meta, key, opts) do
    with_dynamic_cache(adapter_meta, :ttl, [key, opts])
  end

  ## Nebulex.Adapter.KV — Write callbacks (local + replicate)

  @impl true
  def put(adapter_meta, key, value, on_write, ttl, keep_ttl?, opts) do
    primary_opts = Keyword.merge(opts, ttl: ttl, keep_ttl: keep_ttl?)

    with {:ok, true} = ok <- do_put(on_write, adapter_meta, key, value, primary_opts) do
      :ok = replicate(adapter_meta, key, {:put, [key, value, primary_opts]})

      ok
    end
  end

  @impl true
  def put_all(adapter_meta, entries, on_write, ttl, opts) do
    primary_opts = Keyword.put(opts, :ttl, ttl)

    with {:ok, true} = ok <- do_put_all(on_write, adapter_meta, entries, primary_opts) do
      Enum.each(entries, fn {key, value} ->
        :ok = replicate(adapter_meta, key, {:put, [key, value, primary_opts]})
      end)

      ok
    end
  end

  @impl true
  def delete(adapter_meta, key, opts) do
    with :ok <- with_dynamic_cache(adapter_meta, :delete, [key, opts]) do
      replicate(adapter_meta, key, {:delete, [key, opts]})
    end
  end

  @impl true
  def take(adapter_meta, key, opts) do
    with {:ok, _value} = ok <- with_dynamic_cache(adapter_meta, :take, [key, opts]) do
      :ok = replicate(adapter_meta, key, {:delete, [key, opts]})

      ok
    end
  end

  @impl true
  def expire(adapter_meta, key, ttl, opts) do
    with {:ok, true} = ok <- with_dynamic_cache(adapter_meta, :expire, [key, ttl, opts]) do
      :ok = replicate(adapter_meta, key, {:expire, [key, ttl, opts]})

      ok
    end
  end

  @impl true
  def touch(adapter_meta, key, opts) do
    with {:ok, true} = ok <- with_dynamic_cache(adapter_meta, :touch, [key, opts]) do
      :ok = replicate(adapter_meta, key, {:touch, [key, opts]})

      ok
    end
  end

  @impl true
  def update_counter(adapter_meta, key, amount, default, ttl, opts) do
    primary_opts = [ttl: ttl, default: default] ++ opts

    with {:ok, value} = ok <-
           with_dynamic_cache(adapter_meta, :incr, [key, amount, primary_opts]) do
      replicate_opts = Keyword.delete(primary_opts, :default)
      :ok = replicate(adapter_meta, key, {:put, [key, value, replicate_opts]})

      ok
    end
  end

  ## Nebulex.Adapter.Queryable

  @impl true
  def execute(adapter_meta, query, opts)

  def execute(adapter_meta, %{op: :delete_all, query: {:in, [_ | _] = keys}} = query, opts) do
    query = build_query(query)

    with {:ok, count} = ok when count > 0 <-
           with_dynamic_cache(adapter_meta, :delete_all, [query, opts]) do
      Enum.each(keys, fn key ->
        :ok = replicate(adapter_meta, key, {:delete, [key, opts]})
      end)

      ok
    end
  end

  def execute(adapter_meta, %{op: :delete_all} = query, opts) do
    query = build_query(query)

    with {:ok, count} = ok when count > 0 <-
           with_dynamic_cache(adapter_meta, :delete_all, [query, opts]) do
      :ok = replicate(adapter_meta, :all, {:delete_all, [query, opts]})

      ok
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

  ## Private functions

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

  defp do_put_all(:put, adapter_meta, entries, primary_opts) do
    with :ok <- with_dynamic_cache(adapter_meta, :put_all, [entries, primary_opts]) do
      {:ok, true}
    end
  end

  defp do_put_all(:put_new, adapter_meta, entries, primary_opts) do
    with_dynamic_cache(adapter_meta, :put_new_all, [entries, primary_opts])
  end

  defp build_query(%{select: select, query: query}) do
    query = with {:q, q} <- query, do: {:query, q}

    [query, select: select]
  end

  defp replicate(%{inbox: inbox, outbox: outbox}, key, command) do
    # Generate a version for the command
    version = System.monotonic_time()

    # Write to inbox tagged :local (for conflict resolution, won't re-apply)
    :ok = PartitionedBuffer.Map.put_newer(inbox, key, {command, :local}, version)

    # Write to outbox (no origin tag, will be tagged :remote on delivery)
    :ok = PartitionedBuffer.Map.put_newer(outbox, key, command, version)
  end
end
