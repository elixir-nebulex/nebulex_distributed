defmodule Nebulex.Adapters.Partitioned do
  @moduledoc """
  Adapter module for the partitioned cache topology.

  ## Features

    * Partitioned cache topology (Sharding Distribution Model).
    * Consistent hashing via `ExHashRing` for distributing keys across cluster
      nodes.
    * Automatic cluster membership management using Erlang's `:pg`
      (process groups).
    * Support for transactions via Erlang global name registration facility.
    * Configurable primary storage adapter.

  ## Partitioned Cache Topology

  There are several key points to consider about a partitioned cache:

    * _**Partitioned**_: The data in a distributed cache is spread out over
      all the servers in such a way that no two servers are responsible for
      the same piece of cached data. This means that the size of the cache
      and the processing power associated with the management of the cache
      can grow linearly with the size of the cluster. Also, it means that
      operations against data in the cache can be accomplished with a
      "single hop," in other words, involving at most one other server.

    * _**Load-Balanced**_:  Since the data is spread out evenly over the
      servers, the responsibility for managing the data is automatically
      load-balanced across the cluster.

    * _**Ownership**_: Exactly one node in the cluster is responsible for each
      piece of data in the cache.

    * _**Point-To-Point**_: The communication for the partitioned cache is all
      point-to-point, enabling linear scalability.

    * _**Location Transparency**_: Although the data is spread out across
      cluster nodes, the exact same API is used to access the data, and the
      same behavior is provided by each of the API methods. This is called
      location transparency, which means that the developer does not have to
      code based on the topology of the cache, since the API and its behavior
      will be the same with a local cache, a replicated cache, or a distributed
      cache.

    * _**Failover**_: Failover of a distributed cache involves promoting backup
      data to be primary storage. When a cluster node fails, all remaining
      cluster nodes determine what data each holds in backup that the failed
      cluster node had primary responsible for when it died. Those data becomes
      the responsibility of whatever cluster node was the backup for the data.
      However, this adapter does not provide fault-tolerance implementation,
      each piece of data is kept in a single node/machine (via sharding), then,
      if a node fails, the data kept by this node won't be available for the
      rest of the cluster members.

  > Based on **"Distributed Caching Essential Lessons"** by **Cameron Purdy**
    and [Coherence Partitioned Cache Service][oracle-pcs].

  [oracle-pcs]: https://docs.oracle.com/cd/E13924_01/coh.340/e13819/partitionedcacheservice.htm

  ## Consistent Hashing and Key Distribution

  The adapter uses `ExHashRing` to implement consistent hashing, which maps keys
  to nodes in a way that minimizes data redistribution when the cluster topology
  changes.

  ### How key distribution works

  The process is as follows:

    1. **Virtual Nodes (Vnodes)**: Each physical node in the cluster is assigned
      a set of virtual nodes (vnodes) in the hash ring. This enables even
      distribution of keys across the cluster.

    2. **Key Hashing**: When a key is accessed, its hash value (computed using
      `erlang:phash2/1`) is used to find the corresponding vnode in the ring.

    3. **Node Lookup**: `ExHashRing.Ring` finds the node responsible for that
      vnode, which becomes the target for the operation.

    4. **RPC Routing**: The request is sent to the target node via RPC (remote
      procedure call) to read or write the cached value.

  ### Benefits of consistent hashing

    * **Minimal Key Redistribution**: When nodes join or leave, only a fraction
      of keys are redistributed to other nodes (proportional to the change in
      cluster size).
    * **Even Distribution**: Keys are evenly spread across all nodes in the
      cluster.
    * **Predictable Mapping**: The same key always maps to the same node,
      ensuring cache hits across the cluster.
    * **Efficient Lookup**: Hash ring lookups are O(log n) in terms of vnodes.

  ## Cluster Membership Management

  The adapter maintains a distributed view of the hash ring across all cluster
  nodes using two key components:

  ### Process Groups (`:pg`)

  The adapter uses Erlang's built-in `:pg` (process groups) module to track
  cluster membership. When a partitioned cache is started:

    1. The cache supervisor PID is registered in a `:pg` group named after the
      cache (e.g., the `:name` option or the cache module name).
    2. All nodes with the same cache running join the same group.
    3. When a node joins or leaves the cluster, `:pg` automatically notifies all
      members subscribed to that group.

  ### Ring Monitor

  The `Nebulex.Adapters.Partitioned.RingMonitor` is a `GenServer` that:

    1. **Subscribes to Cluster Changes**: Uses
      `Nebulex.Distributed.Cluster.monitor_scope/0` to subscribe to all
      `:pg` group changes via `:pg.monitor_scope/1`.

    2. **Handles Join/Leave Events**: When nodes join or leave a group,
      RingMonitor receives `{:join, group, pids}` and `{:leave, group, pids}`
      messages and updates the `ExHashRing.Ring` state accordingly.

    3. **Maintains Ring Consistency**: Keeps the hash ring in sync with the
      current cluster topology by adding/removing nodes from the ring.

  ### Handling Race Conditions During Startup

  During initial cluster formation, multiple nodes may start simultaneously,
  leading to race conditions where some nodes miss join events from others.
  To solve this, the RingMonitor uses a **periodic rejoin mechanism**:

    * **Rejoin Interval**: The `:rejoin_interval` option (default: 30 seconds)
      specifies an interval at which the RingMonitor periodically rejoins
      the `:pg` group.

    * **Idempotent Joins**: Since `:pg` treats duplicate joins as idempotent, a
      node can safely rejoin without negative side effects.

    * **Forced Ring Updates**: Each periodic rejoin triggers `:join` events that
      force all nodes to update their ring view, ensuring eventual consistency
      even if some initial join events were missed.

  This mechanism ensures that all nodes have a consistent view of the ring, even
  in the face of concurrent startups or transient network issues.

  ## Primary Storage Adapter

  This adapter depends on a local cache adapter (primary storage), adding a
  distributed layer on top of it. You don't need to manually define the primary
  storage cache; the adapter initializes it automatically as part of the
  supervision tree.

  The `:primary_storage_adapter` option (defaults to `Nebulex.Adapters.Local`)
  configures which adapter to use for the local storage on each node. Options
  for the primary adapter can be specified via the `:primary` configuration option.

  ## Usage

  The cache expects the `:otp_app` and `:adapter` as options when used.
  The `:otp_app` should point to an OTP application with the cache
  configuration. Optionally, you can configure the desired primary
  storage adapter with the option `:primary_storage_adapter`
  (defaults to `Nebulex.Adapters.Local`). See the compile time options
  for more information:

  #{Nebulex.Adapters.Partitioned.Options.compile_options_docs()}

  For example:

      defmodule MyApp.PartitionedCache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: Nebulex.Adapters.Partitioned
      end

  Providing the `:primary_storage_adapter`:

      defmodule MyApp.PartitionedCache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: Nebulex.Adapters.Partitioned,
          primary_storage_adapter: Nebulex.Adapters.Local
      end

  Where the configuration for the cache must be in your application environment,
  usually defined in your `config/config.exs`:

      config :my_app, MyApp.PartitionedCache,
        primary: [
          gc_interval: 3_600_000,
          backend: :shards
        ]

  If your application was generated with a supervisor (by passing `--sup`
  to `mix new`) you will have a `lib/my_app/application.ex` file containing
  the application start callback that defines and starts your supervisor.
  You just need to edit the `start/2` function to start the cache as a
  supervisor on your application's supervisor:

      def start(_type, _args) do
        children = [
          {MyApp.PartitionedCache, []},
          ...
        ]

  See `Nebulex.Cache` for more information.

  ## Configuration options

  This adapter supports the following configuration options:

  #{Nebulex.Adapters.Partitioned.Options.start_options_docs()}

  ## Shared runtime options

  When using the partitioned adapter, all of the cache functions outlined in
  `Nebulex.Cache` accept the following options:

  #{Nebulex.Adapters.Partitioned.Options.common_runtime_options_docs()}

  ### Stream options

  The `stream` command supports the following options:

  #{Nebulex.Adapters.Partitioned.Options.stream_options_docs()}

  ## Telemetry events

  Since the partitioned adapter depends on the configured primary storage
  cache (which uses a local cache adapter), this one will also emit Telemetry
  events. Therefore, there will be events emitted by the partitioned adapter
  as well as the primary storage cache. For example, the cache defined before
  `MyApp.PartitionedCache` will emit the following events:

    * `[:my_app, :partitioned_cache, :command, :start]`
    * `[:my_app, :partitioned_cache, :primary, :command, :start]`
    * `[:my_app, :partitioned_cache, :command, :stop]`
    * `[:my_app, :partitioned_cache, :primary, :command, :stop]`
    * `[:my_app, :partitioned_cache, :command, :exception]`
    * `[:my_app, :partitioned_cache, :primary, :command, :exception]`

  As you may notice, the telemetry prefix by default for the cache is
  `[:my_app, :partitioned_cache]`. However, you could specify the
  `:telemetry_prefix` for the primary storage within the `:primary` options
  (if you want to override the default). See the
  [Telemetry guide](http://hexdocs.pm/nebulex/telemetry.html)
  for more information and examples.

  ## Adapter-specific telemetry events

  The RingMonitor process emits the following Telemetry events during the
  lifetime of the partitioned cache:

    * `telemetry_prefix ++ [:ring_monitor, :started]` - Dispatched when the
      RingMonitor process starts.

      * Measurements: `%{system_time: non_neg_integer}`
      * Metadata:

        ```
        %{
          adapter_meta: %{optional(atom) => term},
          node: atom
        }
        ```

    * `telemetry_prefix ++ [:ring_monitor, :joined]` - Dispatched when the
      RingMonitor has successfully joined the `:pg` group to enter the cluster.

      * Measurements: `%{system_time: non_neg_integer}`
      * Metadata:

        ```
        %{
          adapter_meta: %{optional(atom) => term},
          node: atom
        }
        ```

    * `telemetry_prefix ++ [:ring_monitor, :nodes_added]` - Dispatched when
      nodes are added to the hash ring.

      * Measurements: `%{system_time: non_neg_integer}`
      * Metadata:

        ```
        %{
          adapter_meta: %{optional(atom) => term},
          node: atom,
          nodes: [atom]
        }
        ```

    * `telemetry_prefix ++ [:ring_monitor, :nodes_removed]` - Dispatched when
      nodes are removed from the hash ring.

      * Measurements: `%{system_time: non_neg_integer}`
      * Metadata:

        ```
        %{
          adapter_meta: %{optional(atom) => term},
          node: atom,
          nodes: [atom]
        }
        ```

    * `telemetry_prefix ++ [:ring_monitor, :exit]` - Dispatched when the
      RingMonitor receives an EXIT signal.

      * Measurements: `%{system_time: non_neg_integer}`
      * Metadata:

        ```
        %{
          adapter_meta: %{optional(atom) => term},
          node: atom,
          reason: term
        }
        ```

    * `telemetry_prefix ++ [:ring_monitor, :stopped]` - Dispatched when the
      RingMonitor process terminates.

      * Measurements: `%{system_time: non_neg_integer}`
      * Metadata:

        ```
        %{
          adapter_meta: %{optional(atom) => term},
          node: atom,
          reason: term
        }
        ```

  ## Info API

  As explained above, the partitioned adapter depends on the configured primary
  storage adapter. Therefore, the information the `info` command provides will
  depend on the primary storage adapter. The Nebulex built-in adapters support
  the recommended keys `:server`, `:memory`, and `:stats`. Additionally, the
  partitioned adapter supports:

    * `:nodes_info` - A map with the info for each node.
    * `:nodes` - A list with the cluster nodes.

  For example, the info for `MyApp.PartitionedCache` may look like this:

      iex> MyApp.PartitionedCache.info!()
      %{
        memory: %{total: nil, used: 344600},
        server: %{
          cache_module: MyApp.PartitionedCache,
          cache_name: :partitioned_cache,
          cache_adapter: Nebulex.Adapters.Partitioned,
          cache_pid: #PID<0.1053.0>,
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
        nodes: [:"node1@127.0.0.1", ...],
        nodes_info: %{
          "node1@127.0.0.1": %{
            memory: %{total: nil, used: 68920},
            server: %{
              cache_module: MyApp.PartitionedCache.Primary,
              cache_name: MyApp.PartitionedCache.Primary,
              cache_adapter: Nebulex.Adapters.Local,
              cache_pid: #PID<23981.823.0>,
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
          ...
        }
      }

  ## Extended API

  This adapter provides some additional convenience functions to the
  `Nebulex.Cache` API.

  Retrieving the primary storage or local cache module:

      MyCache.__primary__()

  Retrieving the cluster nodes associated with the given cache `name`:

      MyCache.nodes()

  Get a cluster node based on the given `key`:

      MyCache.find_node("mykey")

      MyCache.find_node!("mykey")

  Joining the cache to the cluster:

      MyCache.join_cluster()

  Leaving the cluster (removes the cache from the cluster):

      MyCache.leave_cluster()

  ## Caveats of partitioned adapter

  For operations that receive anonymous functions as arguments, such as
  `c:Nebulex.Cache.get_and_update/3`, `c:Nebulex.Cache.update/4`,
  `c:Nebulex.Cache.fetch_or_store/3`, and `c:Nebulex.Cache.get_or_store/3`,
  etc., there's an important consideration: these anonymous functions are
  compiled into the module where they are created. Since the distributed adapter
  executes operations on remote nodes, these functions may not exist on the
  target nodes.

  To ensure these operations work correctly in a distributed environment, you
  must provide functions from modules that exist on all nodes in the cluster.
  This can be achieved by:

  * Using named functions from modules that are available across all nodes.
  * Defining the functions in a shared module that's loaded on every node.
  * Using function references that can be serialized and transmitted.

  Example of the recommended approach:

      # Instead of anonymous functions, use named functions from shared modules
      defmodule MyApp.CacheHelpers do
        def increment_value(current_value) do
          (current_value || 0) + 1
        end
      end

      # Use the named function in cache operations
      MyCache.get_and_update("counter", &MyApp.CacheHelpers.increment_value/1)

  """

  # Provide Cache Implementation
  @behaviour Nebulex.Adapter
  @behaviour Nebulex.Adapter.KV
  @behaviour Nebulex.Adapter.Queryable

  # Inherit default transaction implementation
  use Nebulex.Adapter.Transaction

  # Inherit default info implementation
  use Nebulex.Adapters.Common.Info

  # Inherit default observable implementation
  use Nebulex.Adapter.Observable

  import Nebulex.Adapter
  import Nebulex.Utils

  alias __MODULE__.Options
  alias Nebulex.Distributed.{Cluster, RPC}
  alias Nebulex.Distributed.Helpers, as: H

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
        |> get_ring_name()
        |> Cluster.ring_nodes()
      end

      @doc """
      A convenience function to get the node of the given `key`.
      """
      def find_node(name \\ get_dynamic_cache(), key) do
        name
        |> get_ring_name()
        |> Cluster.find_node(key)
      end

      @doc """
      Same as `find_node/2` but raises an error if an error occurs.
      """
      def find_node!(name \\ get_dynamic_cache(), key) do
        case find_node(name, key) do
          {:ok, node} -> node
          {:error, _} = error -> raise error
        end
      end

      @doc """
      A convenience function for joining the cache to the cluster.
      """
      def join_cluster(name \\ get_dynamic_cache()) do
        name
        |> get_ring_name()
        |> Cluster.join()
      end

      @doc """
      A convenience function for removing the cache from the cluster.
      """
      def leave_cluster(name \\ get_dynamic_cache()) do
        name
        |> get_ring_name()
        |> Cluster.leave()
      end

      @doc """
      A convenience function for getting the ring name.
      """
      def get_ring_name(name) do
        name
        |> lookup_meta()
        |> get_in([:hash_ring, :name])
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

    # Hash ring options
    hash_ring =
      opts
      |> Keyword.fetch!(:hash_ring)
      |> Keyword.put(:name, camelize_and_concat([name, Ring]))

    # Prepare metadata
    adapter_meta = %{
      telemetry_prefix: telemetry_prefix,
      telemetry: telemetry,
      name: name,
      primary_name: primary_opts[:name],
      hash_ring: hash_ring,
      rejoin_interval: Keyword.fetch!(opts, :rejoin_interval)
    }

    # Prepare child spec
    child_spec =
      Supervisor.child_spec(
        {Nebulex.Adapters.Partitioned.Supervisor, {cache, name, adapter_meta, primary_opts}},
        id: {__MODULE__, name}
      )

    {:ok, child_spec, adapter_meta}
  end

  ## Nebulex.Adapter.KV

  @impl true
  def fetch(adapter_meta, key, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    call(adapter_meta, key, :fetch, [key, opts], opts)
  end

  @impl true
  def put(adapter_meta, key, value, on_write, ttl, keep_ttl?, opts) do
    opts = Options.validate_common_runtime_opts!(opts)
    primary_opts = [ttl: ttl, keep_ttl: keep_ttl?] ++ opts

    case on_write do
      :put ->
        with :ok <- call(adapter_meta, key, :put, [key, value, primary_opts], opts) do
          {:ok, true}
        end

      :put_new ->
        call(adapter_meta, key, :put_new, [key, value, primary_opts], opts)

      :replace ->
        call(adapter_meta, key, :replace, [key, value, primary_opts], opts)
    end
  end

  @impl true
  def put_all(adapter_meta, entries, on_write, ttl, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    case on_write do
      :put ->
        do_put_all(:put_all, adapter_meta, entries, ttl, opts)

      :put_new ->
        do_put_all(:put_new_all, adapter_meta, entries, ttl, opts)
    end
  end

  defp do_put_all(action, adapter_meta, entries, ttl, opts) do
    timeout = Keyword.fetch!(opts, :timeout)
    opts = [ttl: ttl] ++ opts

    reducer = fn
      {:ok, :ok}, _, {_bool, acc} ->
        {:cont, {true, acc}}

      {:ok, {:ok, true}}, {_, {_, _, [_, _, [kv, _]]}}, {bool, acc} ->
        {:cont, {bool, Enum.reduce(kv, acc, &[elem(&1, 0) | &2])}}

      {:ok, {:ok, false}}, _, {_bool, acc} ->
        {:cont, {false, acc}}

      {:ok, {:error, _} = error}, _, {_bool, acc} ->
        {:halt, {error, acc}}

      {:error, _} = error, _, {_bool, acc} ->
        {:halt, {error, acc}}
    end

    case map_reduce(entries, adapter_meta, action, [opts], timeout, {true, []}, reducer) do
      {:error, _} = error ->
        error

      {true, _} ->
        {:ok, true}

      {false, keys} ->
        :ok = Enum.each(keys, &delete(adapter_meta, &1, []))

        {:ok, false}

      {error, keys} ->
        :ok = Enum.each(keys, &delete(adapter_meta, &1, []))

        error
    end
  end

  @impl true
  def delete(adapter_meta, key, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    call(adapter_meta, key, :delete, [key, opts], opts)
  end

  @impl true
  def take(adapter_meta, key, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    call(adapter_meta, key, :take, [key, opts], opts)
  end

  @impl true
  def has_key?(adapter_meta, key, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    call(adapter_meta, key, :has_key?, [key, opts], opts)
  end

  @impl true
  def ttl(adapter_meta, key, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    call(adapter_meta, key, :ttl, [key, opts], opts)
  end

  @impl true
  def expire(adapter_meta, key, ttl, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    call(adapter_meta, key, :expire, [key, ttl, opts], opts)
  end

  @impl true
  def touch(adapter_meta, key, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    call(adapter_meta, key, :touch, [key, opts], opts)
  end

  @impl true
  def update_counter(adapter_meta, key, amount, default, ttl, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    call(adapter_meta, key, :incr, [key, amount, [ttl: ttl, default: default] ++ opts], opts)
  end

  ## Nebulex.Adapter.Queryable

  @impl true
  def execute(adapter_meta, query, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    do_execute(adapter_meta, query, opts)
  end

  defp do_execute(_adapter_meta, %{op: :get_all, query: {:in, []}}, _opts) do
    {:ok, []}
  end

  defp do_execute(_adapter_meta, %{op: op, query: {:in, []}}, _opts)
       when op in [:count_all, :delete_all] do
    {:ok, 0}
  end

  defp do_execute(adapter_meta, %{op: op, query: {:in, keys}} = query, opts) do
    timeout = Keyword.fetch!(opts, :timeout)

    query = build_query(query)
    group_fun = &Keyword.put(query, :in, &1)

    init_acc = if op == :get_all, do: [], else: 0

    reducer = fn
      {:ok, {:ok, res}}, _, acc when is_list(res) ->
        {:cont, res ++ acc}

      {:ok, {:ok, res}}, _, acc when is_integer(res) ->
        {:cont, acc + res}

      {:ok, {:error, _} = error}, _, _ ->
        {:halt, error}

      {:error, _} = error, _, _ ->
        {:halt, error}
    end

    case map_reduce(keys, adapter_meta, op, [opts], timeout, init_acc, reducer, group_fun) do
      {:error, _} = error ->
        error

      other ->
        {:ok, other}
    end
  end

  defp do_execute(%{hash_ring: hash_ring} = adapter_meta, %{op: op} = query, opts) do
    ring = Keyword.fetch!(hash_ring, :name)
    timeout = Keyword.fetch!(opts, :timeout)
    query = build_query(query)

    RPC.multicall(
      Cluster.ring_nodes(ring),
      __MODULE__,
      :with_dynamic_cache,
      [adapter_meta, op, [query, opts]],
      timeout
    )
    |> handle_rpc_multi_call(op, reducer(op))
  end

  @impl true
  def stream(adapter_meta, query, opts) do
    opts = Options.validate_stream_opts!(opts)

    # The partitioned adapter is a wrapper adapter, it doesn't implement any
    # cache storage, it depends on other cache adapters to do so. There are no
    # entries to stream from the partitioned adapter itself. Therefore, this
    # is a workaround to create a stream to trigger the evaluation against the
    # configured primary storage adapter lazily.
    stream = fn _, _ ->
      {on_error, opts} = Keyword.pop!(opts, :on_error)

      case do_execute(adapter_meta, %{query | op: :get_all}, opts) do
        {:ok, results} ->
          {:halt, results}

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

    {:ok, stream}
  end

  ## Nebulex.Adapter.Transaction

  @impl true
  def transaction(%{hash_ring: hash_ring} = adapter_meta, fun, opts) do
    ring = Keyword.fetch!(hash_ring, :name)

    opts =
      opts
      |> Options.validate_common_runtime_opts!()
      |> Keyword.put(:nodes, Cluster.ring_nodes(ring))

    super(adapter_meta, fun, opts)
  end

  @impl true
  def in_transaction?(adapter_meta, opts) do
    opts = Options.validate_common_runtime_opts!(opts)

    super(adapter_meta, opts)
  end

  ## Nebulex.Adapter.Info

  @impl true
  def info(adapter_meta, spec, opts)

  def info(adapter_meta, :all, opts) do
    with {:ok, server} <- super(adapter_meta, :server, opts),
         {:ok, nodes_info} <- fetch_nodes_info(adapter_meta, :all, opts) do
      nodes_info
      |> info_agg()
      |> Map.merge(%{server: server, nodes: Map.keys(nodes_info), nodes_info: nodes_info})
      |> wrap_ok()
    end
  end

  def info(adapter_meta, :server, opts) do
    super(adapter_meta, :server, opts)
  end

  def info(%{hash_ring: hash_ring}, :nodes, _opts) do
    ring = Keyword.fetch!(hash_ring, :name)

    {:ok, Cluster.ring_nodes(ring)}
  end

  def info(adapter_meta, :nodes_info, opts) do
    fetch_nodes_info(adapter_meta, :all, opts)
  end

  def info(_adapter_meta, [], _opts) do
    {:ok, %{}}
  end

  def info(adapter_meta, spec, opts) when is_list(spec) do
    server =
      if Enum.member?(spec, :server) do
        {:ok, server} = super(adapter_meta, :server, opts)

        %{server: server}
      else
        %{}
      end

    with {:ok, nodes_info} <-
           fetch_nodes_info(
             adapter_meta,
             Enum.reject(spec, &(&1 in [:nodes, :nodes_info])),
             opts
           ) do
      info =
        if Enum.member?(spec, :nodes_info) do
          %{nodes_info: nodes_info}
        else
          %{}
        end

      info =
        if Enum.member?(spec, :nodes) do
          Map.put(info, :nodes, Map.keys(nodes_info))
        else
          info
        end

      nodes_info
      |> info_agg()
      |> Map.merge(info)
      |> Map.merge(server)
      |> wrap_ok()
    end
  end

  def info(adapter_meta, spec, opts) do
    with {:ok, nodes_info} <- fetch_nodes_info(adapter_meta, spec, opts) do
      {:ok, info_agg(nodes_info)}
    end
  end

  defp fetch_nodes_info(%{hash_ring: hash_ring} = adapter_meta, spec, opts) do
    opts = Options.validate_common_runtime_opts!(opts)
    ring = Keyword.fetch!(hash_ring, :name)

    RPC.multicall(
      Cluster.ring_nodes(ring),
      __MODULE__,
      :with_dynamic_cache,
      [adapter_meta, :info, [spec, opts]],
      Keyword.fetch!(opts, :timeout)
    )
    |> handle_rpc_multi_call(:info, reducer(:info))
  end

  defp info_agg(info) do
    info
    |> Map.values()
    |> Enum.reduce(%{}, &H.merge_info_maps(&2, Map.delete(&1, :server)))
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

  ## Private Functions

  defp build_query(%{select: select, query: query}) do
    query = with {:q, q} <- query, do: {:query, q}

    [query, select: select]
  end

  defp reducer(:info) do
    &Map.new/1
  end

  defp reducer(op) when op in [:get_all, :stream] do
    &Enum.flat_map(&1, fn {_node, results} -> results end)
  end

  defp reducer(op) when op in [:count_all, :delete_all] do
    fn result ->
      result
      |> Enum.map(&elem(&1, 1))
      |> Enum.sum()
    end
  end

  defp find_node(%{hash_ring: hash_ring}, key) do
    hash_ring
    |> Keyword.fetch!(:name)
    |> Cluster.find_node(key)
  end

  defp call(adapter_meta, key, action, args, opts) do
    timeout = Keyword.fetch!(opts, :timeout)

    with {:ok, node} <- find_node(adapter_meta, key) do
      RPC.call(node, __MODULE__, :with_dynamic_cache, [adapter_meta, action, args], timeout)
    end
  end

  defp map_reduce(enum, meta, action, args, timeout, acc, reducer, group_fun \\ & &1) do
    enum
    |> group_by_node(meta, action)
    |> Enum.map(fn {node, group} ->
      {node, {__MODULE__, :with_dynamic_cache, [meta, action, [group_fun.(group) | args]]}}
    end)
    |> RPC.multi_mfa_call(timeout, acc, reducer)
  catch
    error -> error
  end

  defp group_by_node(enum, adapter_meta, action) when action in [:put_all, :put_new_all] do
    Enum.group_by(enum, &find_node_or_throw(adapter_meta, elem(&1, 0)))
  end

  defp group_by_node(enum, adapter_meta, _action) do
    Enum.group_by(enum, &find_node_or_throw(adapter_meta, &1))
  end

  defp find_node_or_throw(adapter_meta, key) do
    case find_node(adapter_meta, key) do
      {:ok, node} -> node
      {:error, _} = error -> throw(error)
    end
  end

  defp handle_rpc_multi_call({res, []}, _action, fun) do
    {:ok, fun.(res)}
  end

  defp handle_rpc_multi_call({_responses, errors}, action, _) do
    wrap_error Nebulex.Error,
      reason: {:rpc, {:unexpected_errors, errors}},
      module: __MODULE__,
      action: action
  end

  ## Error formatting

  @doc false
  def format_error({:rpc, {:unexpected_errors, errors}}, metadata) do
    action = Keyword.fetch!(metadata, :action)

    formatted_errors =
      Enum.map_join(errors, "\n\n", fn {{:error, reason}, node} ->
        ("Node #{node}:\n\n" <> Exception.format(:error, reason))
        |> String.replace("\n", "\n    ")
      end)

    """
    RPC multicall #{action} got unexpected errors.

    #{formatted_errors}
    """
  end
end
