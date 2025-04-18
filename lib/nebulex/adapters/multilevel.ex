defmodule Nebulex.Adapters.Multilevel do
  @moduledoc """
  Adapter module for the multi-level cache topogy.

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
  `level` option, where the value is a positive integer greater than 0.

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
            gc_interval: :timer.hours(12),
            backend: :shards
          },
          {
            MyApp.Multilevel.L2,
            primary: [
              gc_interval: :timer.hours(12),
              backend: :shards
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

  Since the multi-level adapter works as a wrapper for the configured cache
  levels, these will emit the Nebulex Telemetry events. Therefore, there will
  be events emitted for each cache level. For example, the cache defined before
  `MyApp.Multilevel` will emit the following events:

    * Top level cache:
      * `[:my_app, :multilevel, :command, :start]`
      * `[:my_app, :multilevel, :command, :stop]`
      * `[:my_app, :multilevel, :command, :exception]`

    * L1 cache:
      * `[:my_app, :multilevel, :command, :start]`
      * `[:my_app, :multilevel, :command, :stop]`
      * `[:my_app, :multilevel, :command, :exception]`

    * L2 cache:
      * `[:my_app, :multilevel, :command, :start]`
      * `[:my_app, :multilevel, :primary, :command, :start]`
      * `[:my_app, :multilevel, :command, :stop]`
      * `[:my_app, :multilevel, :primary, :command, :stop]`
      * `[:my_app, :multilevel, :command, :exception]`
      * `[:my_app, :multilevel, :primary, :command, :exception]`

  As you may notice, the telemetry prefix by default for all cache levels is
  `[:my_app, :multilevel]`, but you can get the details about the cache from
  the metadata. Alternatively, you could specify the `:telemetry_prefix` for
  each cache level within the `:levels` option. For example:

      config :my_app, MyApp.Multilevel,
        levels: [
          {MyApp.Multilevel.L1, telemetry_prefix: [:my_app, :multilevel, :l1]},
          {MyApp.Multilevel.L2, telemetry_prefix: [:my_app, :multilevel, :l2]}
        ]

  In this case, the telemetry prefix for the L1 cache will be
  `[:my_app, :multilevel, :l1]` and the L2 cache will be
  `[:my_app, :multilevel, :l2]`.

  See also the [Telemetry guide](http://hexdocs.pm/nebulex/telemetry.html)
  for more information.

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

  ## Caveats of multi-level adapter

  Because this adapter reuses other existing/configured adapters, it inherits
  all their limitations too. Therefore, it is highly recommended to check the
  documentation of the adapters to use.
  """

  # Provide Cache Implementation
  @behaviour Nebulex.Adapter
  @behaviour Nebulex.Adapter.KV
  @behaviour Nebulex.Adapter.Queryable
  @behaviour Nebulex.Adapter.Info

  # Inherit default transaction implementation
  use Nebulex.Adapter.Transaction

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
          Cluster.get_nodes(name || cache) ++ acc
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
