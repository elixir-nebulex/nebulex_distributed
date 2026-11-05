defmodule Nebulex.Distributed.RPC do
  @moduledoc """
  RPC utilities.
  """

  import Nebulex.Utils, only: [wrap_error: 2]

  @typedoc "Task callback"
  @type mfa_call() :: {module(), atom(), [any()]}

  @typedoc "Group entry: node -> NFA call"
  @type node_mfa_call() :: {node(), mfa_call()}

  @typedoc "Node group"
  @type node_mfa_map() :: %{optional(node()) => mfa_call()} | [node_mfa_call()]

  @typedoc "Reducer accumulator"
  @type reducer_acc() :: any()

  @typedoc "Reducer function spec"
  @type reducer_fun() :: (result :: any(), node_mfa_call() | node(), reducer_acc() -> any())

  # Default timeout (5 seconds)
  @default_timeout :timer.seconds(5)

  ## API

  @doc """
  Evaluates `apply(mod, fun, args)` on node `node` without waiting for the
  result. Nothing is returned, and a node that is down is not an error.

  ## Example

      iex> Nebulex.Distributed.RPC.cast(node(), Map, :new, [[]])
      :ok

  """
  @spec cast(node(), module(), atom(), [any()]) :: :ok
  def cast(node, mod, fun, args) do
    :erpc.cast(node, mod, fun, args)
  end

  @doc """
  Evaluates `apply(mod, fun, args)` on node `node` and returns the corresponding
  evaluation result.

  A timeout, in milliseconds or `:infinity`, can be given with a default value
  of `5000`.

  ## Example

      iex> Nebulex.Distributed.RPC.call(node(), Map, :new, [[]])
      %{}

  """
  @spec call(node(), module(), atom(), [any()], timeout()) :: any()
  def call(node, mod, fun, args, timeout \\ @default_timeout)

  def call(node, mod, fun, args, _timeout) when node == node() do
    apply(mod, fun, args)
  end

  def call(node, mod, fun, args, timeout) do
    :erpc.call(node, mod, fun, args, timeout)
  rescue
    e in ErlangError ->
      handle_error(e, {node, {mod, fun, args}})
  catch
    :exit, reason ->
      handle_error({:exit, reason}, {node, {mod, fun, args}})
  end

  @doc """
  In contrast to a regular single-node RPC, a multicall is an RPC that is sent
  concurrently from one client to multiple servers. The function evaluates
  `apply(module, fun, args)` on the specified nodes and collects the answers.
  Then, evaluates the `reducer_fun` function on each answer.

  ## Example

      iex> alias Nebulex.Distributed.RPC
      iex> RPC.multicall([node()], Map, :new, [[foo: :bar]])
      {[{:"primary@127.0.0.1", %{foo: :bar}}], []}
      iex> RPC.multicall([node()], Map, :new, [[foo: :bar]], 1000)
      {[{:"primary@127.0.0.1", %{foo: :bar}}], []}
      iex> RPC.multicall([node()], Map, :new, [[foo: :bar]], 1000, {[], []})
      {[{:"primary@127.0.0.1", %{foo: :bar}}], []}

  """
  @spec multicall([node()], module(), atom(), [any()], timeout(), reducer_acc(), reducer_fun()) ::
          any()
  def multicall(
        nodes,
        mod,
        fun,
        args,
        timeout \\ @default_timeout,
        reducer_acc \\ {[], []},
        reducer_fun \\ default_reducer()
      ) do
    nodes
    |> :erpc.multicall(mod, fun, args, timeout)
    |> Enum.zip(nodes)
    |> Enum.map(fn {result, node} ->
      {handle_error(result, {node, {mod, fun, args}}), node}
    end)
    |> Enum.reduce_while(reducer_acc, fn {res, node}, acc ->
      reducer_fun.(res, node, acc)
    end)
  end

  @doc """
  Similar to `multicall/7`, but it allows specifying the MFA per node.

  ## Example

      iex> node = node()
      iex> alias Nebulex.Distributed.RPC
      iex> RPC.multi_mfa_call(%{node => {Map, :new, [[foo: :bar]]}})
      {[{{:"primary@127.0.0.1", {Map, :new, [[foo: :bar]]}}, %{foo: :bar}}], []}
      iex> RPC.multi_mfa_call(%{node => {Map, :new, [[foo: :bar]]}}, 1000)
      {[{{:"primary@127.0.0.1", {Map, :new, [[foo: :bar]]}}, %{foo: :bar}}], []}
      iex> RPC.multi_mfa_call(
      ...>   %{node => {Map, :new, [[foo: :bar]]}},
      ...>   1000,
      ...>   {[], []}
      ...> )
      {[{{:"primary@127.0.0.1", {Map, :new, [[foo: :bar]]}}, %{foo: :bar}}], []}

  """
  @spec multi_mfa_call(node_mfa_map(), timeout(), reducer_acc(), reducer_fun()) :: any()
  def multi_mfa_call(
        node_group,
        timeout \\ @default_timeout,
        reducer_acc \\ {[], []},
        reducer_fun \\ default_reducer()
      ) do
    node_group
    |> Enum.map(fn {node, {mod, fun, args}} = group ->
      {:erpc.send_request(node, mod, fun, args), group}
    end)
    |> Enum.reduce_while(reducer_acc, fn {req_id, group}, acc ->
      try do
        res = :erpc.receive_response(req_id, timeout)

        reducer_fun.({:ok, res}, group, acc)
      rescue
        e in ErlangError ->
          handle_error(e, group)
          |> reducer_fun.(group, acc)
      catch
        :exit, reason ->
          handle_error({:exit, reason}, group)
          |> reducer_fun.(group, acc)
      end
    end)
  end

  ## Handling errors

  @doc false
  def handle_error(reason, node_mfa)

  def handle_error(%ErlangError{original: original}, node_mfa) do
    handle_error(original, node_mfa)
  end

  def handle_error({:error, reason}, node_mfa) do
    handle_error(reason, node_mfa)
  end

  def handle_error({:exception, reason, st}, {node, mfa}) do
    raise """
    RPC runtime error occurred while executing the command and raised the following exception:

        #{Exception.format(:error, reason, st) |> String.replace("\n", "\n    ")}

    Cache command:

    #{format_mfa(mfa)}

    Node:

    #{inspect(node)}
    """
  end

  def handle_error({:erpc, reason}, {node, mfa}) do
    wrap_error Nebulex.Error,
      reason: {:rpc, {:error, reason}},
      module: __MODULE__,
      node: node,
      mfa: mfa
  end

  def handle_error({:exit, reason}, {node, mfa}) do
    wrap_error Nebulex.Error,
      reason: {:rpc, {:exit, reason}},
      module: __MODULE__,
      node: node,
      mfa: mfa
  end

  def handle_error(other, _node_mfa) do
    other
  end

  ## Exception formatter

  @doc false
  def format_error(error, opts)

  def format_error({:rpc, {:error, reason}}, opts) do
    node = Keyword.fetch!(opts, :node)
    mfa = Keyword.fetch!(opts, :mfa)

    """
    the RPC operation failed with reason: #{inspect(reason)}.

    Cache command:

    #{format_mfa(mfa)}

    Node:

    #{inspect(node)}
    """
  end

  def format_error({:rpc, {:exit, {:exception, reason}}}, opts) do
    node = Keyword.fetch!(opts, :node)
    mfa = Keyword.fetch!(opts, :mfa)

    """
    the applied function exited with reason: #{inspect(reason)}.

    Cache command:

    #{format_mfa(mfa)}

    Node:

    #{inspect(node)}
    """
  end

  def format_error({:rpc, {:exit, {:signal, reason}}}, opts) do
    node = Keyword.fetch!(opts, :node)
    mfa = Keyword.fetch!(opts, :mfa)

    """
    the process that executed the command exited with reason: #{inspect(reason)}.

    Cache command:

    #{format_mfa(mfa)}

    Node:

    #{inspect(node)}
    """
  end

  # Internal MFAs running a cache command on the node's cache are reported
  # as the cache command itself.
  defp format_mfa({_, _, [%{cache: cache}, action, args]}) when is_atom(action) do
    format_mfa({cache, action, args})
  end

  defp format_mfa({m, f, a}) when is_list(a) do
    "#{inspect(m)}.#{f}/#{Enum.count(a)}"
  end

  ## Private functions

  defp default_reducer do
    fn
      {:ok, {:ok, res}}, node_call, {ok, err} ->
        {:cont, {[{node_call, res} | ok], err}}

      {:ok, {:error, _} = error}, node_call, {ok, err} ->
        {:cont, {ok, [{error, node_call} | err]}}

      {:ok, res}, node_call, {ok, err} ->
        {:cont, {[{node_call, res} | ok], err}}

      {kind, _} = error, node_call, {ok, err} when kind in [:error, :exit, :throw] ->
        {:cont, {ok, [{error, node_call} | err]}}
    end
  end
end
