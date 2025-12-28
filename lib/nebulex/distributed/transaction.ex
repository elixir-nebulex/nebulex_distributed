defmodule Nebulex.Distributed.Transaction do
  @moduledoc """
  Default transaction implementation for distributed cache adapters.

  This module provides a transaction implementation based on Erlang's `:global`
  module for distributed locking across multiple nodes. It is designed for
  distributed cache topologies such as partitioned, multilevel, and replicated
  caches where transactions need to coordinate across a cluster of nodes.

  Distributed adapters in the `nebulex_distributed` package use this module
  via `use Nebulex.Distributed.Transaction` to inherit the `:global`-based
  transaction implementation.

  ## How It Works

  The transaction mechanism uses `:global.set_lock/3` to acquire distributed
  locks across specified nodes:

  1. **Lock acquisition**: Attempts to acquire locks for specified keys (or a
     global lock if no keys are specified) across all nodes in the cluster.
  2. **All-or-nothing**: If any lock cannot be acquired, all partial locks are
     released and the transaction is aborted.
  3. **Execution**: Once all locks are acquired, the transaction function
     executes.
  4. **Lock release**: Locks are released in an `after` block to ensure cleanup
     even if the transaction fails.

  ## Lock Scope

  ### Global Lock (Not Recommended)

  When no keys are specified, a global lock is used, serializing all
  transactions across the cluster:

      MyCache.transaction(fn ->
        # Critical section - entire cache is locked
      end)

  **Warning**: This approach severely impacts performance as all transactions
  are serialized, regardless of which keys they access.

  ### Fine-Grained Locking (Recommended)

  Specify the keys involved to enable concurrent transactions on different keys:

      MyCache.transaction(fn ->
        # Only :counter is locked
        counter = MyCache.get(:counter)
        MyCache.put(:counter, counter + 1)
      end, keys: [:counter])

  Multiple processes can run transactions concurrently as long as they don't
  access the same keys.

  ## Nested Transactions

  Nested transactions are supported. If a transaction is already in progress
  (detected via process dictionary), the nested transaction executes without
  attempting to acquire locks again:

      MyCache.transaction(fn ->
        # Outer transaction acquires locks

        MyCache.transaction(fn ->
          # Nested transaction - reuses outer locks
        end)
      end)

  ## Node Coordination

  By default, locks are acquired only on the local node (`[node()]`). For true
  distributed transactions, specify all nodes in the cluster:

      MyCache.transaction(
        fn ->
          # Critical section
        end,
        keys: [:key1],
        nodes: [node() | Node.list()]
      )

  This ensures the transaction is coordinated across all nodes in the cluster.

  > #### 💡 Important Note {: .info}
  >
  > When using any distributed adapter (`Nebulex.Adapters.Partitioned`,
  > `Nebulex.Adapters.Multilevel`, etc.), you **do not need to specify the
  > `:nodes` option**. The adapters automatically determine and set the nodes
  > based on the cluster topology.

  ## Performance Considerations

  - **Fine-grained locking**: Always specify keys to maximize concurrency.
  - **Lock contention**: Multiple transactions on the same keys will serialize.
  - **Network overhead**: Distributed lock coordination adds latency.
  - **Retry mechanism**: Failed lock acquisitions retry indefinitely by default
    (configurable via `:retries` option).

  ## Use Cases

  This implementation is suitable for:

  - **Distributed caches** running across multiple nodes.
  - **Strong consistency** requirements across the cluster.
  - **Atomic operations** on cache entries that need cluster-wide coordination.
  - **Partitioned caches** where transactions may span multiple partitions.

  For single-node scenarios, consider using a local locking mechanism like
  `Nebulex.Locks` (used by `nebulex_local`) for better performance.

  ## Options

  #{Nebulex.Distributed.Transaction.Options.options_docs()}

  ## Examples

  ### Basic Transaction with Fine-Grained Locking

      # Increment a counter atomically
      MyCache.transaction(fn ->
        counter = MyCache.get!(:counter, default: 0)
        MyCache.put!(:counter, counter + 1)
      end, keys: [:counter])

  ### Multi-Key Transaction

      # Transfer balance between two accounts
      MyCache.transaction(fn ->
        alice = MyCache.get!(:alice)
        bob = MyCache.get!(:bob)

        MyCache.put!(:alice, %{alice | balance: alice.balance - 100})
        MyCache.put!(:bob, %{bob | balance: bob.balance + 100})
      end, keys: [:alice, :bob])

  ### Distributed Transaction Across Cluster

      # With Partitioned or Multilevel adapters (nodes automatically determined)
      MyCache.transaction(fn ->
        # Critical section coordinated across cluster
        # Nodes are automatically discovered via :pg
        value = MyCache.get!(:shared_resource)
        MyCache.put!(:shared_resource, update(value))
      end, keys: [:shared_resource])

      # With custom adapters or direct module usage (manual node specification)
      nodes = [node() | Node.list()]

      MyCache.transaction(fn ->
        # Critical section coordinated across specified nodes
        value = MyCache.get!(:shared_resource)
        MyCache.put!(:shared_resource, update(value))
      end, keys: [:shared_resource], nodes: nodes)

  ### Transaction with Custom Retry Policy

      # Limit retry attempts to avoid indefinite blocking
      MyCache.transaction(
        fn ->
          # Critical section
        end,
        keys: [:key1],
        retries: 5
      )
      |> case do
        {:ok, result} ->
          # Transaction succeeded
          ...

        {:error, %Nebulex.Error{reason: :transaction_aborted}} ->
          # Failed to acquire locks after retries
          ...
      end

  """

  alias __MODULE__.Options

  import Nebulex.Utils, only: [wrap_ok: 1, wrap_error: 2]

  @doc false
  defmacro __using__(_opts) do
    quote do
      @behaviour Nebulex.Adapter.Transaction

      @impl true
      defdelegate transaction(adapter_meta, fun, opts), to: unquote(__MODULE__)

      @impl true
      defdelegate in_transaction?(adapter_meta, opts), to: unquote(__MODULE__)

      defoverridable transaction: 3, in_transaction?: 2
    end
  end

  @doc false
  def transaction(%{cache: cache, pid: pid} = adapter_meta, fun, opts) do
    opts = Options.validate!(opts)

    adapter_meta
    |> do_in_transaction?()
    |> do_transaction(
      pid,
      adapter_meta[:name] || cache,
      Keyword.fetch!(opts, :keys),
      Keyword.get(opts, :nodes, [node()]),
      Keyword.fetch!(opts, :retries),
      fun
    )
  end

  @doc false
  def in_transaction?(adapter_meta, _opts) do
    wrap_ok do_in_transaction?(adapter_meta)
  end

  ## Helpers

  defp do_in_transaction?(%{pid: pid}) do
    !!Process.get({pid, self()})
  end

  defp do_transaction(true, _pid, _name, _keys, _nodes, _retries, fun) do
    {:ok, fun.()}
  end

  defp do_transaction(false, pid, name, keys, nodes, retries, fun) do
    ids = lock_ids(name, keys)

    case set_locks(ids, nodes, retries) do
      true ->
        try do
          _ = Process.put({pid, self()}, %{keys: keys, nodes: nodes})

          {:ok, fun.()}
        after
          _ = Process.delete({pid, self()})

          del_locks(ids, nodes)
        end

      false ->
        wrap_error Nebulex.Error,
          reason: :transaction_aborted,
          cache: name,
          nodes: nodes,
          cache: name
    end
  end

  defp set_locks(ids, nodes, retries) do
    maybe_set_lock = fn id, {:ok, acc} ->
      case :global.set_lock(id, nodes, retries) do
        true -> {:cont, {:ok, [id | acc]}}
        false -> {:halt, {:error, acc}}
      end
    end

    case Enum.reduce_while(ids, {:ok, []}, maybe_set_lock) do
      {:ok, _} ->
        true

      {:error, locked_ids} ->
        :ok = del_locks(locked_ids, nodes)

        false
    end
  end

  defp del_locks(ids, nodes) do
    Enum.each(ids, &:global.del_lock(&1, nodes))
  end

  defp lock_ids(name, []) do
    [{name, self()}]
  end

  defp lock_ids(name, keys) do
    Enum.map(keys, &{{name, &1}, self()})
  end
end
