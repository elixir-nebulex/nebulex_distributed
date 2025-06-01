defmodule Nebulex.Distributed.Cluster do
  # The module used by cache adapters for
  # distributed caching functionality.
  @moduledoc false

  import Nebulex.Utils, only: [wrap_error: 2]

  alias ExHashRing.Ring

  @doc """
  Returns the `:pg` scope.
  """
  @spec pg_scope() :: atom()
  def pg_scope, do: __MODULE__

  @doc """
  Returns the `:pg` child spec.
  """
  @spec pg_child_spec() :: Supervisor.child_spec()
  def pg_child_spec do
    %{id: :pg, start: {:pg, :start_link, [pg_scope()]}}
  end

  @doc """
  Wrapper for `:pg.monitor_scope/1`.
  """
  @spec monitor_scope() :: reference()
  def monitor_scope do
    {pg_ref, _} = :pg.monitor_scope(pg_scope())

    pg_ref
  end

  @doc """
  Joins the node where the cache `name`'s supervisor process is running to the
  `name`'s node group.
  """
  @spec join(name :: atom()) :: :ok
  def join(name) do
    pid = Process.whereis(name) || self()

    if pid in pg_members(name) do
      :ok
    else
      :ok = pg_join(name, pid)
    end
  end

  @doc """
  Makes the node where the cache `name`'s supervisor process is running, leave
  the `name`'s node group.
  """
  @spec leave(name :: atom()) :: :ok
  def leave(name) do
    pg_leave(name, Process.whereis(name) || self())
  end

  @doc """
  Returns the list of nodes joined to given `name`'s node group.
  """
  @spec pg_nodes(name :: atom()) :: [node()]
  def pg_nodes(name) do
    name
    |> pg_members()
    |> Enum.map(&node/1)
    |> Enum.uniq()
  end

  @doc """
  Returns the list of nodes in the ring.
  """
  @spec ring_nodes(Ring.ring()) :: [node()]
  def ring_nodes(ring) do
    {:ok, nodes} = Ring.get_nodes(ring)

    Enum.map(nodes, &to_node/1)
  end

  @doc """
  Finds a node in the ring.
  """
  @spec find_node(Ring.ring(), any()) :: {:ok, node()} | {:error, Nebulex.Error.t()}
  def find_node(ring, key) do
    case Ring.find_node(ring, :erlang.phash2(key)) do
      {:ok, node} ->
        {:ok, to_node(node)}

      {:error, reason} ->
        wrap_error Nebulex.Error,
          module: __MODULE__,
          reason: {:find_node_error, reason},
          ring: ring,
          key: key
    end
  end

  ## Error formatting

  @doc false
  def format_error({:find_node_error, reason}, metadata) do
    ring = Keyword.fetch!(metadata, :ring)
    key = Keyword.fetch!(metadata, :key)

    "Error finding node in ring #{inspect(ring)} for key #{inspect(key)}: #{inspect(reason)}"
  end

  ## PG

  defp pg_join(name, pid) do
    :ok = :pg.join(__MODULE__, name, pid)
  end

  defp pg_leave(name, pid) do
    _ = :pg.leave(__MODULE__, name, pid)

    :ok
  end

  defp pg_members(name) do
    :pg.get_members(__MODULE__, name)
  end

  ## Other helpers

  # FIXME: This is a hack due to HashRing defines a node name as a binary.
  defp to_node(node) when is_atom(node), do: node
  # coveralls-ignore-start
  # sobelow_skip ["DOS.StringToAtom"]
  defp to_node(node) when is_binary(node), do: String.to_atom(node)
  # coveralls-ignore-stop
end
