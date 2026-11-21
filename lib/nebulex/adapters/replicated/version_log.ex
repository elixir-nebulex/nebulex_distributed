defmodule Nebulex.Adapters.Replicated.VersionLog do
  @moduledoc false

  # For every key changed recently, the version and the kind (`:put` or
  # `:delete`) of its last change, plus at most one flush barrier. Values in
  # the primary cache carry no version. The log lets a node tell a late
  # command from a new one across processing ticks.
  #
  # Entries expire by rotating two generations: writes go to the current
  # table, reads look at both, and every `window` the old table is cleared
  # and becomes the current one. An entry is therefore kept for at least one
  # window and at most two, with no timer per entry.
  #
  # Writes for a key come from the inbox processor, which handles a key in
  # one partition at a time, so a lookup followed by an insert is enough.
  # Losing the log is safe: every key then counts as old.

  use GenServer

  import Nebulex.Utils, only: [camelize_and_concat: 1]

  # State
  defstruct [:log, :window, :timer_ref]

  @typedoc "The log identifier (the name of its metadata table)."
  @type t() :: atom()

  @typedoc "The kind of the last change of a key."
  @type kind() :: :put | :delete

  ## API

  @doc false
  def start_link(adapter_meta) do
    GenServer.start_link(__MODULE__, adapter_meta)
  end

  @doc """
  Returns the log identifier for the cache `name`.
  """
  @spec name(atom()) :: t()
  def name(name), do: camelize_and_concat([name, VersionLog])

  @doc """
  Returns how long (in milliseconds) an entry is kept at least.

  It covers the slowest delivery of a replication batch (the interval plus
  every retry with its timeout) and, when anti-entropy is enabled, four
  anti-entropy intervals.
  """
  @spec window(map()) :: pos_integer()
  def window(%{
        interval: interval,
        timeout: timeout,
        retries: retries,
        retry_delay: retry_delay,
        anti_entropy_interval: anti_entropy_interval
      }) do
    delivery = interval + timeout * (retries + 1) + retry_delay * retries

    if is_integer(anti_entropy_interval),
      do: max(delivery, 4 * anti_entropy_interval),
      else: delivery
  end

  @doc """
  Returns the logged `{version, kind}` for `key`, or `nil` if the key has
  not changed within the window.
  """
  @spec lookup(t(), any()) :: {integer(), kind()} | nil
  def lookup(log, key) do
    {current, previous} = generations(log)

    # A key is only written to the current generation with a version greater
    # than the one in the previous generation, so the current one wins.
    with nil <- lookup_gen(current, key) do
      lookup_gen(previous, key)
    end
  end

  @doc """
  Records a change of `kind` with `version` for `key` if the version is
  greater than the logged one and than the barrier.

  Returns `true` if the change was recorded (and must be applied), and
  `false` if it is stale.
  """
  @spec record(t(), any(), integer(), kind()) :: boolean()
  def record(log, key, version, kind) do
    if version > max_version(log, key) do
      {current, _previous} = generations(log)

      true = :ets.insert(current, {key, version, kind})
    else
      false
    end
  end

  @doc """
  Returns `true` if `version` is greater than the logged version for `key`
  and than the barrier.
  """
  @spec newer?(t(), any(), integer()) :: boolean()
  def newer?(log, key, version) do
    version > max_version(log, key)
  end

  @doc """
  Returns the version for a local write of `key`: the wall clock, raised
  above the logged version and the barrier when the clock is behind them.
  """
  @spec next_version(t(), any()) :: integer()
  def next_version(log, key) do
    max(System.system_time(), max_version(log, key) + 1)
  end

  @doc """
  Returns the version for a local flush: the wall clock, raised above every
  logged version and the barrier when the clock is behind them.
  """
  @spec next_flush_version(t()) :: integer()
  def next_flush_version(log) do
    {current, previous} = generations(log)

    max_logged =
      [current, previous]
      |> Enum.map(&:ets.foldl(fn {_key, version, _kind}, acc -> max(version, acc) end, -1, &1))
      |> Enum.max()

    Enum.max([System.system_time(), max_logged + 1, barrier_or(log, -1) + 1])
  end

  @doc """
  Returns the flush barrier, or `nil` if there was no flush within the
  window.
  """
  @spec barrier(t()) :: integer() | nil
  def barrier(log), do: barrier_or(log, nil)

  @doc """
  Sets the flush barrier to `version` if it is greater than the current
  one. Returns `true` if it was set.
  """
  @spec put_barrier(t(), integer()) :: boolean()
  def put_barrier(log, version) do
    if version > barrier_or(log, -1) do
      [{:generations, _current, _previous, epoch}] = :ets.lookup(log, :generations)

      true = :ets.insert(log, {:barrier, version, epoch})
    else
      false
    end
  end

  @doc """
  Returns the keys whose last logged change is a `:put` with a version
  greater than `version`.
  """
  @spec puts_newer_than(t(), integer()) :: [any()]
  def puts_newer_than(log, version) do
    {current, previous} = generations(log)
    match_spec = [{{:"$1", :"$2", :put}, [{:>, :"$2", version}], [:"$1"]}]

    [current, previous]
    |> Enum.flat_map(&:ets.select(&1, match_spec))
    |> Enum.uniq()
    |> Enum.filter(&match?({_, :put}, lookup(log, &1)))
  end

  ## GenServer callbacks

  @impl true
  def init(%{version_log: log, version_window: window}) do
    _ =
      :ets.new(log, [
        :set,
        :named_table,
        :public,
        read_concurrency: true
      ])

    true = :ets.insert(log, {:generations, new_gen(), new_gen(), 0})

    {:ok, %__MODULE__{log: log, window: window, timer_ref: schedule_rotation(window)}}
  end

  @impl true
  def handle_info(:rotate, %__MODULE__{log: log, window: window} = state) do
    :ok = rotate(log)

    {:noreply, %{state | timer_ref: schedule_rotation(window)}}
  end

  ## Private functions

  defp new_gen do
    :ets.new(__MODULE__, [
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])
  end

  defp generations(log) do
    [{:generations, current, previous, _epoch}] = :ets.lookup(log, :generations)

    {current, previous}
  end

  defp lookup_gen(gen, key) do
    case :ets.lookup(gen, key) do
      [{^key, version, kind}] -> {version, kind}
      [] -> nil
    end
  end

  defp max_version(log, key) do
    logged =
      case lookup(log, key) do
        {version, _kind} -> version
        nil -> -1
      end

    max(logged, barrier_or(log, -1))
  end

  defp barrier_or(log, default) do
    case :ets.lookup(log, :barrier) do
      [{:barrier, version, _epoch}] -> version
      [] -> default
    end
  end

  # The previous generation is cleared and becomes the current one. Entries
  # written during the epoch that just ended stay readable for one more
  # window. The barrier follows the same rule.
  defp rotate(log) do
    [{:generations, current, previous, epoch}] = :ets.lookup(log, :generations)

    true = :ets.delete_all_objects(previous)
    true = :ets.insert(log, {:generations, previous, current, epoch + 1})

    case :ets.lookup(log, :barrier) do
      [{:barrier, _version, barrier_epoch}] when barrier_epoch < epoch ->
        true = :ets.delete(log, :barrier)

      _else ->
        :ok
    end

    :ok
  end

  defp schedule_rotation(window) do
    Process.send_after(self(), :rotate, window)
  end
end
