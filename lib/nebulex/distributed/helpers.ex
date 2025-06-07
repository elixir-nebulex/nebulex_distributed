defmodule Nebulex.Distributed.Helpers do
  @moduledoc false

  ## API

  @doc """
  Merges two maps into one, resolving conflicts through a custom function
  that aggregates the results.

  ## Examples

      iex> Nebulex.Distributed.Helpers.merge_info_maps(
      ...>   %{a: %{a1: 1, a2: "a2", a3: %{a4: 5}}, b: 2},
      ...>   %{a: %{a1: 3, a2: "a21", a3: %{a4: 5}}, b: 3, c: %{c1: 1}}
      ...> )
      %{a: %{a1: 4, a2: "a21", a3: %{a4: 10}}, b: 5, c: %{c1: 1}}

  """
  @spec merge_info_maps(map(), map()) :: map()
  def merge_info_maps(map1, map2) when is_map(map1) and is_map(map2) do
    Map.merge(map1, map2, fn
      _k, v1, v2 when is_integer(v1) and is_integer(v2) ->
        v1 + v2

      _k, v1, v2 when is_map(v1) and is_map(v2) ->
        merge_info_maps(v1, v2)

      _k, _v1, v2 ->
        v2
    end)
  end
end
