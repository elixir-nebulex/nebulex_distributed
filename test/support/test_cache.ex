defmodule Nebulex.Distributed.TestCache do
  @moduledoc false

  defmodule Commons do
    @moduledoc false

    defmacro __using__(_opts) do
      quote do
        import Nebulex.Utils, only: [wrap_error: 2]

        @dialyzer {:nowarn_function, raise_error: 0, exit: 0, exit_signal: 0}

        @doc false
        def return_error(result) do
          wrap_error Nebulex.Error, reason: :error, result: result
        end

        @doc false
        def raise_error do
          raise ArgumentError, "error"
        end

        @doc false
        def exit do
          exit("bye")
        end

        @doc false
        def exit_signal do
          Task.async(fn -> raise "bye" end)
          |> Task.await()
        end

        @doc false
        def get_and_update_fun(nil), do: {nil, 1}
        def get_and_update_fun(current) when is_integer(current), do: {current, current * 2}

        @doc false
        def get_and_update_bad_fun(_), do: :other
      end
    end
  end

  defmodule PartitionedCache do
    @moduledoc false
    use Nebulex.Cache,
      otp_app: :nebulex_distributed,
      adapter: Nebulex.Adapters.Partitioned,
      adapter_opts: [primary_storage_adapter: Nebulex.Adapters.Local]

    use Commons
  end

  defmodule PartitionedCachex do
    @moduledoc false
    use Nebulex.Cache,
      otp_app: :nebulex_distributed,
      adapter: Nebulex.Adapters.Partitioned,
      adapter_opts: [primary_storage_adapter: Nebulex.Adapters.Cachex]

    use Commons
  end

  defmodule PartitionedNilCache do
    @moduledoc false
    use Nebulex.Cache,
      otp_app: :nebulex_distributed,
      adapter: Nebulex.Adapters.Partitioned,
      adapter_opts: [primary_storage_adapter: Nebulex.Adapters.Nil]

    use Commons
  end

  defmodule Multilevel do
    @moduledoc false
    use Nebulex.Cache,
      otp_app: :nebulex_distributed,
      adapter: Nebulex.Adapters.Multilevel

    use Commons

    defmodule L1 do
      @moduledoc false
      use Nebulex.Cache,
        otp_app: :nebulex_distributed,
        adapter: Nebulex.Adapters.Local
    end

    defmodule L2 do
      @moduledoc false
      use Nebulex.Cache,
        otp_app: :nebulex_distributed,
        adapter: Nebulex.Adapters.Local
    end

    defmodule L3 do
      @moduledoc false
      use Nebulex.Cache,
        otp_app: :nebulex_distributed,
        adapter: Nebulex.Adapters.Partitioned
    end
  end

  defmodule MultilevelNil do
    @moduledoc false
    use Nebulex.Cache,
      otp_app: :nebulex_distributed,
      adapter: Nebulex.Adapters.Multilevel

    use Commons

    defmodule L1 do
      @moduledoc false
      use Nebulex.Cache,
        otp_app: :nebulex_distributed,
        adapter: Nebulex.Adapters.Local
    end

    defmodule L2 do
      @moduledoc false
      use Nebulex.Cache,
        otp_app: :nebulex_distributed,
        adapter: Nebulex.Adapters.Nil
    end
  end

  defmodule MultilevelCachex do
    @moduledoc false
    use Nebulex.Cache,
      otp_app: :nebulex_distributed,
      adapter: Nebulex.Adapters.Multilevel

    use Commons

    defmodule L1 do
      @moduledoc false
      use Nebulex.Cache,
        otp_app: :nebulex_distributed,
        adapter: Nebulex.Adapters.Cachex
    end

    defmodule L2 do
      @moduledoc false
      use Nebulex.Cache,
        otp_app: :nebulex_distributed,
        adapter: Nebulex.Adapters.Cachex
    end

    defmodule L3 do
      @moduledoc false
      use Nebulex.Cache,
        otp_app: :nebulex_distributed,
        adapter: Nebulex.Adapters.Partitioned,
        adapter_opts: [primary_storage_adapter: Nebulex.Adapters.Cachex]
    end
  end
end
