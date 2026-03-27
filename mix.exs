defmodule NebulexDistributed.MixProject do
  use Mix.Project

  @source_url "https://github.com/elixir-nebulex/nebulex_distributed"
  @version "3.1.0"

  def project do
    [
      app: :nebulex_distributed,
      version: @version,
      elixir: "~> 1.14",
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      deps: deps(),

      # Testing
      test_coverage: [tool: ExCoveralls, export: "test-coverage"],

      # Dialyzer
      dialyzer: dialyzer(),

      # Usage Rules
      usage_rules: usage_rules(),

      # Hex
      package: package(),
      description: "Distributed cache topologies adapters for Nebulex",

      # Docs
      name: "Nebulex.Distributed",
      docs: docs()
    ]
  end

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.json": :test,
        "test.ci": :test
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [],
      mod: {Nebulex.Distributed.Application, []}
    ]
  end

  defp deps do
    [
      nebulex_dep(),
      {:nebulex_local, "~> 3.0"},
      {:nebulex_streams, "~> 0.2"},
      {:ex_hash_ring, "~> 6.0 or ~> 7.0"},
      {:partitioned_buffer, "~> 0.4"},
      {:telemetry, "~> 0.4 or ~> 1.0", optional: true},

      # Test & Code Analysis
      {:excoveralls, "~> 0.18", only: :test},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.14", only: [:dev, :test], runtime: false},
      {:stream_data, "~> 1.2", only: [:dev, :test]},
      {:mimic, "~> 2.0", only: :test},
      {:ex2ms, "~> 1.7", only: :test},

      # Benchmark Test
      {:benchee, "~> 1.5", only: [:dev, :test]},
      {:benchee_html, "~> 1.0", only: [:dev, :test]},

      # Usage Rules
      {:usage_rules, "~> 1.0", only: [:dev]},

      # Docs
      {:ex_doc, "~> 0.40", only: [:dev, :test], runtime: false}
    ]
  end

  defp nebulex_dep do
    if path = System.get_env("NEBULEX_PATH") do
      {:nebulex, path: path, override: true}
    else
      {:nebulex, "~> 3.0"}
    end
  end

  defp aliases do
    [
      "nbx.setup": [
        "cmd rm -rf nebulex",
        "cmd git clone --depth 1 --branch main https://github.com/elixir-nebulex/nebulex"
      ],
      "test.ci": [
        "deps.unlock --check-unused",
        "compile --warnings-as-errors",
        "format --check-formatted",
        "credo --strict",
        "sobelow --skip --exit Low",
        "coveralls.html",
        "dialyzer --format short"
      ],
      "ur.sync": ["usage_rules.sync"]
    ]
  end

  defp package do
    [
      name: :nebulex_distributed,
      maintainers: [
        "Carlos Bolanos"
      ],
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib .formatter.exs mix.exs README* CHANGELOG* LICENSE*)
    ]
  end

  defp docs do
    [
      main: "Nebulex.Distributed",
      source_ref: "v#{@version}",
      source_url: @source_url,
      canonical: "https://hexdocs.pm/nebulex_distributed",
      groups_for_modules: [
        # Nebulex.Distributed

        "Built-in adapters": [
          Nebulex.Adapters.Coherent,
          Nebulex.Adapters.Multilevel,
          Nebulex.Adapters.Partitioned,
          Nebulex.Adapters.Replicated
        ],
        Utilities: [
          Nebulex.Distributed.Cluster,
          Nebulex.Distributed.RPC,
          Nebulex.Distributed.RingMonitor,
          Nebulex.Distributed.Transaction
        ]
      ]
    ]
  end

  defp dialyzer do
    [
      plt_add_apps: [:nebulex, :mix],
      plt_file: {:no_warn, "priv/plts/" <> plt_file_name()},
      flags: [
        :unmatched_returns,
        :error_handling,
        :extra_return,
        :no_opaque,
        :no_return
      ]
    ]
  end

  defp plt_file_name do
    "dialyzer-#{Mix.env()}-#{System.version()}-#{System.otp_release()}.plt"
  end

  defp usage_rules do
    [
      # The file to write usage rules into (required for usage_rules syncing)
      file: "AGENTS.md",

      # rules to include directly in AGENTS.md
      usage_rules: [
        {:nebulex,
         [
           sub_rules: [
             "workflow",
             "nebulex",
             "elixir-style",
             "elixir"
           ]
         ]},
        :otp
      ],

      # Agent skills configuration
      skills: [
        # Auto-build a "use-<pkg>" skill per dependency
        deps: [:nebulex]
      ]
    ]
  end
end
