defmodule HLS.MixProject do
  use Mix.Project

  @github_url "https://github.com/kim-company/kim_hls"

  def project do
    [
      app: :kim_hls,
      version: "3.0.5",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      source_url: @github_url,
      name: "HTTP Live Streaming (HLS) library",
      description: description(),
      package: package(),
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {HLS.Application, []},
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:telemetry, "~> 1.3", optional: true},
      {:req, "~> 0.5", only: :test, optional: true},
      {:plug, "~> 1.0", only: :test},
      {:benchee, "~> 1.3", only: :dev, runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      maintainers: ["KIM Keep In Mind"],
      files: ~w(lib mix.exs README.md LICENSE),
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @github_url}
    ]
  end

  defp description do
    """
    HTTP Live Streaming (HLS) library. Modules, variables and functionality is bound to RFC 8216.
    """
  end
end
