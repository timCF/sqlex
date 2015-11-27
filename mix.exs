defmodule Sqlex.Mixfile do
  use Mix.Project

  def project do
    [ app: :sqlex,
      version: "0.0.1",
      build_per_environment: false,
      deps: deps ]
  end

  # Configuration for the OTP application
  def application do
    [
      applications: [:emysql]
    ]
  end

  # Returns the list of dependencies in the format:
  # { :foobar, "0.1", git: "https://github.com/elixir-lang/foobar.git" }
  defp deps do
    [
      {:emysql, github: "timCF/Emysql"},
    ]

  end
end
