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
    [ {:emysql, github: "Eonblast/Emysql"},
      {:inutils, git: "git@git.maxbet.asia:elixir/inutils.git"},
      {:decimal, github: "ericmj/decimal", tag: "0676c03d8460809db400c7c3dbaef65adc543721"} ]
      
  end
end
