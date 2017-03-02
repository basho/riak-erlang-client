defmodule Riakc.Mixfile do
  use Mix.Project

  @version File.read!("VERSION") |> String.strip

  def project do
    [app: :riakc,
     version: @version,
     description: "The Riak client for Erlang",
     package: package(),
     deps: deps(),
     erlc_options: erlc_options()]
  end

  defp erlc_options do
    otp_rel = :erlang.list_to_integer(:erlang.system_info(:otp_release))

    o1 = try do
      case otp_rel do
        v when v >= 17 -> [{:d, :namespaced_types}]
        _ -> []
      end
    catch
      _ -> []
    end

    o2 = try do
      case otp_rel do
        v when v >= 18 -> [{:d, :deprecated_now}]
        _ -> []
      end
    catch
      _ -> []
    end

    o3 = try do
      case otp_rel do
        v when v >= 19 -> [{:d, :deprecated_19}]
        _ -> []
      end
    catch
      _ -> []
    end

    extra_options = o1 ++ o2 ++ o3

    [:debug_info, :warnings_as_errors | extra_options]
  end

  defp deps do
    [
      {:riak_pb, "~> 2.3"},
      {:ex_doc, ">= 0.0.0", only: :dev}
    ]
  end

  defp package do
    [files: ~w(include src mix.exs LICENSE Makefile README.md RELNOTES.md rebar.config rebar.config.script tools.mk tools test priv VERSION),
     maintainers: ["Drew Kerrigan", "Luke Bakken", "Alex Moore"],
     licenses: ["Apache 2.0"],
     links: %{"GitHub" => "https://github.com/basho/riak-erlang-client"}]
  end
end
