defmodule Gsweb.Application do
  @moduledoc """
  Start the GenServer web/http REST server
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Gsweb.HTTPServer,
      {KV, name: {:global, :kv1}},
      {PS, name: {:global, :ps1}}
    ]

    opts = [strategy: :one_for_one, name: Gsweb.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
