defmodule Gsweb.HTTPServer do
  use GenServer
  require Logger

  @moduledoc """
  A REST interface to GenServer call and cast
  with variants to receive incoming messages
  and broadcast them as server-sent events
  """

  @one_day_ms 24 * 60 * 60 * 1_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def init(_opts) do
    dispatch =
      :cowboy_router.compile([
        {:_,
         [
           {"/up", Gsweb.Up, []},
           {"/process/:process_name/call", Gsweb.Call, []},
           {"/process/:process_name/call-receive", Gsweb.CallReceive, []},
           {"/process/:process_name/cast", Gsweb.Call, []},
           {"/process/:process_name/cast-receive", Gsweb.CallReceive, []}
         ]}
      ])

    {:ok, _} =
      :cowboy.start_clear(
        :http,
        [port: 4000],
        %{
          idle_timeout: @one_day_ms,
          env: %{dispatch: dispatch}
        }
      )

    Logger.notice("Cowboy ready at http://localhost:4000")

    {:ok, %{}}
  end
end
