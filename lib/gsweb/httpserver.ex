defmodule Gsweb.HTTPServer do
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def init(_opts) do
    dispatch =
      :cowboy_router.compile([
        {:_,
         [
           {"/up", Gsweb.Up, []}
         ]}
      ])

    {:ok, _} =
      :cowboy.start_clear(
        :http,
        [port: 4000],
        %{env: %{dispatch: dispatch}}
      )

    {:ok, %{}}
  end
end
