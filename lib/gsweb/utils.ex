defmodule Gsweb.Utils do
  require Logger

  def resolve(process_name) do
    :global.whereis_name(process_name) || :global.whereis_name(String.to_atom(process_name))
  end

  @heartbeat_ms 60_000

  def loop(req, handler) do
    receive do
      {:cowboy_req, :terminate} ->
        :ok

      message ->
        handler.(message)

        loop(req, handler)
    after
      @heartbeat_ms ->
        handler.(["heartbeat", to_string(DateTime.utc_now())])

        loop(req, handler)
    end
  end
end
