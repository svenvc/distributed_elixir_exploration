defmodule Gsweb.Call do
  @behaviour :cowboy_handler
  import Gsweb.Utils
  require Logger

  def init(req, state) do
    process_name = :cowboy_req.binding(:process_name, req)
    {:ok, body, req} = :cowboy_req.read_body(req)
    message = JSON.decode!(body)

    Logger.debug("/process/#{process_name}/call #{inspect(message)}")

    result = GenServer.call(resolve(process_name), message)

    req =
      :cowboy_req.reply(
        200,
        %{"content-type" => "application/json"},
        JSON.encode!(result) <> "\r\n",
        req
      )

    {:ok, req, state}
  end
end
