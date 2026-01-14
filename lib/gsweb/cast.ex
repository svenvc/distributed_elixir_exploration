defmodule Gsweb.Cast do
  @behaviour :cowboy_handler
  import Gsweb.Utils
  require Logger

  def init(req, state) do
    process_name = :cowboy_req.binding(:process_name, req)
    {:ok, body, req} = :cowboy_req.read_body(req)
    message = JSON.decode!(body)

    Logger.debug("/process/#{process_name}/cast #{inspect(message)}")

    :ok = GenServer.cast(resolve(process_name), message)

    req =
      :cowboy_req.reply(
        200,
        %{"content-type" => "application/json"},
        "true\r\n",
        req
      )

    {:ok, req, state}
  end
end
