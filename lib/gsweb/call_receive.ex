defmodule Gsweb.CallReceive do
  @behaviour :cowboy_handler
  import Gsweb.Utils
  require Logger

  def init(req, state) do
    process_name = :cowboy_req.binding(:process_name, req)
    {:ok, body, req} = :cowboy_req.read_body(req)
    message = JSON.decode!(body)

    Logger.debug("/process/#{process_name}/call-receive #{inspect(message)}")

    _result = GenServer.call(resolve(process_name), message)

    req =
      :cowboy_req.stream_reply(
        200,
        %{
          "content-type" => "text/event-stream",
          "cache-control" => "no-cache",
          "connection" => "keep-alive"
        },
        req
      )

    loop(
      req,
      fn message ->
        json_message = JSON.encode!(message)

        Logger.debug(json_message)

        frame = "data: #{json_message}\n\n"

        :ok = :cowboy_req.stream_body(frame, :nofin, req)
      end
    )

    {:ok, req, state}
  end
end
