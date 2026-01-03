defmodule Gsweb.Call do
  @behaviour :cowboy_handler

  def init(req, state) do
    process_name = :cowboy_req.binding(:process_name, req)
    {:ok, body, req} = :cowboy_req.read_body(req)
    message = JSON.decode!(body)

    IO.puts("/process/#{process_name}/call #{inspect(message)}")

    result = GenServer.call({:global, process_name}, message)

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
