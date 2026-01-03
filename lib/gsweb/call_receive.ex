defmodule Gsweb.CallReceive do
  @behaviour :cowboy_handler

  def init(req, state) do
    process_name = :cowboy_req.binding(:process_name, req)
    {:ok, body, req} = :cowboy_req.read_body(req)
    message = JSON.decode!(body)

    IO.puts("/process/#{process_name}/call-receive #{inspect(message)}")

    _result = GenServer.call({:global, process_name}, message)

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

    loop(req)

    {:ok, req, state}
  end

  defp loop(req) do
    receive do
      {:cowboy_req, :terminate} ->
        :ok

      message ->
        frame = "data: #{JSON.encode!(message)}\n\n"
        :ok = :cowboy_req.stream_body(frame, :nofin, req)
        loop(req)
    end
  end
end
