defmodule Gsweb.Up do
  @behaviour :cowboy_handler

  def init(req, state) do
    new_req = :cowboy_req.reply(200, %{"content-type" => "text/plain"}, "OK\r\n", req)

    {:ok, new_req, state}
  end
end
