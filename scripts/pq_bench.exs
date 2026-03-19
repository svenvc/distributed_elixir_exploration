queue_name = :pq
worker_name = :pqw

File.rm_rf!("pq")

{:ok, worker} =
  PQWorker.start(
    queue_name: queue_name,
    name: worker_name,
    handler_function: fn _msg -> :ack end
  )

{:ok, queue} =
  PQ.start(
    name: queue_name,
    delegate: worker
  )

count = System.get_env("COUNT", "1000") |> String.to_integer()

IO.puts("\nPersistent Queue Benchmark\nSending and consuming #{count} messages")

defmodule Util do
  def enqueue(q, m, t \\ 100) do
    if !PQ.enqueue(q, m) do
      if t >= 0 do
        Process.sleep(10)
        enqueue(q, m, t - 10)
      else
        raise "timed_out"
      end
    end
  end
end

{us, _} =
  :timer.tc(fn ->
    0..(count - 1)
    |> Enum.each(fn i ->
      Util.enqueue(
        queue,
        %{"index" => i, "extra" => "this is a benchmark", "test" => true}
      )
    end)
  end)

IO.puts("#{count} messages took #{us / 1_000_000} s")
IO.puts("#{Float.round(us / count, 2)} μs/msg #{Float.round(count / us * 1_000_000, 2)} msg/s")

Process.sleep(100)

IO.inspect(PQ.empty?(queue), label: "Queue empty ?")

PQWorker.stop(worker)
PQ.stop(queue)
