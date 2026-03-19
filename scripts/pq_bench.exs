queue_name = :pq
worker_name = :pqw

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

count = 1_000

IO.puts("\nPersistent Queue Benchmark\nSending and consuming #{count} messages")

{us, _} = :timer.tc(fn ->
  1..count
  |> Enum.each(fn i ->
    PQ.enqueue(queue, %{"index" => i, "extra" => "this is a benchmark", "test" => true})
  end)
end)

IO.puts("#{count} messages took #{us/1_000_000} s")
IO.puts("#{us/count} μs/msg #{Float.round(count/us*1_000_000, 2)} msg/s")

Process.sleep(100)

IO.inspect(PQ.empty?(queue), label: "Queue empty ?")

PQWorker.stop(worker)
PQ.stop(queue)
