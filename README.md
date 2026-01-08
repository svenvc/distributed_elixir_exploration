# Exploring Distributed Elixir

See the following blog:

- https://blog.stfx.eu/2025-12-29-exploring-distributed-elixer.html


Setting up a cluster

```
$ iex --sname zed -S mix

$ iex --sname core -S mix run --no-start

iex(zed@pathfinder)> Node.connect :core@pathfinder
```

Inspecting the cluster

```
Node.list

:global.registered_names
```

Starting the demo GenServers.

```
GenServer.start(KV, nil, name: {:global, :kv1})

GenServer.start(PS, nil, name: {:global, :ps1})
```

Using the key-value server

```
GenServer.call({:global :kv1}, {:set, :foo, 42})

GenServer.call({:global, kv1}, {:get, :foo})
```

Using the publish-subscribe server

```
GenServer.call({:global, ps1}, {:subscribe, "topic1"})

GenServer.call({:global, ps1}, {:broadcast, "topic1", [:msg, "hi"]})
```

REST interface

```
$ curl http://localhost:4000/process/kv1/call -d '["get", "foo"]'    

$ curl http://localhost:4000/process/kv1/call -d '["set", "foo", 42]'

$ curl -N http://localhost:4000/process/ps1/call-receive -d '["subscribe", "topic1"]'

$ curl http://localhost:4000/process/ps1/call -d '["broadcast", "topic1", ["msg", "hi"]]'
```
