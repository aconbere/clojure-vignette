# vignette

A clojure implementation of avibryant's [vignette](https://github.com/avibryant/vignette). Most of this documentation will be a retelling of the documenation there specific to this language and implementation.

Vignette is a simple distributed, eventually consistent, sketch database.

## Data Model

The database is a simple map from keys to a sparse vector of integers.


```clojure
{"foo" {0 5, 1 10, 5 3}}
```

values in the database are only modifiable via element-wise max.

sending the update `{"foo" {0 3, 1 11}}` would result in the new vector `{"foo" {0 5, 1 11, 5 3}}` being stored.

## Communication

Vignette communicates via messages sent encoded as msgpack over udp.

```clojure
{"key" "foo" "vector" {0 8, 3 7} "full" true}
```

Nodes in the network communicate via a form of gossip communication.

A vignette node should respond to a receiving a message from a given sender by.

* If the node has not seen the key pick a random collection of known nodes and ask what the current state of the key is.
* If the node has seen the key. Apply the element-wise max to the vector for that key. Forward any changes to a random sample of known neighbors.
* If the not seen the key store it. And...
  * if the message indicates that this is a full message you're done.
  * Otherwise, Query a random collection of nodes for the current state of that key.

## Presence

In order to form the network each Vignette node announces itself to at least one known node when coming online by sending a storage message of the form `{ "n:<host>:<port> { 0 <timestamp> }}`. This is sufficient to percolate this knowledge to much of the network. Every time t nodes will rebroadcast their presence to a sample of their known nodes. It should be expected that nodes that have not heard from eachother in sufficient time will stop sending updates that direction.

## Usage

```bash
$ lein run <port> [<host>:port]
```

## Run Examples

```bash
$ lein run 6000
$ lein run 6001 127.0.0.1:6000
$ lein run 6002 127.0.0.1:6000 127.0.0.1:6001
```

At this point the nodes will all be talking to each other letting eachother know they're there. It looks pretty chatty, but it's not too bad (yay udp!)

from the repl

```clojure
(use 'vignette.examples)
(def s (example-server 7000))
(example-run s)
(example-run s)
(example-run s)
```

At that point you'll notice a ton of chatter at the network figures itself out.


