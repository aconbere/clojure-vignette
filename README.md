# vignette

A clojure implementation of avibryant's [vignette](https://github.com/avibryant/vignette)

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


