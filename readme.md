# Gossip glomers

A series of distributed systems challenges. By [fly.io](https://fly.io/dist-sys/)

## [Challenge #1: Echo](https://fly.io/dist-sys/1/)

```shell
go install ./echo

./maelstrom/maelstrom test -w echo --bin ~/go/bin/echo --node-count 1 --time-limit 10
```

## [Challenge #2: Unique ID Generation](https://fly.io/dist-sys/2/)

```shell
go install ./unique-ids

./maelstrom/maelstrom test -w unique-ids --bin ~/go/bin/unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

```