package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"time"
)

type Json map[string]any

type Body struct {
	Type  string `json:"type"`
	Delta int    `json:"delta"`
}

const KEY = "counter"

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		var body Body
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		writeValue(kv, body.Delta)

		return n.Reply(msg, Json{
			"type": "add_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body Body
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		return n.Reply(msg, Json{
			"type":  "read_ok",
			"value": readValue(kv),
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func readValue(kv *maelstrom.KV) int {
	value, err := kv.Read(context.Background(), KEY)
	if err == nil {
		return value.(int)
	}

	if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
		err := compareAndSwap(kv, 0, 0)
		if err != nil {
			value, _ := kv.Read(context.Background(), KEY)
			return value.(int)
		}

		return 0
	}

	panic("This should not happen")
}

func writeValue(kv *maelstrom.KV, delta int) {
	value := readValue(kv)

	err := compareAndSwap(kv, value, value+delta)
	if err != nil {
		time.Sleep(500 * time.Millisecond)
		writeValue(kv, delta)
	}

	return
}

func compareAndSwap(kv *maelstrom.KV, from, to int) error {
	err := kv.CompareAndSwap(context.Background(), KEY, from, to, true)
	if err == nil {
		return nil
	}

	if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
		return err
	}

	panic("This should not happen")
}
