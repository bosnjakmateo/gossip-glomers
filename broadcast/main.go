package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"strings"
)

type Json map[string]any

type Body struct {
	Message  int                 `json:"message"`
	Topology map[string][]string `json:"topology"`
}

func main() {
	n := maelstrom.NewNode()

	messages := make([]int, 0)
	messagesMap := make(map[int]bool)
	topology := make(map[string][]string)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body Body
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if messagesMap[body.Message] == true {
			return nil
		}

		messages = append(messages, body.Message)
		messagesMap[body.Message] = true

		neighbourNodes := topology[msg.Dest]

		for _, node := range neighbourNodes {
			n.Send(node, Json{
				"message": body.Message,
				"type":    "broadcast",
			})
		}

		if broadcastReceivedFromNeighbour(msg) {
			return nil
		}

		return n.Reply(msg, Json{
			"type": "broadcast_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(msg, Json{
			"type":     "read_ok",
			"messages": messages,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body Body
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology = body.Topology

		return n.Reply(msg, Json{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func broadcastReceivedFromNeighbour(msg maelstrom.Message) bool {
	return strings.Contains(msg.Src, "n")
}
