package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
	"time"
)

type Json map[string]any

type Body struct {
	Message  int                 `json:"message"`
	Topology map[string][]string `json:"topology"`
}

type Messages struct {
	messages  []int
	muMessage sync.RWMutex
	status    map[int]bool
	muStatus  sync.RWMutex
}

func main() {
	n := maelstrom.NewNode()
	messages := Messages{
		messages:  make([]int, 0),
		muMessage: sync.RWMutex{},
		status:    make(map[int]bool),
		muStatus:  sync.RWMutex{},
	}
	topology := make(map[string][]string)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body Body
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		n.Reply(msg, Json{
			"type": "broadcast_ok",
		})

		if messages.messageExists(body.Message) {
			return nil
		}

		messages.appendMessage(body.Message)

		neighbourNodes := topology[msg.Dest]

		for _, node := range neighbourNodes {
			if node == msg.Src {
				continue
			}

			broadcastMessage(n, node, body)
		}

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(msg, Json{
			"type":     "read_ok",
			"messages": messages.getMessage(),
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

func broadcastMessage(n *maelstrom.Node, node string, body Body) error {
	ack := false
	ackMu := sync.Mutex{}

	for !ack {
		n.RPC(node, Json{
			"message": body.Message,
			"type":    "broadcast",
		}, func(msg maelstrom.Message) error {
			ackMu.Lock()
			defer ackMu.Unlock()
			ack = true
			return nil
		})

		time.Sleep(500 * time.Millisecond)
	}

	return nil
}

func (messages *Messages) messageExists(message int) bool {
	messages.muStatus.RLock()
	defer messages.muStatus.RUnlock()
	return messages.status[message]
}

func (messages *Messages) appendMessage(message int) {
	messages.muMessage.Lock()
	defer messages.muMessage.Unlock()
	messages.messages = append(messages.messages, message)

	messages.muStatus.Lock()
	defer messages.muStatus.Unlock()
	messages.status[message] = true
}

func (messages *Messages) getMessage() []int {
	messages.muMessage.RLock()
	defer messages.muMessage.RUnlock()
	return messages.messages
}
