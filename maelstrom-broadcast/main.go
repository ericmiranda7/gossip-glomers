package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

func main() {
	var valStore []float64
	var mu sync.Mutex

	n := maelstrom.NewNode()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		return recvBroadcast(n, msg, &valStore, &mu)
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		return readHandler(n, msg, valStore)
	})
	n.Handle("topology", func(msg maelstrom.Message) error {
		return topologyHandler(n, msg)
	})

	err := n.Run()
	if err != nil {
		log.Fatal(err)
	}

}

func topologyHandler(n *maelstrom.Node, msg maelstrom.Message) error {
	body := map[string]any{}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}

	//nodes := body["topology"].(map[string][]string)[n.ID()]

	err = n.Reply(msg, map[string]string{"type": "topology_ok"})
	if err != nil {
		return err
	}

	return nil
}

func readHandler(n *maelstrom.Node, msg maelstrom.Message, store []float64) error {
	res := map[string]any{}
	res["type"] = "read_ok"
	res["messages"] = store

	err := n.Reply(msg, res)
	if err != nil {
		return err
	}
	return nil
}

func recvBroadcast(n *maelstrom.Node, msg maelstrom.Message, valStore *[]float64, mu *sync.Mutex) error {
	body := map[string]any{}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}

	val := body["message"].(float64)

	mu.Lock()
	*valStore = append(*valStore, val)
	mu.Unlock()

	err = n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	if err != nil {
		return err
	}

	return nil
}
