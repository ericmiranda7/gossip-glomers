package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

type topologyMsg struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func main() {
	n := maelstrom.NewNode()
	mu := sync.Mutex{}
	storedMsgs := make(map[int]bool)
	var neighbours []string

	n.Handle("broadcast", func(message maelstrom.Message) error {
		var body map[string]any

		err := json.Unmarshal(message.Body, &body)
		if err != nil {
			return err
		}

		// safely store
		val := int(body["message"].(float64))
		mu.Lock()
		if _, exists := storedMsgs[val]; !exists {
			storedMsgs[val] = true
		}
		mu.Unlock()

		// todo(): use src, dest

		// send the message out only to non-SRC neighbours
		broadcastMsg := make(map[string]any)
		broadcastMsg["type"] = "broadcast"
		broadcastMsg["message"] = val
		for _, neighbour := range neighbours {
			if neighbour != message.Src {
				err := n.Send(neighbour, broadcastMsg)
				if err != nil {
					return err
				}
			}
		}

		if message.Src[0] == 'c' {
			res := make(map[string]string)
			res["type"] = "broadcast_ok"
			return n.Reply(message, res)
		}
		return nil
	})

	n.Handle("read", func(message maelstrom.Message) error {
		res := make(map[string]any)
		res["type"] = "read_ok"

		var msgsList []int
		mu.Lock()
		for k := range storedMsgs {
			msgsList = append(msgsList, k)
		}
		mu.Unlock()

		res["messages"] = msgsList

		return n.Reply(message, res)
	})

	n.Handle("topology", func(message maelstrom.Message) error {
		body := topologyMsg{}
		if err := json.Unmarshal(message.Body, &body); err != nil {
			return err
		}

		// 1. parse the topology (who can talk to who)
		//		a. get topology object
		//		b. get neighbours for current node
		neighbours = body.Topology[n.ID()]

		// 2. nodes can only talk to neighbours

		res := make(map[string]any)
		res["type"] = "topology_ok"
		return n.Reply(message, res)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
