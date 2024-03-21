package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

func main() {
	n := maelstrom.NewNode()
	var storedMsgs []int

	n.Handle("broadcast", func(message maelstrom.Message) error {
		var body map[string]any

		err := json.Unmarshal(message.Body, &body)
		if err != nil {
			return err
		}

		storedMsgs = append(storedMsgs, int(body["message"].(float64)))

		res := make(map[string]string)
		res["type"] = "broadcast_ok"
		return n.Reply(message, res)
	})

	/*
		{
		  "type": "read_ok",
		  "messages": [1, 8, 72, 25]
		}
	*/
	n.Handle("read", func(message maelstrom.Message) error {
		res := make(map[string]any)
		res["type"] = "read_ok"
		res["messages"] = storedMsgs

		return n.Reply(message, res)
	})

	n.Handle("topology", func(message maelstrom.Message) error {
		res := make(map[string]any)
		res["type"] = "topology_ok"
		return n.Reply(message, res)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
