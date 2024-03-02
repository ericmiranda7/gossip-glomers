package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("echo", func(message maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(message.Body, &body); err != nil {
			return err
		}

		body["type"] = "echo_ok"

		return n.Reply(message, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
