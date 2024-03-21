package main

import (
	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(message maelstrom.Message) error {
		body := make(map[string]any)
		body["type"] = "generate_ok"

		id := uuid.New()
		body["id"] = id

		return n.Reply(message, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
