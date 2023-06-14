package main

import (
	"encoding/json"
	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

type msgStore struct {
	valStore []float64
	mu       sync.Mutex
}

type Message struct {
	n    *maelstrom.Node
	dest string
	val  float64
}

func main() {
	var msgStore msgStore
	var neighbourNodes []string
	var processedMsgs []string
	valChan := make(chan float64)
	retryChan := make(chan Message)

	n := maelstrom.NewNode()

	// background housekeepers
	go func() {
		for {
			select {
			case v := <-valChan:
				// msgStore updater
				msgStore.mu.Lock()
				msgStore.valStore = append(msgStore.valStore, v)
				msgStore.mu.Unlock()
			case m := <-retryChan:
				// msg retrier
				err := retryMessage(m, retryChan)
				if err != nil {
					log.Println("some error on retry: ", err)
				}
			}
		}
	}()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		return recvBroadcast(n, msg, valChan, neighbourNodes, &processedMsgs, retryChan)
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		return readHandler(n, msg, &msgStore)
	})
	n.Handle("topology", func(msg maelstrom.Message) error {
		return topologyHandler(n, msg, &neighbourNodes)
	})

	err := n.Run()
	if err != nil {
		log.Fatal(err)
	}
	close(valChan)
}

func topologyHandler(n *maelstrom.Node, msg maelstrom.Message, nodes *[]string) error {
	body := map[string]any{}

	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}
	log.Println("bodice", body)

	aInterfaces := body["topology"].(map[string]any)[n.ID()].([]any)
	var strs []string
	for _, s := range aInterfaces {
		strs = append(strs, s.(string))
	}
	*nodes = strs

	err = n.Reply(msg, map[string]string{"type": "topology_ok"})
	if err != nil {
		return err
	}

	log.Println("huks: ", *nodes)
	return nil
}

func readHandler(n *maelstrom.Node, msg maelstrom.Message, store *msgStore) error {
	res := map[string]any{}
	res["type"] = "read_ok"
	store.mu.Lock()
	res["messages"] = store.valStore
	store.mu.Unlock()

	err := n.Reply(msg, res)
	if err != nil {
		return err
	}
	return nil
}

func recvBroadcast(n *maelstrom.Node, msg maelstrom.Message, valChan chan float64, neighbours []string, processedMsgs *[]string, retryChan chan Message) error {
	body, err := extractBody(msg)
	if err != nil {
		return err
	}

	var id string
	// if node src
	if msg.Src[0] == 'n' {
		id = body["mid"].(string)
		for _, s := range *processedMsgs {
			if s == id {
				err = n.Reply(msg, map[string]string{"type": "broadcast_ok"})
				if err != nil {
					return err
				}
				return nil
			}
		}
	} else {
		id = uuid.NewString()
	}

	val := body["message"].(float64)
	valChan <- val // send msg for storage

	// notify neighbours
	// todo(): don't send to src
	for _, destNode := range neighbours {
		err := n.RPC(destNode, map[string]any{"type": "broadcast", "message": val, "mid": id}, func(msg maelstrom.Message) error {
			body, err := extractBody(msg)
			if err != nil {
				return err
			}

			if body["type"].(string) != "broadcast_ok" {
				msgData := Message{
					n:    n,
					dest: destNode,
					val:  val,
				}
				retryChan <- msgData
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	err = n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	*processedMsgs = append(*processedMsgs, id)
	if err != nil {
		return err
	}
	return nil
}

func extractBody(msg maelstrom.Message) (map[string]any, error) {
	body := map[string]any{}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func retryMessage(msgData Message, retryChan chan Message) error {
	err := msgData.n.RPC(msgData.dest, msgData.val, func(msg maelstrom.Message) error {
		body, err := extractBody(msg)
		if err != nil {
			return err
		}

		if body["type"].(string) != "broadcast_ok" {
			retryChan <- msgData
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
