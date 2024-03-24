package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"os"
	"sync"
	"time"
)

type topologyMsg struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type storedMsgMap struct {
	mu           *sync.Mutex
	storedMsgMap map[int]bool
}

type failedMsgs struct {
	mu     *sync.Mutex
	msgMap map[string][]retryMsg
}

type retryMsg struct {
	src          string
	neighbours   *[]string
	n            *maelstrom.Node
	broadcastMsg map[string]any
}

func main() {
	logger := log.New(os.Stderr, "", 0)
	n := maelstrom.NewNode()
	storedMsgs := storedMsgMap{&sync.Mutex{}, make(map[int]bool)}
	fm := failedMsgs{
		mu:     &sync.Mutex{},
		msgMap: make(map[string][]retryMsg),
	}
	var neighbours []string

	// retry routine
	go func() {
		for {
			time.Sleep(5 * time.Second)

			fm.mu.Lock()
			for _, retryMsgList := range fm.msgMap {
				for _, msg := range retryMsgList {
					logger.Println("RETRYING_MSG", msg)
					err := sendBroadcastMsg(msg, fm, logger)
					if err != nil {
						return
					}
				}
			}
			fm.mu.Unlock()
		}
	}()

	n.Handle("broadcast", func(request maelstrom.Message) error {
		// 0. by default, add all msgs to retry list
		// 1. use RPC instead of send for propating msgs
		// 2. create a list of msgs that did not receive ack
		// 3. retry msgs in above list every x seconds

		var body map[string]any

		err := json.Unmarshal(request.Body, &body)
		if err != nil {
			return err
		}

		// safely store
		val := int(body["message"].(float64))
		storedMsgs.mu.Lock()
		if _, exists := storedMsgs.storedMsgMap[val]; !exists {
			storedMsgs.storedMsgMap[val] = true
		}
		storedMsgs.mu.Unlock()

		// todo(): use src, dest

		// send the message out only to nochan retryMsgn-SRC neighbours
		broadcastMsg := make(map[string]any)
		broadcastMsg["type"] = "broadcast"
		broadcastMsg["message"] = val
		rm := retryMsg{
			src:          request.Src,
			neighbours:   &neighbours,
			n:            n,
			broadcastMsg: broadcastMsg,
		}
		err = sendBroadcastMsg(rm, fm, logger)
		if err != nil {
			return err
		}

		var res map[string]any
		if request.Src[0] == 'c' {
			res = make(map[string]any)
			res["type"] = "broadcast_ok"
		} else {
			res = make(map[string]any)
			res["value"] = val
		}
		return n.Reply(request, res)
	})

	n.Handle("read", func(message maelstrom.Message) error {
		res := make(map[string]any)
		res["type"] = "read_ok"

		var msgsList []int
		for k := range storedMsgs.storedMsgMap {
			msgsList = append(msgsList, k)
		}

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
		logger.Println("Neighbours are", neighbours)

		// 2. nodes can only talk to neighbours

		res := make(map[string]any)
		res["type"] = "topology_ok"
		return n.Reply(message, res)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func sendBroadcastMsg(rm retryMsg, fm failedMsgs, logger *log.Logger) error {
	if len(*rm.neighbours) == 0 {
		panic("no neighbours")
	}

	for _, neighbour := range *rm.neighbours {
		if neighbour == rm.src {
			continue
		}

		err := rm.n.RPC(neighbour, rm.broadcastMsg, func(response maelstrom.Message) error {
			var body map[string]any

			err := json.Unmarshal(response.Body, &body)
			if err != nil {
				return err
			}
			resVal := int(body["value"].(float64))
			var newRetryList []retryMsg

			fm.mu.Lock()
			logger.Printf("Received response for value %d with src %s and dest %s", resVal, response.Src, response.Dest)
			logger.Println("Msglist B4 looks like: ", fm.msgMap[response.Src])
			for _, msg := range fm.msgMap[response.Src] {
				if msg.broadcastMsg["message"].(int) == resVal {
					logger.Println("removed value", resVal)
					continue
				}
				newRetryList = append(newRetryList, msg)
			}
			fm.msgMap[response.Src] = newRetryList
			logger.Println("Msglist AFTER looks like: ", fm.msgMap[response.Src])
			fm.mu.Unlock()

			return nil
		})

		fm.mu.Lock()
		// todo(): optimise addition
		fm.msgMap[neighbour] = append(fm.msgMap[neighbour], rm)
		fm.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}
