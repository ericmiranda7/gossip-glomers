package main

import (
	"encoding/json"
	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
	"time"
)

type Server struct {
	n              *maelstrom.Node
	neighbourNodes []string
	valStore       []float64
	valChan        chan float64
	msgChan        chan customMessage
	msgQ           []customMessage
	processedMsgs  []string
	delMsgChan     chan string
	mu             sync.Mutex
}

type topologyMsg struct {
	Topology map[string][]string `json:"topology"`
}

type customMessage struct {
	id   string
	n    *maelstrom.Node
	dest string
	val  any
}

func main() {
	n := maelstrom.NewNode()
	s := Server{
		n:              n,
		neighbourNodes: []string{},
		valStore:       []float64{},
		valChan:        make(chan float64),
		msgChan:        make(chan customMessage),
		msgQ:           []customMessage{},
		processedMsgs:  []string{},
		delMsgChan:     make(chan string),
		mu:             sync.Mutex{},
	}

	s.n.Handle("broadcast", s.recvBroadcast)
	s.n.Handle("read", s.readHandler)
	s.n.Handle("topology", s.topologyHandler)
	s.n.Handle("broadcast_fine", s.broadcastOk)

	s.start()
}

func (s *Server) start() {
	// background housekeepers
	go func() {
		for {
			select {
			case v := <-s.valChan:
				// msgStore updater
				s.mu.Lock()
				s.valStore = append(s.valStore, v)
				s.mu.Unlock()
			case m := <-s.msgChan:
				// msg sender
				s.msgQ = append(s.msgQ, m)
				err := sendMessage(m)
				if err != nil {
					log.Println("some error on send: ", err)
				}
			case dm := <-s.delMsgChan:
				// todo() make simple?
				delInd := -1
				for i, e := range s.msgQ {
					if e.id == dm {
						delInd = i
					}
				}
				if delInd == -1 {
					break
				}
				s.msgQ[delInd] = s.msgQ[len(s.msgQ)-1]
				s.msgQ = s.msgQ[:len(s.msgQ)-1]
			}
		}
	}()

	go func() {
		for {
			time.Sleep(time.Second * 5)
			for _, m := range s.msgQ {
				err := sendMessage(m)
				if err != nil {
					log.Println("some error on send: ", err)
				}
			}
		}
	}()

	err := s.n.Run()
	if err != nil {
		log.Fatal(err)
	}
}

func (s *Server) recvBroadcast(msg maelstrom.Message) error {
	body, err := extractBody(msg)
	if err != nil {
		return err
	}

	var id string
	// if node src
	if msg.Src[0] == 'n' {
		id = body["mid"].(string)
		for _, pm := range s.processedMsgs {
			if pm == id {
				return s.n.Reply(msg, map[string]string{"type": "broadcast_fine", "mid": id})
			}
		}
	} else {
		id = uuid.NewString()
	}

	val := body["message"].(float64)
	s.valChan <- val // send msg for storage

	// notify neighbours
	for _, destNode := range s.neighbourNodes {
		if destNode == msg.Src {
			continue
		}
		msgData := customMessage{
			id:   id,
			n:    s.n,
			dest: destNode,
			val:  map[string]any{"type": "broadcast", "message": val, "mid": id},
		}
		s.msgChan <- msgData
	}

	if msg.Src[0] == 'c' {
		err = s.n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	} else {
		err = s.n.Reply(msg, map[string]string{"type": "broadcast_fine", "mid": id})
	}
	s.processedMsgs = append(s.processedMsgs, id)
	return err
}

func (s *Server) broadcastOk(msg maelstrom.Message) error {
	body, err := extractBody(msg)
	if err != nil {
		return err
	}

	s.delMsgChan <- body["mid"].(string)

	return nil
}

func (s *Server) topologyHandler(msg maelstrom.Message) error {
	var tbody topologyMsg

	err := json.Unmarshal(msg.Body, &tbody)
	if err != nil {
		return err
	}

	topology := tbody.Topology[s.n.ID()]
	s.neighbourNodes = topology

	err = s.n.Reply(msg, map[string]string{"type": "topology_ok"})
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) readHandler(msg maelstrom.Message) error {
	res := map[string]any{}
	res["type"] = "read_ok"
	// todo(): since its a slice the below isn't really copied
	s.mu.Lock()
	res["messages"] = s.valStore
	s.mu.Unlock()

	return s.n.Reply(msg, res)
}

func extractBody(msg maelstrom.Message) (map[string]any, error) {
	body := map[string]any{}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func sendMessage(msgData customMessage) error {
	err := msgData.n.Send(msgData.dest, msgData.val)
	return err
}
