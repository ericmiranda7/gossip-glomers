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
	msgQ           map[string]customMessage
	processedMsgs  map[string]bool
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
	s := Server{
		n:              maelstrom.NewNode(),
		neighbourNodes: []string{},
		valStore:       []float64{},
		valChan:        make(chan float64),
		msgChan:        make(chan customMessage),
		msgQ:           make(map[string]customMessage),
		processedMsgs:  make(map[string]bool),
		mu:             sync.Mutex{},
	}

	// client handlers
	s.n.Handle("topology", s.topologyHandler)
	s.n.Handle("broadcast", s.recvBroadcast)
	s.n.Handle("read", s.readHandler)

	// node handlers
	s.n.Handle("broadcast_fine", s.neighbourNotified)

	// background housekeepers
	go s.processMessages()

	go s.retryMechanism()

	err := s.n.Run()
	if err != nil {
		log.Fatal(err)
	}
}

func (s *Server) retryMechanism() {
	// TODO(): the below code is erroneous, shift from an infinite for to goroutine per message
	for {
		time.Sleep(time.Second * 5)
		for _, m := range s.msgQ {
			rpcMessage(m, s.neighbourNotified)
		}
	}
}

func (s *Server) processMessages() {
	for {
		select {
		case v := <-s.valChan:
			// msgStore updater
			s.mu.Lock()
			s.valStore = append(s.valStore, v)
			s.mu.Unlock()
		case m := <-s.msgChan:
			// msg sender
			s.mu.Lock()
			s.msgQ[m.id] = m
			s.mu.Unlock()
			rpcMessage(m, s.neighbourNotified)
		}
	}
}

func (s *Server) recvBroadcast(msg maelstrom.Message) error {
	body, err := extractBody(msg)
	if err != nil {
		return err
	}

	var id string
	switch msg.Src[0] {
	case 'n':
		// if node src
		id = body["mid"].(string)
		s.mu.Lock()
		_, ok := s.processedMsgs[id]
		s.mu.Unlock()
		if ok {
			return s.n.Reply(msg, map[string]string{"type": "broadcast_fine", "mid": id})
		}
		break
	case 'c':
		// if client src
		id = uuid.NewString()
	}

	val := body["message"].(float64)
	s.valChan <- val // send msg for storage

	// notify neighbours
	for _, destNode := range s.neighbourNodes {
		if destNode == msg.Src {
			continue
		}
		s.msgChan <- customMessage{
			id:   id,
			n:    s.n,
			dest: destNode,
			val:  map[string]any{"type": "broadcast", "message": val, "mid": id},
		}
	}

	if msg.Src[0] == 'c' {
		err = s.n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	} else {
		err = s.n.Reply(msg, map[string]string{"type": "broadcast_fine", "mid": id})
	}
	s.mu.Lock()
	s.processedMsgs[id] = true
	s.mu.Unlock()
	return err
}

func (s *Server) neighbourNotified(msg maelstrom.Message) error {
	body, err := extractBody(msg)
	if err != nil {
		return err
	}

	// todo(): each resource with own mu?
	s.mu.Lock()
	delete(s.msgQ, body["mid"].(string))
	s.mu.Unlock()
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
	// mu not needed since valStore is appended always
	res["messages"] = s.valStore

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

func rpcMessage(msgData customMessage, resHandler maelstrom.HandlerFunc) {
	err := msgData.n.RPC(msgData.dest, msgData.val, resHandler)
	if err != nil {
		log.Println("some error on send: ", err)
	}
}
