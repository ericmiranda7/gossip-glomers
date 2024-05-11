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
	s := Server{
		n:              maelstrom.NewNode(),
		neighbourNodes: []string{},
		valStore:       []float64{},
		valChan:        make(chan float64),
		msgChan:        make(chan customMessage),
		msgQ:           make(map[string]customMessage),
		processedMsgs:  make(map[string]bool),
		delMsgChan:     make(chan string),
		mu:             sync.Mutex{},
	}

	// client handlers
	s.n.Handle("topology", s.topologyHandler)
	s.n.Handle("broadcast", s.recvBroadcast)
	s.n.Handle("read", s.readHandler)

	// node handlers
	s.n.Handle("broadcast_fine", s.broadcastOk)

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
			sendMessage(m)
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
			s.msgQ[m.id] = m
			sendMessage(m)
		case dm := <-s.delMsgChan:
			delete(s.msgQ, dm)
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
	s.mu.Lock()
	s.processedMsgs[id] = true
	s.mu.Unlock()
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

func sendMessage(msgData customMessage) {
	err := msgData.n.Send(msgData.dest, msgData.val)
	if err != nil {
		log.Println("some error on send: ", err)
	}
}
