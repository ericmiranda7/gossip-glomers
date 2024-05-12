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
	valStore       []int
	valChan        chan int
	msgChan        chan customMessage
	msgQ           map[string]map[string]customMessage // node_id -> (msg_id -> customMessage) eg. n4 -> [mid1, mid4]
	processedMsgs  map[string]bool
	mu             sync.Mutex
}

type topologyMsg struct {
	Topology map[string][]string `json:"topology"`
}

type customMessage struct {
	id      string
	n       *maelstrom.Node
	dest    string
	msgBody any
}

type messageBody struct {
	MsgType    string          `json:"type"`
	Message    int             `json:"message"`
	MsgId      int             `json:"msg_id"`
	Mid        string          `json:"mid"`
	AlreadyGot map[string]bool `json:"alreadyGot"`
}

func main() {
	s := Server{
		n:              maelstrom.NewNode(),
		neighbourNodes: []string{},
		valStore:       []int{},
		valChan:        make(chan int),
		msgChan:        make(chan customMessage),
		msgQ:           make(map[string]map[string]customMessage), // node_id -> (msg_id -> message) todo(): is nested map necessary? just use array indexed by node id
		processedMsgs:  make(map[string]bool),
		mu:             sync.Mutex{},
	}

	// client handlers
	s.n.Handle("topology", s.topologyHandler)
	s.n.Handle("broadcast", s.recvBroadcast)
	s.n.Handle("read", s.readHandler)

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
		s.mu.Lock()

		for _, msgMap := range s.msgQ {
			for _, msg := range msgMap {
				rpcMessage(msg, s.neighbourNotified)
			}
		}

		s.mu.Unlock()
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

			if _, ok := s.msgQ[m.n.ID()]; !ok {
				s.msgQ[m.n.ID()] = make(map[string]customMessage)
			}
			s.msgQ[m.n.ID()][m.id] = m

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

	var needToSendTo []string

	var id string
	log.Println("MSGSRC IS", msg.Src, msg.Src[0])
	switch msg.Src[0] {
	case 'n':
		// if node src
		id = body.Mid
		s.mu.Lock()
		_, processed := s.processedMsgs[id]
		s.mu.Unlock()
		if processed {
			log.Println("m true")
			// avoid duplicates
			// if its already been processed, means its already been sent to my neighbours. no need to propagate
			return s.n.Reply(msg, map[string]string{"type": "broadcast_fine", "mid": id})
		}

		for _, n := range s.neighbourNodes {
			log.Println("alreadyNotified:", body.AlreadyGot[n], n)
			if _, alreadyNotified := body.AlreadyGot[n]; !alreadyNotified && n != msg.Src {
				needToSendTo = append(needToSendTo, n)
			}
		}
	case 'c':
		// if client src
		id = uuid.NewString()
		for _, n := range s.neighbourNodes {
			if n != msg.Src {
				needToSendTo = append(needToSendTo, n)
			}
		}
		err = s.n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	}

	val := body.Message
	s.valChan <- val // send msg for storage

	body.AlreadyGot[s.n.ID()] = true

	// build notified nodes map
	for _, destNode := range needToSendTo {
		body.AlreadyGot[destNode] = true
	}

	log.Println("I NEED TO SEND TO", needToSendTo)
	for _, destNode := range needToSendTo {
		s.msgChan <- customMessage{
			id:      id,
			n:       s.n,
			dest:    destNode,
			msgBody: messageBody{MsgType: "broadcast", Message: val, Mid: id, AlreadyGot: body.AlreadyGot},
		}
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

	for _, msgMap := range s.msgQ {
		delete(msgMap, body.Mid)
	}

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

func extractBody(msg maelstrom.Message) (*messageBody, error) {
	body := &messageBody{AlreadyGot: make(map[string]bool)}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func rpcMessage(msgData customMessage, resHandler maelstrom.HandlerFunc) {
	err := msgData.n.RPC(msgData.dest, msgData.msgBody, resHandler)
	if err != nil {
		log.Println("some error on send: ", err)
	}
}
