package main

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"time"
)

type Server struct {
	n              *maelstrom.Node
	neighbourNodes []string
	valStore       []int
	valChan        chan int
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
		valChan:        make(chan int, 10000),
	}

	// client handlers
	s.n.Handle("topology", s.topologyHandler)
	s.n.Handle("broadcast", s.broadcastHandler)
	s.n.Handle("read", s.readHandler)

	// background housekeepers
	go s.processMessages()

	err := s.n.Run()
	if err != nil {
		log.Fatal(err)
	}
}

func (s *Server) processMessages() {
	for {
		v := <-s.valChan
		s.valStore = append(s.valStore, v)
	}
}

func (s *Server) broadcastHandler(msg maelstrom.Message) error {
	body, err := extractBody(msg)
	if err != nil {
		return err
	}

	var needToSendTo []string

	var id string
	switch msg.Src[0] {
	case 'n':
		// if node src
		id = body.Mid

		err := s.n.Reply(msg, map[string]string{"type": "broadcast_fine", "mid": id})
		if err != nil {
			return err
		}

		for _, n := range s.neighbourNodes {
			if alreadyNotified := body.AlreadyGot[n]; !alreadyNotified && n != msg.Src {
				needToSendTo = append(needToSendTo, n)
			}
		}
	case 'c':
		// if client src
		id = uuid.NewString()
		needToSendTo = append(needToSendTo, s.neighbourNodes...)
		err = s.n.Reply(msg, map[string]string{"type": "broadcast_ok"})
		if err != nil {
			return err
		}
	}

	s.valChan <- body.Message // send msg for storage

	body.AlreadyGot[s.n.ID()] = true
	body.Mid = id

	s.notifyNeighbours(needToSendTo, body)
	body.Mid = id

	return nil
}

func (s *Server) notifyNeighbours(needToSendTo []string, body *messageBody) {
	// build notified nodes map
	for _, destNode := range needToSendTo {
		body.AlreadyGot[destNode] = true
	}

	for _, destNode := range needToSendTo {
		cmsg := customMessage{
			id:      body.Mid,
			n:       s.n,
			dest:    destNode,
			msgBody: messageBody{MsgType: "broadcast", Message: body.Message, Mid: body.Mid, AlreadyGot: body.AlreadyGot},
		}
		go s.neighbourNotifier(cmsg)
	}
}

func (s *Server) neighbourNotifier(msg customMessage) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond*500))
	_, err := s.n.SyncRPC(ctx, msg.dest, msg.msgBody)
	if err != nil {
		log.Println("Some error", err)
		go s.neighbourNotifier(msg)
	}
	cancel()
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
