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
	notifMap       map[string]map[string]customMessage // node_id -> (msg_id -> customMessage) eg. n4 -> [mid1, mid4]
	processedMsgs  map[string]bool
	msgMapMutex    sync.Mutex
	valStoreMutex  sync.Mutex
	processedMu    sync.Mutex
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
		msgChan:        make(chan customMessage, 10000),
		notifMap:       make(map[string]map[string]customMessage), // node_id -> (msg_id -> message) todo(): is nested map necessary? just use array indexed by node id
		processedMsgs:  make(map[string]bool),
		msgMapMutex:    sync.Mutex{},
		valStoreMutex:  sync.Mutex{},
		processedMu:    sync.Mutex{},
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
		time.Sleep(time.Second * 8)
		s.msgMapMutex.Lock()

		for _, msgMap := range s.notifMap {
			for _, msg := range msgMap {
				rpcMessage(msg, s.neighbourNotifiedCallback)
			}
		}

		s.msgMapMutex.Unlock()
	}
}

func (s *Server) processMessages() {
	for {
		select {
		case v := <-s.valChan:
			// msgStore updater
			s.valStoreMutex.Lock()
			s.valStore = append(s.valStore, v)
			s.valStoreMutex.Unlock()
		case m := <-s.msgChan:
			// msg sender
			//s.msgMapMutex.Lock()
			//
			//if _, exists := s.notifMap[m.dest]; !exists {
			//	s.notifMap[m.dest] = make(map[string]customMessage) // msg_id -> message
			//}
			//s.notifMap[m.dest][m.id] = m
			//
			//s.msgMapMutex.Unlock()
			//rpcMessage(m, s.neighbourNotifiedCallback)
			go s.neighbourNotifier(m)
		}
	}
}

func (s *Server) neighbourNotifier(msg customMessage) {
	// sync RPC with timeout
	s.n.SyncRPC(nil, msg.dest, msg.msgBody)
	// once timeout, retry (simply add msg to msgChan)
}

func (s *Server) recvBroadcast(msg maelstrom.Message) error {
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
		s.processedMu.Lock()
		// todo(): maybe set to true earlier to avoid duplicates?
		_, processed := s.processedMsgs[id]
		s.processedMu.Unlock()
		if processed {
			// avoid duplicates
			// if its already been processed, means its already been sent to my neighbours. no need to propagate
			return s.n.Reply(msg, map[string]string{"type": "broadcast_fine", "mid": id})
		}

		for _, n := range s.neighbourNodes {
			if _, alreadyNotified := body.AlreadyGot[n]; !alreadyNotified && n != msg.Src {
				needToSendTo = append(needToSendTo, n)
			}
		}
	case 'c':
		// if client src
		id = uuid.NewString()
		needToSendTo = append(needToSendTo, s.neighbourNodes...)
		err = s.n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	}

	val := body.Message
	s.valChan <- val // send msg for storage
	s.processedMu.Lock()
	s.processedMsgs[id] = true
	s.processedMu.Unlock()
	body.AlreadyGot[s.n.ID()] = true

	s.notifyNeighbours(needToSendTo, body, id, val)

	return err
}

func (s *Server) notifyNeighbours(needToSendTo []string, body *messageBody, id string, val int) {
	// build notified nodes map
	for _, destNode := range needToSendTo {
		body.AlreadyGot[destNode] = true
	}

	for _, destNode := range needToSendTo {
		s.msgChan <- customMessage{
			id:      id,
			n:       s.n,
			dest:    destNode,
			msgBody: messageBody{MsgType: "broadcast", Message: val, Mid: id, AlreadyGot: body.AlreadyGot},
		}
	}
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

func (s *Server) neighbourNotifiedCallback(msg maelstrom.Message) error {
	body, err := extractBody(msg)
	if err != nil {
		return err
	}

	s.msgMapMutex.Lock()

	for nodeId, msgMap := range s.notifMap {
		if msg.Src == nodeId {
			delete(msgMap, body.Mid)
		}
	}

	s.msgMapMutex.Unlock()
	return nil
}

func rpcMessage(msgData customMessage, resHandler maelstrom.HandlerFunc) {
	err := msgData.n.RPC(msgData.dest, msgData.msgBody, resHandler)
	if err != nil {
		log.Println("some error on send: ", err)
	}
}

func extractBody(msg maelstrom.Message) (*messageBody, error) {
	body := &messageBody{AlreadyGot: make(map[string]bool)}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return nil, err
	}
	return body, nil
}
