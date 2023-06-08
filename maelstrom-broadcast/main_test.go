package main

import (
	"bytes"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"sync"
	"testing"
)

func TestRecvBroadcast(t *testing.T) {
	n := maelstrom.NewNode()
	var valStore []float64
	jason, _ := json.Marshal(map[string]float64{"message": 1000})
	mu := sync.Mutex{}
	msg := maelstrom.Message{
		Src:  "src",
		Dest: "dst",
		Body: jason,
	}

	err := recvBroadcast(n, msg, &valStore, &mu)
	if err != nil {
		t.Fatal("wasn't expecting error")
	}

	if len(valStore) == 0 {
		t.Error("broadcast msg not saved!")
	}
}

func TestRead(t *testing.T) {
	n := maelstrom.NewNode()
	myStd := &bytes.Buffer{}
	n.Stdout = myStd
	valStore := []float64{2, 4, 6, 1}
	jason, _ := json.Marshal(map[string]string{"type": "read"})
	msg := maelstrom.Message{
		Src:  "src",
		Dest: "dest",
		Body: jason,
	}

	err := readHandler(n, msg, valStore)
	if err != nil {
		t.Fatal("wasn't expecting error")
	}

	if myStd.String() == "" {
		t.Error("shouldn't be empty!")
	}
}
