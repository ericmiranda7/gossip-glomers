package main

import (
	"bytes"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"testing"
)

func TestRecvBroadcast(t *testing.T) {
	n := maelstrom.NewNode()
	jason, _ := json.Marshal(map[string]float64{"message": 1000})
	valChan := make(chan float64, 1)
	msg := maelstrom.Message{
		Src:  "src",
		Dest: "dst",
		Body: jason,
	}

	err := recvBroadcast(n, msg, valChan, []string{"n0", "n1"})
	if err != nil {
		t.Fatal("wasn't expecting error")
	}

	if v := <-valChan; v != 1000 {
		t.Error("broadcast msg not saved!")
	}
}

func TestRead(t *testing.T) {
	n := maelstrom.NewNode()
	myStd := &bytes.Buffer{}
	n.Stdout = myStd
	valStore := []float64{2, 4, 6, 1}
	store := msgStore{valStore: valStore}
	jason, _ := json.Marshal(map[string]string{"type": "read"})
	msg := maelstrom.Message{
		Src:  "src",
		Dest: "dest",
		Body: jason,
	}

	err := readHandler(n, msg, &store)
	if err != nil {
		t.Fatal("wasn't expecting error")
	}

	if myStd.String() == "" {
		t.Error("shouldn't be empty!")
	}
}
