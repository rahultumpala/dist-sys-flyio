package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var messages []int = make([]int, 0)
var node *maelstrom.Node

func handle_propagate(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	messages = append(messages, int(body["message"].(float64)))

	var reply = make(map[string]any)
	reply["type"] = "propagate_ok"

	return node.Reply(msg, reply)
}

func propagate(input map[string]any) {
	// propagate this message to all neighbors

	var body map[string]any = make(map[string]any)
	body["message"] = input["message"]
	body["type"] = "propagate"

	var ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, vertex := range node.NodeIDs() {
		if vertex != node.ID() {
			_, err := node.SyncRPC(ctx, vertex, body)
			if err != nil {
				panic(err)
			}
		}
	}
}

func handle_broadcast(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	messages = append(messages, int(body["message"].(float64)))

	// propagate this message to all nodes in the network
	propagate(body)

	var reply = make(map[string]any)
	reply["type"] = "broadcast_ok"

	return node.Reply(msg, reply)
}

func handle_read(msg maelstrom.Message) error {

	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var reply = make(map[string]any)
	reply["type"] = "read_ok"
	reply["messages"] = messages

	return node.Reply(msg, reply)
}

func handle_topology(msg maelstrom.Message) error {

	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// do nothing, all nodes are available in node.NodeIDs()

	var reply = make(map[string]any)
	reply["type"] = "topology_ok"

	return node.Reply(msg, reply)
}

func main() {
	node = maelstrom.NewNode()
	node.Handle("read", handle_read)
	node.Handle("topology", handle_topology)
	node.Handle("broadcast", handle_broadcast)

	// custom RPC message
	node.Handle("propagate", handle_propagate)

	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}
}
