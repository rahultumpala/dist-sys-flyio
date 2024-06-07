package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var messages []float64 = make([]float64, 0)
var node *maelstrom.Node

// store msgs that timed out while gossiping
var failed_msgs map[string][]float64 = make(map[string][]float64)

func handle_propagate(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	messages = append(messages, body["message"].(float64))

	var reply = make(map[string]any)
	reply["type"] = "propagate_ok"

	return node.Reply(msg, reply)
}

func propagate(input map[string]any) {
	// propagate this message to all neighbors

	var body map[string]any = make(map[string]any)
	body["message"] = input["message"]
	body["type"] = "propagate"

	var ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	for _, vertex := range node.NodeIDs() {
		if vertex == node.ID() {
			continue
		}
		_, err := node.SyncRPC(ctx, vertex, body)
		if err != nil {
			// if failed, add to failed_msgs of that vertex, to be returned later when requested
			failed_msgs[vertex] = append(failed_msgs[vertex], input["message"].(float64))
		}
	}
}

func handle_broadcast(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	messages = append(messages, body["message"].(float64))

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

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// request for failed messages for all other nodes in the network and add to current list
	var reqBody map[string]any = make(map[string]any)
	reqBody["type"] = "req_failed_msg"
	for _, v := range node.NodeIDs() {
		if v == node.ID() {
			continue
		}

		reply, err := node.SyncRPC(ctx, v, reqBody)
		if err == nil {
			if err := json.Unmarshal(reply.Body, &body); err != nil {
				return err
			}
			if body["messages"] == nil {
				continue
			}
			failed := body["messages"].([]interface{})
			for _, fmsg := range failed {
				messages = append(messages, fmsg.(float64))
			}

		}
	}

	var reply = make(map[string]any)
	reply["type"] = "read_ok"
	reply["messages"] = messages // return updated current list

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

	// custom RPC msg to perform gossip
	node.Handle("propagate", handle_propagate)

	// custom RPC msg to request failed messages from other nodes
	node.Handle("req_failed_msg", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var reply map[string]any = make(map[string]any)

		reply["messages"] = failed_msgs[msg.Src]
		// set to nil after responding to avoid sending duplicates
		failed_msgs[msg.Src] = nil

		return node.Reply(msg, reply)
	})

	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}
}
