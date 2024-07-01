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
var broadcast_batch []float64 = make([]float64, 0)

func handle_propagate(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var reply = make(map[string]any)
	reply["type"] = "propagate_ok"

	err := node.Reply(msg, reply)
	if err != nil {
		panic(err)
	}

	for _, value := range body["message"].([]interface{}) {
		messages = append(messages, value.(float64))
	}

	return nil
}

func propagate() {
	if len(broadcast_batch) == 0 {
		return
	}

	// propagate this message to all neighbors
	var body map[string]any = make(map[string]any)
	body["message"] = broadcast_batch
	body["type"] = "propagate"

	for _, vertex := range node.NodeIDs() {
		if vertex == node.ID() {
			continue
		}

		go func(vertex string) {
			/*
			 run a background task that will keep sending a message to ALL Nodes in the network
			 until the msg is successful (with a timeout of 200ms)

			 This is fault tolerant - works even in the case of network partitions
			 (verified with --nemesis partition)

			 This is resilient - when rate >= 100 response is not delayed because
			 propagation is performed in the background

			 This makes our propagation model eventually consistent
			 Messages are propagated for sure, but immediate read requests might respond
			 with incoomplete data
			*/
			for {
				var ctx, cancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
				defer cancel()
				_, err := node.SyncRPC(ctx, vertex, body)
				if err != nil {
					// if failed keep retrying
				} else {
					return
				}
			}
		}(vertex)
	}
}

func handle_broadcast(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	messages = append(messages, body["message"].(float64))

	var reply = make(map[string]any)
	reply["type"] = "broadcast_ok"

	err := node.Reply(msg, reply)
	if err != nil {
		panic(err)
	}

	/*
		Batch broadcast updates so that msgs per op are reduced.
	*/
	broadcast_batch = append(broadcast_batch, body["message"].(float64))
	if len(broadcast_batch) >= 2 {
		propagate()
		broadcast_batch = make([]float64, 0)
	} else {
		go func() {
			// propagate this message to all nodes in the network
			time.Sleep(time.Millisecond * 250)
			propagate()
			broadcast_batch = make([]float64, 0)
		}()
	}

	return nil
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

	// custom RPC msg to perform gossip
	node.Handle("propagate", handle_propagate)

	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}
}
