package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var node *maelstrom.Node
var counter int = 0
var kv maelstrom.KV

/*
-----------
   Utils
-----------
*/

func get_body_from_msg(msg maelstrom.Message) (map[string]any, error) {
	var body map[string]any
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func propagate(body map[string]any) {
	body["type"] = "propagate"
	for _, vertex := range node.NodeIDs() {
		if vertex == node.ID() {
			continue
		}
		go func() {
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				defer cancel()
				_, err := node.SyncRPC(ctx, vertex, body)
				if err != nil {
					// RPC failed/timed out, retry.
				} else {
					// SUCCESS
					return
				}
			}
		}()
	}
}

func add_delta(delta int) {
	/*
	 Write to SeqKV store in backgroud, and update counter to ensure it contains updated state.
	 local var counter is like in-mem cache. Updating it ensures immediate reads are not stale
	*/
	go func(from int, to int) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		kv.CompareAndSwap(ctx, "counter", from, to, true)
	}(counter, counter+delta)
	counter += delta
}

/*
------------------
   RPC Handlers
------------------
*/

func handle_propagate(msg maelstrom.Message) error {
	body, err := get_body_from_msg(msg)
	if err != nil {
		return err
	}
	delta := int(body["delta"].(float64))
	add_delta(delta)

	reply := make(map[string]any)
	reply["type"] = "propagate_ok"
	return node.Reply(msg, body)
}

func handle_add(msg maelstrom.Message) error {
	body, err := get_body_from_msg(msg)
	if err != nil {
		return err
	}

	delta := int(body["delta"].(float64))
	add_delta(delta)

	// Propagate this msg to all nodes in the network
	propagate(body)

	var reply map[string]any = make(map[string]any)
	reply["type"] = "add_ok"
	return node.Reply(msg, reply)
}

func handle_read(msg maelstrom.Message) error {
	_, err := get_body_from_msg(msg)
	if err != nil {
		return err
	}

	// Read into local variable in background and update state
	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			err = kv.ReadInto(ctx, "counter", counter)
			if err == nil {
				return
			}
		}
	}()

	var reply map[string]any = make(map[string]any)
	reply["type"] = "read_ok"
	// return local variable assuming it always contains updated state
	reply["value"] = counter
	return node.Reply(msg, reply)
}

func main() {
	node = maelstrom.NewNode()
	kv = *maelstrom.NewSeqKV(node)
	node.Handle("add", handle_add)
	node.Handle("read", handle_read)
	node.Handle("propagate", handle_propagate)
	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}
}
