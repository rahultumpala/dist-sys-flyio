package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

/*

Strategy:

SeqKV doesn't work in real-time constraints but LinKV does.

Use SeqKV to store messages for each offset of a client. - background process
Use LinKV to store latest offsets for each client. - background process

Send Handler:

- Ack the send req.
- Update in-memory cache of messages and latest offsets
- Write to SeqKV the message with corresponding offset
- Write to LinKV the latest offset
- Gossip the send message to other nodes in the network
- Gossip Latest offset to other nodes in the network

Poll Handler:

- Assumption is that all in-memory caches are fresh
	( stale state here is invalidated when new gossips are read )
- Read all messages from in-memory state and respond

Commit Offset handler:

- Update local committed offset state
- Write to LinKV
- Gossip to other nodes in network

List Committed Offsets Handler:

- Read from local state
	( state state here is invalidated when new gossips are read )

*/

// GLOBALS

var node *maelstrom.Node

// KV Stores
var seqKV maelstrom.KV = *maelstrom.NewSeqKV(node)
var linKV maelstrom.KV = *maelstrom.NewLinKV(node)

// in memory cache
var committed_offsets map[string]float64 = make(map[string]float64)
var latest_offsets map[string]float64 = make(map[string]float64)
var messages map[string]float64 = make(map[string]float64)

/*
-----------
   Utils
-----------
*/

func get_body_from_msg(msg maelstrom.Message) map[string]any {
	var body map[string]any
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		log.Print("ERROR Deserializing body from Request")
		panic(err)
	}
	return body
}

func get_offset(key string) float64 {
	value, ok := latest_offsets[key]
	if !ok {
		latest_offsets[key] = 1
	}
	return value + 1
}

func set_offset(key string, value float64) {
	latest_offsets[key] = value
}

/*
------------------
   RPC Handlers
------------------
*/

func handle_send(msg maelstrom.Message) error {
	body := get_body_from_msg(msg)
	var key string = body["key"].(string)
	msg_val := body["msg"].(float64)

	offset := get_offset(key)
	msg_storage_key := fmt.Sprintf("%s_%f", key, offset)
	messages[msg_storage_key] = msg_val

	reply := make(map[string]any)
	reply["type"] = "send_ok"
	reply["offset"] = offset

	set_offset(key, offset)
	node.Reply(msg, reply)

	// Write latest Offset to LinKV
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		recent_offset_key := fmt.Sprintf("latest_%s", key)
		linKV.CompareAndSwap(ctx, recent_offset_key, offset, offset, true)
	}()

	// Write Message to SeqKV
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		seqKvKey := fmt.Sprintf("%s_%f", key, offset)
		seqKV.Write(ctx, seqKvKey, msg_val)
	}()

	// Gossip Send Message, latest offset to other nodes
	go func() {
		for _, vertex := range node.NodeIDs() {
			if vertex == node.ID() {
				continue
			}
			body["type"] = "gossip_send"
			body["latest_offset"] = offset + 1
			node.Send(vertex, body)
		}
	}()

	return nil
}

func handle_poll(msg maelstrom.Message) error {
	body := get_body_from_msg(msg)

	var offsets map[string]any = body["offsets"].(map[string]any)
	var results map[string][][]float64 = make(map[string][][]float64)

	for key, offset := range offsets {
		latest_offset := get_offset(key) - 1
		req_offset := offset.(float64)

		results[key] = make([][]float64, 0)

		for ; req_offset <= latest_offset; req_offset++ {
			results[key] = append(results[key], []float64{req_offset, messages[fmt.Sprintf("%s_%f", key, req_offset)]})
		}
	}

	var reply map[string]any = make(map[string]any)
	reply["type"] = "poll_ok"
	reply["msgs"] = results

	return node.Reply(msg, reply)
}

func handle_commit_offsets(msg maelstrom.Message) error {
	body := get_body_from_msg(msg)
	var offsets map[string]any = body["offsets"].(map[string]any)

	for key, offset := range offsets {
		commit_offset := offset.(float64)
		committed_offsets[key] = commit_offset

		go func(key string) {
			commit_offset_key := fmt.Sprintf("commit_%s", key)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			linKV.CompareAndSwap(ctx, commit_offset_key, latest_offsets[key], commit_offset, true)
		}(key)
	}

	var reply map[string]string = make(map[string]string)
	reply["type"] = "commit_offsets_ok"

	node.Reply(msg, reply)

	// Gossip Send Message, latest offset to other nodes
	go func() {
		for _, vertex := range node.NodeIDs() {
			if vertex == node.ID() {
				continue
			}
			body["type"] = "gossip_commit_offset"
			node.Send(vertex, body)
		}
	}()

	return nil
}

func handle_list_committed_offsets(msg maelstrom.Message) error {
	var reply map[string]any = make(map[string]any)
	reply["type"] = "list_committed_offsets_ok"
	reply["offsets"] = committed_offsets
	return node.Reply(msg, reply)
}

/*
---------------------
   Gossip Handlers
---------------------
*/

func handle_send_gossip(msg maelstrom.Message) error {
	body := get_body_from_msg(msg)
	var key string = body["key"].(string)
	msg_val := body["msg"].(float64)
	offset := body["latest_offset"]
	msg_storage_key := fmt.Sprintf("%s_%f", key, offset)
	messages[msg_storage_key] = msg_val
	return nil
}

func handle_commit_offset_gossip(msg maelstrom.Message) error {

	body := get_body_from_msg(msg)
	var offsets map[string]any = body["offsets"].(map[string]any)

	for key, offset := range offsets {
		commit_offset := offset.(float64)
		committed_offsets[key] = commit_offset
	}

	return nil
}

func main() {
	node = maelstrom.NewNode()
	node.Handle("send", handle_send)
	node.Handle("poll", handle_poll)
	node.Handle("commit_offsets", handle_commit_offsets)
	node.Handle("list_committed_offsets", handle_list_committed_offsets)

	node.Handle("gossip_send", handle_send_gossip)
	node.Handle("gossip_commit_offset", handle_commit_offset_gossip)

	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}
}
