package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

/*

Strategy:

SeqKV doesn't work in real-time constraints but LinKV does.

Use SeqKV to store messages for each offset of a client. - background process
Use LinKV to store latest offsets for each client. - background process

--- Update:
This strategy works well upto a max rate of 400 reqs/second.
Beyond that, timeouts will become frequent as a result of many heavy coroutines that need to be scheduled.
To fix this bottleneck, I added batching. Gossip & write in batches.
This scaled upto 2k rps with a gossip containing ~40 msgs at the least.

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
	( stale state here is invalidated when new gossips are read )


Note:
-----

In this strategy, KV Stores are only used as persistent storage,
to reload in-memory cache in the event of a node crash.

However, this crash is not simulated as a nemesis in this challenge,
which means it is possible to pass this challenge without using KV at all.

*/

// GLOBALS
var node *maelstrom.Node
var rw sync.RWMutex

// KV Stores
var seqKV maelstrom.KV
var linKV maelstrom.KV

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
	rw.RLock()
	defer rw.RUnlock()
	value, ok := latest_offsets[key]
	if !ok {
		latest_offsets[key] = 1
	}

	return value + 1
}

func set_offset(key string, value float64) {
	rw.Lock()
	defer rw.Unlock()
	latest_offsets[key] = value
}

func set_val(m map[string]float64, key string, val float64) {
	rw.Lock()
	defer rw.Unlock()
	m[key] = val
}

func get_val(m map[string]float64, key string) float64 {
	rw.RLock()
	defer rw.RUnlock()
	return m[key]
}

/*
------------------
   RPC Handlers
------------------
*/

var send_batch []map[string]any = make([]map[string]any, 0)

func handle_send(msg maelstrom.Message) error {
	body := get_body_from_msg(msg)
	var key string = body["key"].(string)
	msg_val := body["msg"].(float64)

	offset := get_offset(key)
	msg_storage_key := fmt.Sprintf("%s_%f", key, offset)
	set_val(messages, msg_storage_key, msg_val)

	reply := make(map[string]any)
	reply["type"] = "send_ok"
	reply["offset"] = offset

	set_offset(key, offset)
	node.Reply(msg, reply)

	body["latest_offset"] = offset + 1

	send_batch = append(send_batch, body)
	return nil
}

func handle_poll(msg maelstrom.Message) error {
	body := get_body_from_msg(msg)

	var offsets map[string]any = body["offsets"].(map[string]any)
	var results map[string][][]float64 = make(map[string][][]float64)

	for key, offset := range offsets {
		latest_offset := get_offset(key) - 1
		req_offset := offset.(float64)

		result := make([][]float64, 0)
		for ; req_offset <= latest_offset; req_offset++ {
			results[key] = append(results[key], []float64{req_offset, get_val(messages, fmt.Sprintf("%s_%f", key, req_offset))})
		}
		results[key] = result
	}

	var reply map[string]any = make(map[string]any)
	reply["type"] = "poll_ok"
	reply["msgs"] = results

	return node.Reply(msg, reply)
}

var commit_batch []map[string]any = make([]map[string]any, 0)

func handle_commit_offsets(msg maelstrom.Message) error {
	body := get_body_from_msg(msg)
	var offsets map[string]any = body["offsets"].(map[string]any)

	for key, offset := range offsets {
		commit_offset := offset.(float64)
		set_val(committed_offsets, key, commit_offset)
	}

	var reply map[string]string = make(map[string]string)
	reply["type"] = "commit_offsets_ok"

	node.Reply(msg, reply)

	body["type"] = "gossip_commit_offset"
	commit_batch = append(commit_batch, body)

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
	b := get_body_from_msg(msg)
	var batch []any = b["batch"].([]any)
	for _, v := range batch {
		body := v.(map[string]any)
		var key string = body["key"].(string)
		msg_val := body["msg"].(float64)
		offset := body["latest_offset"]
		msg_storage_key := fmt.Sprintf("%s_%f", key, offset)
		set_val(messages, msg_storage_key, msg_val)
	}
	return nil
}

func handle_commit_offset_gossip(msg maelstrom.Message) error {

	body := get_body_from_msg(msg)
	var batch []any = body["batch"].([]any)
	for _, v := range batch {
		body := v.(map[string]any)
		var offsets map[string]any = body["offsets"].(map[string]any)

		for key, offset := range offsets {
			commit_offset := offset.(float64)
			set_val(committed_offsets, key, commit_offset)
		}
	}

	return nil
}

/*
---------------------

	Background

---------------------
*/
func init_batch_routines() {
	go func() {
		for {
			if len(send_batch) >= 2 {
				log.Printf("Gossiping %d SEND messages", len(send_batch))
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

				// Gossip Send Message, latest offset to other nodes
				var body map[string]any = make(map[string]any)
				body["type"] = "gossip_send"
				body["batch"] = send_batch
				for _, vertex := range node.NodeIDs() {
					if vertex == node.ID() {
						continue
					}
					node.RPC(vertex, body, nil)
				}
				for _, body := range send_batch {
					var key string = body["key"].(string)
					msg_val := body["msg"].(float64)
					offset := body["latest_offset"].(float64) - 1

					// Write latest Offset to LinKV
					recent_offset_key := fmt.Sprintf("latest_%s", key)
					linKV.CompareAndSwap(ctx, recent_offset_key, offset, offset, true)

					// Write Message to SeqKV
					seqKvKey := fmt.Sprintf("%s_%f", key, offset)
					seqKV.Write(ctx, seqKvKey, msg_val)
				}
				cancel()
				send_batch = make([]map[string]any, 0)
			}

			if len(commit_batch) >= 2 {
				log.Printf("Gossiping %d COMMIT messages", len(commit_batch))
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				// Gossip Latest Offset to other nodes
				var body map[string]any = make(map[string]any)
				body["type"] = "gossip_commit_offset"
				body["batch"] = commit_batch
				for _, vertex := range node.NodeIDs() {
					if vertex == node.ID() {
						continue
					}
					node.RPC(vertex, body, nil)
				}

				for _, body := range commit_batch {
					// Update offsets in LinKV
					var offsets map[string]any = body["offsets"].(map[string]any)
					for key, offset := range offsets {
						commit_offset := offset.(float64)
						committed_offsets[key] = commit_offset
						commit_offset_key := fmt.Sprintf("commit_%s", key)
						linKV.CompareAndSwap(ctx, commit_offset_key, latest_offsets[key], commit_offset, true)
					}
				}
				cancel()
				commit_batch = make([]map[string]any, 0)
			}

			time.Sleep(2 * time.Second)
		}
	}()

}

func main() {
	node = maelstrom.NewNode()
	seqKV = *maelstrom.NewSeqKV(node)
	linKV = *maelstrom.NewLinKV(node)

	node.Handle("send", handle_send)
	node.Handle("poll", handle_poll)
	node.Handle("commit_offsets", handle_commit_offsets)
	node.Handle("list_committed_offsets", handle_list_committed_offsets)

	node.Handle("gossip_send", handle_send_gossip)
	node.Handle("gossip_commit_offset", handle_commit_offset_gossip)

	init_batch_routines()

	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}
}
