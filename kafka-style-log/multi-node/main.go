package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// GLOBALS

/*

Strategy:

SeqKV doesn't work in real-time constraints but LinKV does.

Use SeqKV to store messages for each offset of a client. - Do in background.
Use LinKV to generate incremental offsets for each client. - Do in realtime.

*/

var node *maelstrom.Node
var seqKV maelstrom.KV = *maelstrom.NewSeqKV(node)
var linKV maelstrom.KV = *maelstrom.NewLinKV(node)
var committed_offsets map[string]float64 = make(map[string]float64)

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

func get_offset(ctx context.Context, key string) float64 {
	value, err := linKV.Read(ctx, key)
	if err != nil && err.(*maelstrom.RPCError).Text == "KeyDoesNotExist" {
		return 0
	}
	return value.(float64)
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

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	recent_offset_key := fmt.Sprintf("recent_%s", key)
	offset := get_offset(ctx, recent_offset_key)

	linKV.CompareAndSwap(ctx, recent_offset_key, offset, offset+1, true)

	go func() {
		seqKvKey := fmt.Sprintf("%s_%f", key, offset)
		seqKV.Write(ctx, seqKvKey, msg_val)
	}()

	reply := make(map[string]any)
	reply["type"] = "send_ok"
	reply["offset"] = offset

	node.Reply(msg, reply)

	return nil
}

func handle_poll(msg maelstrom.Message) error {
	body := get_body_from_msg(msg)

	var offsets map[string]any = body["offsets"].(map[string]any)
	var results map[string][][]float64 = make(map[string][][]float64)

	for key, offset := range offsets {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		recent_offset_key := fmt.Sprintf("recent_%s", key)
		recent_offset := get_offset(ctx, recent_offset_key)

		results[key] = make([][]float64, 0)
		req_offset := offset.(float64)

		for ; req_offset <= recent_offset; req_offset++ {
			seqKvKey := fmt.Sprintf("%s_%f", key, req_offset)
			value, _ := seqKV.Read(ctx, seqKvKey)
			results[key] = append(results[key], []float64{req_offset, value.(float64)})
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
		commit_offset_key := fmt.Sprintf("commit_%s", key)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		current_commit_offset := get_offset(ctx, commit_offset_key)

		linKV.CompareAndSwap(ctx, commit_offset_key, current_commit_offset, commit_offset, true)

		committed_offsets[key] = commit_offset
	}

	var reply map[string]string = make(map[string]string)
	reply["type"] = "commit_offsets_ok"

	return node.Reply(msg, reply)
}

func handle_list_committed_offsets(msg maelstrom.Message) error {
	var reply map[string]any = make(map[string]any)
	reply["type"] = "list_committed_offsets_ok"
	reply["offsets"] = committed_offsets
	return node.Reply(msg, reply)
}

func main() {
	node = maelstrom.NewNode()
	node.Handle("send", handle_send)
	node.Handle("poll", handle_poll)
	node.Handle("commit_offsets", handle_commit_offsets)
	node.Handle("list_committed_offsets", handle_list_committed_offsets)
	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}
}
