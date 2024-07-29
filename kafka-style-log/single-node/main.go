package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var node *maelstrom.Node

/*
----- STRUCT -----
linked list
*/

type Event struct {
	offset float64
	value  float64
	next   *Event
}

var msgs_tail map[string]Event = make(map[string]Event)
var msgs_head map[string]Event = make(map[string]Event)

func (event Event) to_list() []float64 {
	result := make([]float64, 0)
	result = append(result, event.offset)
	result = append(result, event.value)
	return result
}

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

func get_recent_msg(key string) (Event, bool) {
	event, ok := msgs_tail[key]
	if !ok {
		return Event{}, false
	}
	return event, true
}

/*
------------------
   RPC Handlers
------------------
*/

var offset float64 = 0
var committed_offsets map[string]float64 = make(map[string]float64)

func handle_send(msg maelstrom.Message) error {
	body := get_body_from_msg(msg)
	var key string = body["key"].(string)
	msg_val := body["msg"].(float64)

	val, ok := get_recent_msg(key)
	curr_event := Event{offset, msg_val, nil}
	if !ok {
		msgs_head[key] = curr_event
		msgs_tail[key] = curr_event
	} else {
		val.next = &curr_event
		msgs_tail[key] = curr_event
	}

	reply := make(map[string]any)
	reply["type"] = "send_ok"
	reply["offset"] = offset

	offset++
	return node.Reply(msg, reply)
}

func handle_poll(msg maelstrom.Message) error {
	body := get_body_from_msg(msg)

	var offsets map[string]any = body["offsets"].(map[string]any)
	var results map[string][][]float64 = make(map[string][][]float64)

	for key, req_offset := range offsets {
		event := msgs_head[key]
		for event.next != nil {
			if event.offset >= req_offset.(float64) {
				result, ok := results[key]
				if !ok {
					result = make([][]float64, 0)
				}
				result = append(result, event.to_list())
				results[key] = result
			}
			event = *event.next
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

	for key, off_float := range offsets {
		commit_offset := off_float.(float64)
		event := msgs_head[key]
		if commit_offset == msgs_tail[key].offset {
			delete(msgs_head, key)
			delete(msgs_tail, key)
		} else {
			for event.next != nil {
				if event.offset <= commit_offset {
					next := *event.next
					event.next = nil

					msgs_head[key] = next
					event = next
				}
			}
		}
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
