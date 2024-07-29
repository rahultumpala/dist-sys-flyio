package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var node *maelstrom.Node

type Event struct {
	offset int
	value  int
}

func (event Event) to_list() []int {
	result := make([]int, 0)
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

/*
------------------
   RPC Handlers
------------------
*/

var offset int = 0
var msgs map[string][]Event = make(map[string][]Event)

func handle_send(msg maelstrom.Message) error {
	body := get_body_from_msg(msg)
	var key string = body["key"].(string)
	msg_val := body["msg"].(int)

	val, ok := msgs[key]
	if !ok {
		val = make([]Event, 0)
	}
	val = append(val, Event{offset, msg_val})
	msgs[key] = val

	reply := make(map[string]any)
	reply["type"] = "send_ok"
	reply["offset"] = offset

	offset++
	return node.Reply(msg, reply)
}

func handle_poll(msg maelstrom.Message) error {
	body := get_body_from_msg(msg)

	var offsets map[string]float64 = body["offsets"].(map[string]float64)
	var results map[string][][]int = make(map[string][][]int)

	for key, req_offset := range offsets {
		for _, event := range msgs[key] {
			if event.offset >= int(req_offset) {
				result, ok := results[key]
				if !ok {
					result = make([][]int, 0)
				}
				result = append(result, event.to_list())
				results[key] = result
			}
		}
	}

	var reply map[string]any = make(map[string]any)
	reply["type"] = "poll_ok"
	reply["msgs"] = results

	return node.Reply(msg, reply)
}

func main() {
	node = maelstrom.NewNode()
	node.Handle("send", handle_send)
	node.Handle("poll", handle_poll)
	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}
}
