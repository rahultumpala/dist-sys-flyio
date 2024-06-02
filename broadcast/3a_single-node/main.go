package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var messages []int =  make([]int, 0)
var node *maelstrom.Node

func handle_broadcast(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err;
	}

	messages = append(messages, int(body["message"].(float64)))

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

	var reply = make(map[string]any)
	reply["type"] = "topology_ok"
	
	return node.Reply(msg, reply)
}


func main(){
	node = maelstrom.NewNode()
	node.Handle("read", handle_read)
	node.Handle("topology", handle_topology)
	node.Handle("broadcast", handle_broadcast)

	err := node.Run()
	if(err != nil) {
		log.Fatal(err)
	}
}