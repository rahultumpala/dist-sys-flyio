package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var node *maelstrom.Node

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
var msgs map[string][]float64 = make(map[string][]float64)

func handle_send(msg maelstrom.Message) error {
	body := get_body_from_msg(msg)
	var key string = body["key"].(string)
	msg_val := body["msg"].(float64)

	val, ok := msgs[key]
	if !ok {
		val = make([]float64, 0)
	}
	val = append(val, msg_val)
	msgs[key] = val

	reply := make(map[string]any)
	reply["type"] = "send_ok"
	reply["offset"] = offset

	offset++
	return node.Reply(msg, reply)
}

func main() {
	node = maelstrom.NewNode()
	node.Handle("send", handle_send)
	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}
}
