package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// GLOBALS
var node *maelstrom.Node

var rw sync.RWMutex
var kv map[float64]float64 = make(map[float64]float64)

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

type operation struct {
	read  bool
	write bool
	key   float64
	val   float64
}

func generate_op_list(input any) []operation {
	var ops []operation = make([]operation, 0)
	for _, val := range input.([]any) {
		item := val.([]any)
		var op operation
		op.key = item[1].(float64)
		if item[2] != nil {
			op.val = item[2].(float64)
		}
		if item[0].(string) == "r" {
			op.read = true
		} else {
			op.write = true
		}
		ops = append(ops, op)
	}
	return ops
}

func (op *operation) to_array() []any {
	var array []any = make([]any, 0)
	if op.read {
		array = append(array, "r")
	} else {
		array = append(array, "w")
	}
	array = append(array, op.key)
	array = append(array, op.val)
	return array
}

/*
------------------
   RPC Handlers
------------------
*/

func handle_txn(msg maelstrom.Message) error {
	body, err := get_body_from_msg(msg)

	if err != nil {
		return err
	}

	var ops []operation = generate_op_list(body["txn"].(any))
	for _, op := range ops {
		if op.read {
			rw.RLock()
			op.val = kv[op.key]
			rw.RUnlock()
		} else {
			rw.Lock()
			kv[op.key] = op.val
			rw.Unlock()
		}
	}

	body["type"] = "txn_ok"
	resp := make([]any, 0)
	for _, op := range ops {
		resp = append(resp, op.to_array())
	}
	body["txn"] = resp
	node.Reply(msg, body)

	return nil
}

func main() {
	node = maelstrom.NewNode()
	node.Handle("txn", handle_txn)
	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}
}
