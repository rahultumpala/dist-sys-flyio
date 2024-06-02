package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		body["type"] = "generate_ok"
		pid := os.Getpid()
		// unique id = [process id]_[nansecond time]_[incoming msg_id]
		body["id"] = strconv.Itoa(pid) + "_" + strconv.Itoa(time.Now().Nanosecond()) + "_" + fmt.Sprintf("%v", body["msg_id"])

		return n.Reply(msg, body)
	})
	err := n.Run()
	if err != nil {
		log.Fatal(err)
	}
}
