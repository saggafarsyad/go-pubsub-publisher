package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type Input struct {
	ProjectId      string      `json:"project_id"`
	CredentialPath string      `json:"credential_path"`
	Topic          string      `json:"topic"`
	Payload        interface{} `json:"payload"`
}

func main() {
	// Load input
	buf, err := ioutil.ReadFile("input.json")
	if err != nil {
		panic(fmt.Errorf("FATAL: cannot read config.json file (%s)", err))
	}

	// Parse test cases in json
	var input Input
	err = json.Unmarshal(buf, &input)
	if err != nil {
		panic(fmt.Errorf("FATAL: cannot parse input.json file (%s)", err))
	}

	// Init client
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, input.ProjectId, option.WithCredentialsFile(input.CredentialPath))
	if err != nil {
		log.Printf("ERROR: failed to init new PubSub client (%s)", err)
		return
	}

	t := client.Topic(input.Topic)
	t.PublishSettings.NumGoroutines = 1

	payloadBuf, err := json.Marshal(input.Payload)
	if err != nil {
		log.Printf("ERROR: failed to marshal json (%s)", err)
		return
	}

	// Publish
	result := t.Publish(ctx, &pubsub.Message{Data: payloadBuf})

	// Get id
	resultId, err := result.Get(ctx)
	if err != nil {
		log.Printf("ERROR: failed to retrieve result (%s)", err)
		return
	}

	log.Printf("DEBUG: result=%+v", resultId)
}
