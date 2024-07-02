package hyperbasebench_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

// Environment variables:
//
// - BROKER: string
// - TOPIC: string
// - QOS: integer
// - PROJECT_ID: string
// - TOKEN_ID: string
// - AUTH_COLLECTION_ID: string
// - AUTH_USER_ID: string
// - TARGET_COLLECTION_ID: string
// - COUNT: integer
// - PARALLEL: integer

func TestMQTTInsertOneUntil(t *testing.T) {
	type UserCredential struct {
		CollectionID uuid.UUID `json:"collection_id"`
		ID           uuid.UUID `json:"id"`
	}
	type Payload struct {
		ProjectID    uuid.UUID      `json:"project_id"`
		TokenID      uuid.UUID      `json:"token_id"`
		User         UserCredential `json:"user"`
		CollectionID uuid.UUID      `json:"collection_id"`
		Data         Data           `json:"data"`
	}

	states := []string{"Active", "Idle"}

	broker, ok := os.LookupEnv("BROKER")
	if !ok {
		t.Fatal("BROKER does not exist")
	}
	topic, ok := os.LookupEnv("TOPIC")
	if !ok {
		t.Fatal("TOPIC does not exist")
	}
	qosStr, ok := os.LookupEnv("QOS")
	if !ok {
		t.Fatal("QOS does not exist")
	}
	qos, err := strconv.Atoi(qosStr)
	if err != nil {
		t.Fatal(err)
	}
	projectIDStr, ok := os.LookupEnv("PROJECT_ID")
	if !ok {
		t.Fatal("PROJECT_ID does not exist")
	}
	projectID, err := uuid.Parse(projectIDStr)
	if err != nil {
		t.Fatal(err)
	}
	tokenIDStr, ok := os.LookupEnv("TOKEN_ID")
	if !ok {
		t.Fatal("TOKEN_IF does not exist")
	}
	tokenID, err := uuid.Parse(tokenIDStr)
	if err != nil {
		t.Fatal(err)
	}
	authCollectionIDStr, ok := os.LookupEnv("AUTH_COLLECTION_ID")
	if !ok {
		t.Fatal("AUTH_COLLECTION_ID does not exist")
	}
	authCollectionID, err := uuid.Parse(authCollectionIDStr)
	if err != nil {
		t.Fatal(err)
	}
	authUserIDStr, ok := os.LookupEnv("AUTH_USER_ID")
	if !ok {
		t.Fatal("AUTH_USER_ID does not exist")
	}
	authUserID, err := uuid.Parse(authUserIDStr)
	if err != nil {
		t.Fatal(err)
	}
	targetCollectionIDStr, ok := os.LookupEnv("TARGET_COLLECTION_ID")
	if !ok {
		t.Fatal("TARGET_COLLECTION_ID does not exist")
	}
	targetCollectionID, err := uuid.Parse(targetCollectionIDStr)
	if err != nil {
		t.Fatal(err)
	}
	countStr, ok := os.LookupEnv("COUNT")
	if !ok {
		t.Fatal("COUNT does not exist")
	}
	count, err := strconv.Atoi(countStr)
	if err != nil {
		t.Fatal(err)
	}
	parallelStr, ok := os.LookupEnv("PARALLEL")
	if !ok {
		t.Fatal("PARALLEL does not exist")
	}
	parallel, err := strconv.Atoi(parallelStr)
	if err != nil {
		t.Fatal(err)
	}

	store := Store{}

	fpub := func(client mqtt.Client, payload any) {

		success := true
		tStart := time.Now()
		token := client.Publish(topic, byte(qos), true, payload)
		if token.Wait() && token.Error() != nil {
			success = false
			t.Error(token.Error())
		}
		tEnd := time.Now()

		store.Append(tEnd.Sub(tStart), success)
	}

	fworker := func(client mqtt.Client, reqs <-chan any, wg *sync.WaitGroup) {
		for req := range reqs {
			fpub(client, req)
		}
		wg.Done()
	}

	taskQueue := make(chan any, parallel)
	wg := new(sync.WaitGroup)

	for i := range parallel {
		wg.Add(1)

		opts := mqtt.NewClientOptions()
		opts.AddBroker(broker)
		opts.SetCleanSession(false)
		opts.SetClientID(fmt.Sprintf("TEST-CLIENT-%d", i))
		client := mqtt.NewClient(opts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			t.Fatal(token.Error())
		}

		go fworker(client, taskQueue, wg)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

reqloop:
	for range count {
		select {
		case <-interrupt:
			break reqloop
		default:
			reqDataJSON, err := json.Marshal(Payload{
				ProjectID: projectID,
				TokenID:   tokenID,
				User: UserCredential{
					CollectionID: authCollectionID,
					ID:           authUserID,
				},
				CollectionID: targetCollectionID,
				Data: Data{
					IdleTime:  rand.Float64(),
					State:     states[rand.Intn(2)],
					Timestamp: time.Now(),
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			taskQueue <- reqDataJSON
		}
	}

	close(taskQueue)
	wg.Wait()

	store.PrintResult(t)
}
