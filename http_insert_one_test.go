package hyperbasebench_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Environment variables:
//
// - BASE_URL: string
// - PROJECT_ID: string
// - TOKEN_ID: string
// - TOKEN: string
// - AUTH_COLLECTION_ID: string
// - AUTH_USERNAME: string
// - AUTH_PASSWORD: string
// - TARGET_COLLECTION_ID: string
// - DURATION: duration string
// - PARALLEL: integer

func TestInsertOne(t *testing.T) {
	states := []string{"Active", "Idle"}

	baseURL, ok := os.LookupEnv("BASE_URL")
	if !ok {
		t.Fatal("BASE_URL does not exist")
	}
	projectID, ok := os.LookupEnv("PROJECT_ID")
	if !ok {
		t.Fatal("PROJECT_ID does not exist")
	}
	tokenIDStr, ok := os.LookupEnv("TOKEN_ID")
	if !ok {
		t.Fatal("TOKEN_ID does not exist")
	}
	tokenID, err := uuid.Parse(tokenIDStr)
	if err != nil {
		t.Fatal(err)
	}
	token, ok := os.LookupEnv("TOKEN")
	if !ok {
		t.Fatal("TOKEN does not exist")
	}
	authCollectionIDStr, ok := os.LookupEnv("AUTH_COLLECTION_ID")
	if !ok {
		t.Fatal("AUTH_COLLECTION_ID does not exist")
	}
	authCollectionID, err := uuid.Parse(authCollectionIDStr)
	if err != nil {
		t.Fatal(err)
	}
	authUsername, ok := os.LookupEnv("AUTH_USERNAME")
	if !ok {
		t.Fatal("AUTH_USERNAME does not exist")
	}
	authPassword, ok := os.LookupEnv("AUTH_PASSWORD")
	if !ok {
		t.Fatal("AUTH_PASSWORD does not exist")
	}
	targetCollectionID, ok := os.LookupEnv("TARGET_COLLECTION_ID")
	if !ok {
		t.Fatal("TARGET_COLLECTION_ID does not exist")
	}
	durationStr, ok := os.LookupEnv("DURATION")
	if !ok {
		t.Fatal("LOOP does not exist")
	}
	duration, err := time.ParseDuration(durationStr)
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

	authData := AuthReq{
		TokenID:      tokenID,
		Token:        token,
		CollectionID: authCollectionID,
		Data: AuthCredential{
			Username: authUsername,
			Password: authPassword,
		},
	}

	client := http.Client{}

	authToken := getAuthToken(t, baseURL, &client, authData)

	store := Store{}

	finsert := func(req *http.Request) {
		tStart := time.Now()
		res, err := client.Do(req)
		if err != nil {
			t.Error(err)
		}
		tEnd := time.Now()
		defer res.Body.Close()

		resBytes, err := io.ReadAll(res.Body)
		if err != nil {
			t.Error(err)
		}

		var dataRes Res[Data]

		if err := json.Unmarshal(resBytes, &dataRes); err != nil {
			t.Error(err)
		}

		success := false
		if dataRes.Data.ID != "" {
			success = true
		}

		store.Append(tEnd.Sub(tStart), success)
	}

	fworker := func(reqs <-chan *http.Request) {
		for req := range reqs {
			finsert(req)
		}
	}

	taskQueue := make(chan *http.Request, parallel)

	for range parallel {
		go fworker(taskQueue)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	tStart := time.Now()
reqloop:
	for {
		select {
		case <-interrupt:
			break reqloop
		default:
			if time.Since(tStart) > duration {
				break reqloop
			}

			reqData := Data{
				IdleTime:  rand.Float64(),
				State:     states[rand.Intn(2)],
				Timestamp: time.Now(),
			}

			reqDataJSON, err := json.Marshal(reqData)
			if err != nil {
				t.Fatal(err)
			}

			req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/rest/project/%s/collection/%s/record", baseURL, projectID, targetCollectionID), bytes.NewReader(reqDataJSON))
			if err != nil {
				t.Fatal(err)
			}

			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", authToken))

			taskQueue <- req
		}
	}

	store.PrintResult(t)
}
