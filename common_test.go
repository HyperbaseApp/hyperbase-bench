package hyperbasebench_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

const (
	BytesPerMiB = 1 << 20 // 1 MiB is 2 raised to the power of 20 bytes
)

type ErrorRes struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}
type Res[T any] struct {
	Data  T        `json:"data"`
	Error ErrorRes `json:"error"`
}
type AuthCredential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
type AuthReq struct {
	TokenID      uuid.UUID      `json:"token_id"`
	Token        string         `json:"token"`
	CollectionID uuid.UUID      `json:"collection_id"`
	Data         AuthCredential `json:"data"`
}
type AuthRes struct {
	Token string `json:"token"`
}

type Data struct {
	ID        string    `json:"_id,omitempty"`
	IdleTime  float64   `json:"idle_time"`
	State     string    `json:"state"`
	Timestamp time.Time `json:"timestamp"`
}

type Result struct {
	Duration      time.Duration
	RAMUsage      uint64
	CPUPercentage float64
	Success       bool
}

type Store struct {
	mu      sync.Mutex
	Results []Result
}

func (s *Store) Append(d time.Duration, success bool) {
	s.mu.Lock()
	v, _ := mem.VirtualMemory()
	cpuPercentages, _ := cpu.Percent(0, false)
	s.Results = append(s.Results, Result{d, v.Used, cpuPercentages[0], success})
	s.mu.Unlock()
}

func (s *Store) PrintResult(t *testing.T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.Results) == 0 {
		return
	}

	len := len(s.Results)

	totalDuration := 0
	highestDuration := s.Results[0].Duration
	lowestDuration := s.Results[0].Duration

	totalCPUPercentage := 0.0
	highestCPUPercentage := s.Results[0].CPUPercentage
	lowestCPUPercentage := s.Results[0].CPUPercentage

	totalRAMUsage := uint64(0)
	highestRAMUsage := uint64(s.Results[0].RAMUsage)
	lowestRAMUsage := uint64(s.Results[0].RAMUsage)

	totalError := 0
	for _, r := range s.Results {
		totalDuration += int(r.Duration)
		totalCPUPercentage += r.CPUPercentage
		totalRAMUsage += r.RAMUsage
		if !r.Success {
			totalError += 1
		}
		if r.Duration > highestDuration {
			highestDuration = r.Duration
		} else if r.Duration < lowestDuration {
			lowestDuration = r.Duration
		}
		if r.CPUPercentage > highestCPUPercentage {
			highestCPUPercentage = r.CPUPercentage
		} else if r.CPUPercentage < lowestCPUPercentage {
			lowestCPUPercentage = r.CPUPercentage
		}
		if r.RAMUsage > highestRAMUsage {
			highestRAMUsage = r.RAMUsage
		} else if r.RAMUsage < lowestRAMUsage {
			lowestRAMUsage = r.RAMUsage
		}
	}
	averageDuration := time.Duration(totalDuration / len)
	averageCPUPercentage := totalCPUPercentage / float64(len)
	averageRAMUsage := totalRAMUsage / uint64(len)

	t.Logf(`
Total Duration: %v
Total Request: %d

Average Duration: %v
Highest Duration: %v
Lowest Duration: %v

Average CPU: %f %%
Highest CPU: %f %%
Lowest CPU: %f %%

Average RAM: %d MiB
Highest RAM: %d MiB
Lowest RAM: %d MiB

Total Error: %d`,
		time.Duration(totalDuration),
		len,

		averageDuration,
		highestDuration,
		lowestDuration,

		averageCPUPercentage,
		highestCPUPercentage,
		lowestCPUPercentage,

		averageRAMUsage/BytesPerMiB,
		highestRAMUsage/BytesPerMiB,
		lowestRAMUsage/BytesPerMiB,

		totalError,
	)
}

func getAuthToken(t *testing.T, baseURL string, client *http.Client, authData AuthReq) string {
	authDataJSON, err := json.Marshal(authData)
	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/rest/auth/token-based", baseURL), bytes.NewReader(authDataJSON))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Add("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	resBytes, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}

	var authRes Res[AuthRes]

	if err := json.Unmarshal(resBytes, &authRes); err != nil {
		t.Fatal(err)
	}

	if authRes.Error.Status != "" {
		t.Fatal(authRes.Error.Message)
	}

	return authRes.Data.Token
}
