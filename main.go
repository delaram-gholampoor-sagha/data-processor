package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type DataStruct struct {
	UniqueID string `json:"unique_id"`
	UserID   string `json:"user_id"`
	DataSize int    `json:"data_size"`
}

type UserQuota struct {
	UserID       string `json:"user_id"`
	RequestQuota int    `json:"request_quota"`
	RequestCount int    `json:"request_count"`
	DataQuota    int    `json:"data_quota"`
	DataUsed     int    `json:"data_used"`
	mu           sync.Mutex
}

type StoredData struct {
	UniqueID string `json:"unique_id"`
	UserID   string `json:"user_id"`
	Content  string `json:"content"`
}

type DataMapEntry struct {
	Timestamp time.Time
	Data      interface{}
}

const (
	numWorkers            = 5
	dataQueueBuffer       = 100
	saveInterval          = 10 * time.Second
	ttlDuration           = 5 * time.Minute
	dataCleanupThreshold  = 24 * time.Hour
	maxDataSize           = 1 << 20
	quotaFile             = "user_quotas.json"
	userQuotasFileEnvVar  = "USER_QUOTAS_FILE"
	defaultUserQuotasFile = "user_quotas.json"
)

var (
	userQuotas        = make(map[string]*UserQuota)
	userQuotasMutex   sync.RWMutex
	dataMap           sync.Map
	dataQueue         = make(chan DataStruct, dataQueueBuffer)
	shutdownInitiated int32
	quotaUpdateWG     sync.WaitGroup
	userQuotasFile    string
	dataMapMutex      sync.Mutex
)

func logError(context string, err error) {
	if err != nil {
		log.Printf("%s: %v\n", context, err)
	}
}
func initializeUserQuotas() {
	userQuotasFile = os.Getenv(userQuotasFileEnvVar)
	if userQuotasFile == "" {
		userQuotasFile = defaultUserQuotasFile
	}

	contents, err := ioutil.ReadFile(userQuotasFile)
	if err != nil {
		log.Fatal("Failed to read user quotas:", err)
	}

	if len(contents) == 0 {
		log.Fatal("User quotas file is empty")
	}

	fmt.Printf("User Quotas File Contents: %s\n", contents) // Additional logging to debug

	err = json.Unmarshal(contents, &userQuotas)
	if err != nil {
		log.Fatal("Failed to unmarshal user quotas:", err)
	}

	for userID, quota := range userQuotas {
		quota.RequestCount = 0
		quota.DataUsed = 0
		userQuotas[userID] = quota
	}
}

func worker(id int, wg *sync.WaitGroup, dataQueue chan DataStruct) {
	defer wg.Done()
	for data := range dataQueue {
		fmt.Printf("Worker %d processing data\n", id)
		if err := processData(data); err != nil {
			logError(fmt.Sprintf("Worker %d", id), err)
			continue
		}

		if err := storeData(data); err != nil {
			logError(fmt.Sprintf("Worker %d", id), err)
			continue
		}
	}
}

func processData(data DataStruct) error {

	log.Printf("Processed data for user %s with unique ID %s", data.UserID, data.UniqueID)
	return nil
}

func hash(s string) uint64 {
	h := fnv.New64a()
	_, err := h.Write([]byte(s))
	if err != nil {
		log.Println("Error hashing:", err)
	}
	return h.Sum64()
}

func storeData(data DataStruct) error {

	filename := fmt.Sprintf("%s.txt", data.UserID)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	if _, err := file.WriteString(fmt.Sprintf("%v\n", data)); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	dataMap.Store(hash(data.UniqueID), DataMapEntry{Timestamp: time.Now(), Data: data})

	return nil
}

func saveUserQuotasState() error {
	userQuotasMutex.RLock()
	defer userQuotasMutex.RUnlock()

	userQuotasJSON, err := json.Marshal(userQuotas)
	if err != nil {
		return err
	}

	if err := os.WriteFile(userQuotasFile, userQuotasJSON, 0644); err != nil {
		return err
	}

	return nil
}

func saveUserQuotasPeriodically(ctx context.Context) {
	ticker := time.NewTicker(saveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := saveUserQuotasState(); err != nil {
				logError("Failed to save user quotas", err)
			}
		case <-ctx.Done():

			if err := saveUserQuotasState(); err != nil {
				logError("Failed to save user quotas on shutdown", err)
			}
			return
		}
	}
}

func inputData(w http.ResponseWriter, r *http.Request) {

	if atomic.LoadInt32(&shutdownInitiated) == 1 {
		http.Error(w, "Server is shutting down", http.StatusServiceUnavailable)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Only POST method is accepted", http.StatusMethodNotAllowed)
		return
	}

	defer r.Body.Close()

	var data DataStruct
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		http.Error(w, "Invalid data format", http.StatusBadRequest)
		return
	}

	if entry, exists := dataMap.Load(data.UniqueID); exists {

		entryData, ok := entry.(DataMapEntry)
		if ok {
			response := fmt.Sprintf(
				"Duplicate data: already processed at %v",
				entryData.Timestamp.Format(time.RFC3339),
			)
			http.Error(w, response, http.StatusConflict)
		} else {
			http.Error(w, "Duplicate data", http.StatusConflict)
		}
		return
	}

	userQuota, ok := userQuotas[data.UserID]
	if !ok {
		errMsg := "User not found"
		logError("inputData", fmt.Errorf(errMsg))
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}

	userQuota.mu.Lock()
	defer userQuota.mu.Unlock()

	if userQuota.RequestCount >= userQuota.RequestQuota {
		http.Error(w, "Request quota exceeded", http.StatusForbidden)
		return
	}

	if userQuota.DataUsed+data.DataSize > userQuota.DataQuota {
		http.Error(w, "Data quota exceeded", http.StatusForbidden)
		return
	}

	userQuota.RequestCount++
	userQuota.DataUsed += data.DataSize

	select {
	case dataQueue <- data:
		w.WriteHeader(http.StatusAccepted)
	default:

		http.Error(w, "Server is too busy", http.StatusServiceUnavailable)
		return
	}
}

func main() {

	cleanupInterval := 1 * time.Hour
	go cleanupDataMap(cleanupInterval)
	initializeUserQuotas()
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i, &wg, dataQueue)
	}
	http.HandleFunc("/input", inputData)
	cfg := &tls.Config{
		MinVersion:               tls.VersionTLS12,
		PreferServerCipherSuites: true,
	}
	srv := &http.Server{
		Addr:         ":8080",
		Handler:      nil,
		TLSConfig:    cfg,
		TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler), 0),
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	go saveUserQuotasPeriodically(ctx)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

		<-signals
		atomic.StoreInt32(&shutdownInitiated, 1)

		cancel()

		quotaUpdateWG.Wait()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP server Shutdown: %v", err)
		}
	}()

	log.Println("Server is starting...")
	err := srv.ListenAndServeTLS("server.crt", "server.key")
	if err != http.ErrServerClosed {
		log.Fatalf("HTTP server ListenAndServe: %v", err)
	}

	log.Println("Server stopped")
}

func waitForQuotaUpdatesToFinish() {
	quotaUpdateWG.Wait()
}

func cleanupDataMap(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			dataMap.Range(func(key, value interface{}) bool {
				entry, ok := value.(DataMapEntry)
				if !ok {

					dataMap.Delete(key)
					return true
				}

				if now.Sub(entry.Timestamp) > ttlDuration {
					dataMap.Delete(key)
				}
				return true
			})
		}
	}
}
