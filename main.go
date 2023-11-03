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
	numWorkers            = 5                // Number of workers processing data concurrently
	dataQueueBuffer       = 100              // Buffer size of the dataQueue channel
	saveInterval          = 10 * time.Second // Interval at which user quota state is saved
	ttlDuration           = 5 * time.Minute  // TTL for each entry in dataMap
	dataCleanupThreshold  = 24 * time.Hour   // Threshold after which data should be cleaned up
	maxDataSize           = 1 << 20          // Max size of data (for example 1MB)
	quotaFile             = "user_quotas.json"
	userQuotasFileEnvVar  = "USER_QUOTAS_FILE" // Environment variable for user quotas file
	defaultUserQuotasFile = "user_quotas.json" // Default file for user quotas
)

var (
	userQuotas        = make(map[string]*UserQuota)
	userQuotasMutex   sync.RWMutex
	dataMap           sync.Map
	dataQueue         = make(chan DataStruct, dataQueueBuffer) // Buffered channel for data processing
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

	err = json.Unmarshal(contents, &userQuotas)
	if err != nil {
		log.Fatal("Failed to unmarshal user quotas:", err)
	}

	// Since this is initialization, we assume no concurrent access and thus no need to lock.
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

		// After processing, store the data
		if err := storeData(data); err != nil {
			logError(fmt.Sprintf("Worker %d", id), err)
			continue
		}
	}
}

func processData(data DataStruct) error {
	// Adjusted the processData function to work with existing fields
	// For the sake of example, let's just log that we've processed the data
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
	// Here we are just writing to a file, but you might be storing to a database or elsewhere.
	filename := fmt.Sprintf("%s.txt", data.UserID)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	if _, err := file.WriteString(fmt.Sprintf("%v\n", data)); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	// Store the data in the dataMap with a timestamp
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

	if err := ioutil.WriteFile(userQuotasFile, userQuotasJSON, 0644); err != nil {
		return err
	}

	return nil
}

func getActualContent(uniqueID string) string {
	// Replace this with the actual logic to retrieve content.
	return "Actual content based on uniqueID"
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
			// Perform a final save when shutting down
			if err := saveUserQuotasState(); err != nil {
				logError("Failed to save user quotas on shutdown", err)
			}
			return
		}
	}
}

func inputData(w http.ResponseWriter, r *http.Request) {
	// Check if the server is shutting down
	if atomic.LoadInt32(&shutdownInitiated) == 1 {
		http.Error(w, "Server is shutting down", http.StatusServiceUnavailable)
		return
	}

	// Only allow POST method
	if r.Method != "POST" {
		http.Error(w, "Only POST method is accepted", http.StatusMethodNotAllowed)
		return
	}

	// Ensure the body is closed after the function returns
	defer r.Body.Close()

	// Decode the incoming data
	var data DataStruct
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		http.Error(w, "Invalid data format", http.StatusBadRequest)
		return
	}

	// Check for duplicate data
	if entry, exists := dataMap.Load(data.UniqueID); exists {
		// We can be more informative here. For example, if you store the time the data was processed,
		// you could return that information to the user.
		entryData, ok := entry.(DataMapEntry)
		if ok {
			response := fmt.Sprintf(
				"Duplicate data: already processed at %v",
				entryData.Timestamp.Format(time.RFC3339),
			)
			http.Error(w, response, http.StatusConflict) // 409 Conflict might be more appropriate
		} else {
			http.Error(w, "Duplicate data", http.StatusConflict) // Fallback message
		}
		return
	}

	// Retrieve and update the user's quota
	userQuota, ok := userQuotas[data.UserID]
	if !ok {
		errMsg := "User not found"
		logError("inputData", fmt.Errorf(errMsg))
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}

	userQuota.mu.Lock()
	defer userQuota.mu.Unlock()

	// Check if the user has exceeded their request or data quota
	if userQuota.RequestCount >= userQuota.RequestQuota {
		http.Error(w, "Request quota exceeded", http.StatusForbidden)
		return
	}

	if userQuota.DataUsed+data.DataSize > userQuota.DataQuota {
		http.Error(w, "Data quota exceeded", http.StatusForbidden)
		return
	}

	// Increment the request count and data used
	userQuota.RequestCount++
	userQuota.DataUsed += data.DataSize

	// Attempt to enqueue the data for processing
	select {
	case dataQueue <- data:
		w.WriteHeader(http.StatusAccepted)
	default:
		// If the queue is full, return a server busy error
		http.Error(w, "Server is too busy", http.StatusServiceUnavailable)
		return
	}
}

func main() {

	// Set the cleanup interval to 1 hour, for example
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

	// Periodic user quotas save function
	ctx, cancel := context.WithCancel(context.Background())
	go saveUserQuotasPeriodically(ctx)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

		<-signals
		atomic.StoreInt32(&shutdownInitiated, 1)

		// Cancel the context to stop the periodic saving
		cancel()

		// Wait for the user quotas update to finish
		quotaUpdateWG.Wait()

		// Shutdown the server with a timeout
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

// New function to wait for quota updates to finish
func waitForQuotaUpdatesToFinish() {
	quotaUpdateWG.Wait()
}

// A function that runs periodically to clean up old entries in dataMap
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
					// Value is not a DataMapEntry, which is unexpected,
					// so remove this key-value pair from the map
					dataMap.Delete(key)
					return true
				}

				// Cast the value to DataMapEntry and then check the Timestamp
				if now.Sub(entry.Timestamp) > ttlDuration {
					dataMap.Delete(key)
				}
				return true // Continue iteration
			})
		}
	}
}
