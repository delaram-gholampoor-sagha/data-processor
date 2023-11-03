package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestInputData(t *testing.T) {

	dataQueue = make(chan DataStruct, 1)
	userQuotas["testuser"] = &UserQuota{UserID: "testuser", RequestQuota: 100, DataQuota: 1 << 20}

	handler := http.HandlerFunc(inputData)

	data := DataStruct{UserID: "testuser", DataSize: 512}
	jsonData, _ := json.Marshal(data)
	req, _ := http.NewRequest("POST", "/input", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusAccepted {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusAccepted)
	}

}

func TestHash(t *testing.T) {
	input := "testString"
	expectedHash := hash(input)

	if hash(input) != expectedHash {
		t.Errorf("hash function is not returning consistent results")
	}

	differentInput := "differentTestString"
	if hash(differentInput) == expectedHash {
		t.Errorf("hash function is returning same results for different inputs")
	}
}

func TestInitializeUserQuotas(t *testing.T) {

	os.Setenv(userQuotasFileEnvVar, "test_quotas.json")

	testQuotas := map[string]*UserQuota{"testuser": {UserID: "testuser", RequestQuota: 100}}
	content, _ := json.Marshal(testQuotas)
	_ = os.WriteFile("test_quotas.json", content, 0644)
	defer os.Remove("test_quotas.json")

	initializeUserQuotas()

	if _, ok := userQuotas["testuser"]; !ok {
		t.Fatalf("Expected userQuotas to be initialized with 'testuser'")
	}
}

func TestStoreData(t *testing.T) {
	testData := DataStruct{
		UniqueID: "testID",
		UserID:   "testUser",
		DataSize: 100,
	}

	err := storeData(testData)
	if err != nil {
		t.Fatalf("storeData failed: %v", err)
	}

	filename := fmt.Sprintf("%s.txt", testData.UserID)
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Fatalf("storeData did not create the file: %s", filename)
	}

	defer os.Remove(filename)

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	if !strings.Contains(string(content), testData.UniqueID) {
		t.Errorf("File does not contain the correct data")
	}

}
