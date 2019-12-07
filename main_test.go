package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMain_HandleRequests(t *testing.T) {
	req, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()

	handler := http.HandlerFunc(homePage)
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	expected := document{}
	json.Unmarshal(rr.Body.Bytes(), &expected)
	if bytes.Equal(expected.Byte, []byte{}) {
		t.Errorf("handler returned unexpected body: got %d want %d", expected.Byte, []byte("Hello world"))
	}
	if expected.Name != "Hello world" {
		t.Errorf("handler returned unexpected body: got %s want %s", expected.Name, "Hello world")
	}
}
