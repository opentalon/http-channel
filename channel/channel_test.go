package httpchannel

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pkg "github.com/opentalon/opentalon/pkg/channel"
)

func TestID(t *testing.T) {
	if ID != "http" {
		t.Errorf("ID = %q, want %q", ID, "http")
	}
}

func TestNew_defaults(t *testing.T) {
	ch := New(Config{})
	if ch.cfg.Addr != "0.0.0.0:9100" {
		t.Errorf("default Addr = %q", ch.cfg.Addr)
	}
	if ch.cfg.Path != "/chat" {
		t.Errorf("default Path = %q", ch.cfg.Path)
	}
	if ch.cfg.Timeout != 120*time.Second {
		t.Errorf("default Timeout = %v", ch.cfg.Timeout)
	}
}

func TestConfigure(t *testing.T) {
	ch := New(Config{})
	err := ch.Configure(map[string]interface{}{
		"addr":         "127.0.0.1:0",
		"path":         "/api/chat",
		"timeout":      "30s",
		"cors_origins": []interface{}{"https://example.com"},
	})
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if ch.cfg.Addr != "127.0.0.1:0" {
		t.Errorf("Addr not applied: %q", ch.cfg.Addr)
	}
	if ch.cfg.Path != "/api/chat" {
		t.Errorf("Path not applied: %q", ch.cfg.Path)
	}
	if ch.cfg.Timeout != 30*time.Second {
		t.Errorf("Timeout not applied: %v", ch.cfg.Timeout)
	}
	if len(ch.cfg.CORSOrigins) != 1 || ch.cfg.CORSOrigins[0] != "https://example.com" {
		t.Errorf("CORSOrigins not applied: %v", ch.cfg.CORSOrigins)
	}
}

func TestConfigure_badTimeout(t *testing.T) {
	ch := New(Config{})
	if err := ch.Configure(map[string]interface{}{"timeout": "nope"}); err == nil {
		t.Error("expected error for bad timeout, got nil")
	}
}

func TestCapabilities(t *testing.T) {
	caps := New(Config{}).Capabilities()
	if caps.ID != ID || caps.Name != "HTTP" {
		t.Errorf("unexpected capabilities: %+v", caps)
	}
	if !caps.Files {
		t.Error("Files should be true")
	}
	if caps.ResponseFormat != pkg.FormatMarkdown {
		t.Errorf("ResponseFormat = %q", caps.ResponseFormat)
	}
}

// fakeChannel wires the channel to an inbox we drive from the test, and a
// "Send" path the channel uses to reply.
func startChannel(t *testing.T) (*Channel, chan pkg.InboundMessage, *httptest.Server) {
	t.Helper()
	ch := New(Config{Timeout: 2 * time.Second})
	inbox := make(chan pkg.InboundMessage, 4)
	ch.inbox = inbox
	srv := httptest.NewServer(http.HandlerFunc(ch.handleChat))
	t.Cleanup(srv.Close)
	return ch, inbox, srv
}

func TestHandleChat_missingToken(t *testing.T) {
	_, _, srv := startChannel(t)
	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(`{"content":"hi"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestHandleChat_emptyBody(t *testing.T) {
	_, _, srv := startChannel(t)
	req, _ := http.NewRequest("POST", srv.URL+"?token=t", strings.NewReader(`{}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

// End-to-end: POST a message, simulate the core picking it up off the inbox
// and calling Send. Assert the HTTP response carries the reply.
func TestHandleChat_roundtrip(t *testing.T) {
	ch, inbox, srv := startChannel(t)

	// Core simulator: read the inbound, then call Send back with the same
	// conversation_id and a known reply.
	go func() {
		msg := <-inbox
		if msg.Metadata["profile_token"] != "tok-xyz" {
			t.Errorf("token not forwarded: %q", msg.Metadata["profile_token"])
		}
		_ = ch.Send(context.Background(), pkg.OutboundMessage{
			ConversationID: msg.ConversationID,
			Content:        "hello back",
			Metadata:       map[string]string{"profile_token": "leak", "_typing": "false"},
		})
	}()

	req, _ := http.NewRequest("POST", srv.URL, strings.NewReader(`{"content":"hi"}`))
	req.Header.Set("Authorization", "Bearer tok-xyz")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var out outboundFrame
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Content != "hello back" {
		t.Errorf("content = %q", out.Content)
	}
	if out.ConversationID == "" {
		t.Error("conversation_id empty in response")
	}
	if _, leaked := out.Metadata["profile_token"]; leaked {
		t.Error("profile_token leaked to client")
	}
}

func TestHandleChat_resume(t *testing.T) {
	ch, inbox, srv := startChannel(t)

	go func() {
		msg := <-inbox
		if msg.ConversationID != "conv-42" {
			t.Errorf("conversation_id = %q, want conv-42", msg.ConversationID)
		}
		if msg.Metadata[pkg.ResumeIntentMetadataKey] != "true" {
			t.Errorf("resume_intent not set: %q", msg.Metadata[pkg.ResumeIntentMetadataKey])
		}
		_ = ch.Send(context.Background(), pkg.OutboundMessage{
			ConversationID: msg.ConversationID, Content: "ok",
		})
	}()

	body, _ := json.Marshal(inboundBody{Content: "hi", ConversationID: "conv-42"})
	req, _ := http.NewRequest("POST", srv.URL+"?token=t", bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestHandleChat_timeout(t *testing.T) {
	ch := New(Config{Timeout: 50 * time.Millisecond})
	ch.inbox = make(chan pkg.InboundMessage, 1) // inbox accepts message; nobody Sends back.
	srv := httptest.NewServer(http.HandlerFunc(ch.handleChat))
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"?token=t", strings.NewReader(`{"content":"hi"}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want 504", resp.StatusCode)
	}
}

func TestHandleChat_conflict(t *testing.T) {
	_, inbox, srv := startChannel(t)

	// First request: hold it open by NOT consuming the response side.
	pendingDone := make(chan struct{})
	go func() {
		<-inbox // consume so the inbox-send returns; then never call Send.
		<-pendingDone
	}()

	body, _ := json.Marshal(inboundBody{Content: "hi", ConversationID: "conv-1"})
	req1, _ := http.NewRequest("POST", srv.URL+"?token=t", bytes.NewReader(body))
	go func() { _, _ = http.DefaultClient.Do(req1) }() // fire and forget

	// Give the first request a moment to register the pending entry.
	time.Sleep(20 * time.Millisecond)

	req2, _ := http.NewRequest("POST", srv.URL+"?token=t", bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status = %d, want 409", resp.StatusCode)
	}
	close(pendingDone)
}

func TestExtractToken(t *testing.T) {
	cases := []struct {
		name string
		set  func(r *http.Request)
		want string
	}{
		{"query", func(r *http.Request) { r.URL.RawQuery = "token=q" }, "q"},
		{"bearer", func(r *http.Request) { r.Header.Set("Authorization", "Bearer b") }, "b"},
		{"x-header", func(r *http.Request) { r.Header.Set("X-Profile-Token", "x") }, "x"},
		{"none", func(r *http.Request) {}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/", nil)
			tc.set(r)
			if got := extractToken(r); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSafeMeta(t *testing.T) {
	in := map[string]string{
		"_typing":       "true",
		"profile_token": "leak",
		"keep":          "ok",
	}
	out := safeMeta(in)
	if _, ok := out["_typing"]; ok {
		t.Error("_typing not stripped")
	}
	if _, ok := out["profile_token"]; ok {
		t.Error("profile_token not stripped")
	}
	if out["keep"] != "ok" {
		t.Error("keep stripped")
	}
}

func TestSafeMeta_empty(t *testing.T) {
	if safeMeta(nil) != nil {
		t.Error("nil input should return nil")
	}
	if safeMeta(map[string]string{"_typing": "true"}) != nil {
		t.Error("only-internal input should return nil")
	}
}

func TestSend_unknownConv(t *testing.T) {
	ch := New(Config{})
	// No pending entry — Send should be a no-op, no panic.
	if err := ch.Send(context.Background(), pkg.OutboundMessage{ConversationID: "nope", Content: "x"}); err != nil {
		t.Errorf("Send returned err: %v", err)
	}
}
