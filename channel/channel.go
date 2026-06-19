// Package httpchannel implements a synchronous HTTP request/response channel
// for OpenTalon. Clients POST a message with a profile token; the channel
// pushes the message into the core inbox and blocks the HTTP handler until
// the core calls Send with the final response. This is the same auth model
// used by websocket-channel (Metadata["profile_token"] → core verifier),
// but framed as one-shot request/response instead of a long-lived socket.
package httpchannel

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	pkg "github.com/opentalon/opentalon/pkg/channel"
)

// ID is the channel identifier.
const ID = "http"

// Config holds the HTTP server configuration.
type Config struct {
	Addr        string        // listening address, e.g. "0.0.0.0:9100"
	Path        string        // endpoint path, e.g. "/chat"
	CORSOrigins []string      // allowed origins; empty = allow all (dev mode)
	Timeout     time.Duration // max wait per request for the LLM response
}

// inboundBody is the JSON shape clients POST. Token may also be supplied via
// query (?token=) or Authorization: Bearer.
type inboundBody struct {
	Content        string      `json:"content"`
	ConversationID string      `json:"conversation_id,omitempty"`
	ThreadID       string      `json:"thread_id,omitempty"`
	Files          []fileFrame `json:"files,omitempty"`
}

type fileFrame struct {
	Name     string `json:"name"`
	MimeType string `json:"mime_type"`
	Data     string `json:"data"` // base64-encoded
}

// outboundFrame is what we write back to the HTTP client.
type outboundFrame struct {
	ConversationID string            `json:"conversation_id"`
	Content        string            `json:"content"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// pending tracks a single in-flight request waiting for the core's Send call.
type pending struct {
	resp chan pkg.OutboundMessage
}

// Channel is the OpenTalon channel implementation.
type Channel struct {
	cfg     Config
	inbox   chan<- pkg.InboundMessage
	srv     *http.Server
	pending sync.Map // conversationID → *pending
	stopMu  sync.Mutex
	stopped bool
	wg      sync.WaitGroup
}

// New returns a Channel with the given default config.
func New(cfg Config) *Channel {
	if cfg.Addr == "" {
		cfg.Addr = "0.0.0.0:9100"
	}
	if cfg.Path == "" {
		cfg.Path = "/chat"
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 120 * time.Second
	}
	return &Channel{cfg: cfg}
}

// Configure implements pkg.ConfigurableChannel.
func (c *Channel) Configure(config map[string]interface{}) error {
	if v, ok := config["addr"].(string); ok && v != "" {
		c.cfg.Addr = v
	}
	if v, ok := config["path"].(string); ok && v != "" {
		c.cfg.Path = v
	}
	if v, ok := config["timeout"].(string); ok && v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("parse timeout %q: %w", v, err)
		}
		c.cfg.Timeout = d
	}
	if origins, ok := config["cors_origins"].([]interface{}); ok {
		c.cfg.CORSOrigins = nil
		for _, o := range origins {
			if s, ok := o.(string); ok && s != "" {
				c.cfg.CORSOrigins = append(c.cfg.CORSOrigins, s)
			}
		}
	}
	return nil
}

// ID implements pkg.Channel.
func (c *Channel) ID() string { return ID }

// Capabilities implements pkg.Channel.
func (c *Channel) Capabilities() pkg.Capabilities {
	return pkg.Capabilities{
		ID:               ID,
		Name:             "HTTP",
		Files:            true,
		Threads:          false,
		Reactions:        false,
		Edits:            false,
		MaxMessageLength: 64 * 1024,
		ResponseFormat:   pkg.FormatMarkdown,
	}
}

// Start implements pkg.Channel.
func (c *Channel) Start(ctx context.Context, inbox chan<- pkg.InboundMessage) error {
	c.inbox = inbox

	mux := http.NewServeMux()
	mux.HandleFunc(c.cfg.Path, c.handleChat)

	c.srv = &http.Server{
		Addr:              c.cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		if err := c.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http channel: server error", "error", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.srv.Shutdown(shutCtx)
	}()

	slog.Info("http channel: listening", "addr", c.cfg.Addr, "path", c.cfg.Path)
	return nil
}

// Send implements pkg.Channel. The core delivers the LLM response here; we
// route it back to the HTTP handler waiting on the matching ConversationID.
// Typing indicator frames (_typing=true metadata) are dropped — HTTP is one
// request, one response.
func (c *Channel) Send(_ context.Context, msg pkg.OutboundMessage) error {
	if msg.Metadata["_typing"] == "true" {
		return nil
	}
	v, ok := c.pending.Load(msg.ConversationID)
	if !ok {
		// HTTP client already gave up (timeout / disconnect). Not an error.
		slog.Debug("http channel: response for unknown conversation", "conv", msg.ConversationID)
		return nil
	}
	p := v.(*pending)
	select {
	case p.resp <- msg:
	default:
		// Buffer full — duplicate Send for same conversation (shouldn't
		// happen for non-streaming flows). Drop and log.
		slog.Warn("http channel: dropping duplicate response", "conv", msg.ConversationID)
	}
	return nil
}

// Stop implements pkg.Channel.
func (c *Channel) Stop() error {
	if c.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.srv.Shutdown(ctx)
	}
	c.stopMu.Lock()
	c.stopped = true
	c.stopMu.Unlock()
	c.wg.Wait()
	return nil
}

// handleChat is the POST {path} handler.
func (c *Channel) handleChat(w http.ResponseWriter, r *http.Request) {
	c.applyCORS(w, r)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := extractToken(r)
	if token == "" {
		http.Error(w, "token required", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var in inboundBody
	if err := json.Unmarshal(body, &in); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if in.Content == "" && len(in.Files) == 0 {
		http.Error(w, "content or files required", http.StatusBadRequest)
		return
	}

	// Resume if the client supplied a conversation_id; mint a new one
	// otherwise (this is the same split websocket-channel uses).
	convID := in.ConversationID
	resume := convID != ""
	if convID == "" {
		convID = newID()
	}

	c.stopMu.Lock()
	if c.stopped {
		c.stopMu.Unlock()
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	c.stopMu.Unlock()

	p := &pending{resp: make(chan pkg.OutboundMessage, 1)}
	if _, loaded := c.pending.LoadOrStore(convID, p); loaded {
		// Concurrent request for the same conversation_id. Reject rather
		// than racing — clients should serialise their own turns.
		http.Error(w, "conversation already in flight", http.StatusConflict)
		return
	}
	defer c.pending.Delete(convID)

	meta := map[string]string{"profile_token": token}
	if resume {
		meta[pkg.ResumeIntentMetadataKey] = "true"
	}

	msg := pkg.InboundMessage{
		ChannelID:      ID,
		ConversationID: convID,
		ThreadID:       in.ThreadID,
		SenderID:       convID,
		Content:        in.Content,
		Metadata:       meta,
		Timestamp:      time.Now(),
	}
	for _, f := range in.Files {
		decoded, err := base64.StdEncoding.DecodeString(f.Data)
		if err != nil {
			http.Error(w, "base64 decode "+f.Name+": "+err.Error(), http.StatusBadRequest)
			return
		}
		msg.Files = append(msg.Files, pkg.FileAttachment{
			Name:     f.Name,
			MimeType: f.MimeType,
			Data:     decoded,
			Size:     int64(len(decoded)),
		})
	}

	// Push to core; respect client cancellation and our own timeout.
	timeoutCtx, cancel := context.WithTimeout(r.Context(), c.cfg.Timeout)
	defer cancel()

	select {
	case c.inbox <- msg:
	case <-timeoutCtx.Done():
		http.Error(w, "core inbox unavailable", http.StatusGatewayTimeout)
		return
	}

	select {
	case out := <-p.resp:
		writeJSON(w, http.StatusOK, outboundFrame{
			ConversationID: out.ConversationID,
			Content:        out.Content,
			Metadata:       safeMeta(out.Metadata),
		})
	case <-timeoutCtx.Done():
		http.Error(w, "timed out waiting for response", http.StatusGatewayTimeout)
		return
	}
}

// extractToken pulls the profile token from query string, Authorization
// header, or X-Profile-Token header. Matches the websocket-channel contract.
func extractToken(r *http.Request) string {
	if t := r.URL.Query().Get("token"); t != "" {
		return t
	}
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ")
	}
	if t := r.Header.Get("X-Profile-Token"); t != "" {
		return t
	}
	return ""
}

func (c *Channel) applyCORS(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return
	}
	allow := len(c.cfg.CORSOrigins) == 0 // empty = allow all (dev mode)
	if !allow {
		for _, o := range c.cfg.CORSOrigins {
			if o == origin || o == "*" {
				allow = true
				break
			}
		}
	}
	if !allow {
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Vary", "Origin")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Profile-Token")
}

// safeMeta strips internal keys before echoing metadata to the client.
func safeMeta(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if k == "_typing" || k == "profile_token" {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}
