# http-channel

[![CI](https://github.com/opentalon/http-channel/actions/workflows/ci.yml/badge.svg)](https://github.com/opentalon/http-channel/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/opentalon/http-channel)](https://goreportcard.com/report/github.com/opentalon/http-channel)

Synchronous HTTP request/response channel for [OpenTalon](https://github.com/opentalon/opentalon). Clients `POST` a message with a profile token and get the LLM's reply in the HTTP response body. Same auth model as [websocket-channel](https://github.com/opentalon/websocket-channel) — one shot instead of a long-lived socket.

Use this when you want LLM access from `curl`, a server-side cron, or a backend service that prefers a plain REST call over a streaming socket.

## How it works

1. OpenTalon starts the binary as a subprocess.
2. Client `POST`s to `http://host:9100/chat` with the profile token in `Authorization: Bearer …` (or `?token=…`, or `X-Profile-Token: …`).
3. The channel injects the token into the `InboundMessage` as `metadata["profile_token"]` and pushes it to core.
4. OpenTalon's [Profiles & WhoAmI](https://github.com/opentalon/opentalon/blob/master/docs/profiles.md) verifier resolves the token → `entity_id`, `group`, model, limits, MCP credentials — the same flow every other channel uses.
5. The orchestrator runs the message (session, memory, tools, limits all apply), then calls the channel's `Send` with the final answer.
6. The HTTP handler unblocks and returns the answer as JSON.

The session is scoped to the verified profile, so two clients posting with different tokens never see each other's history.

## OpenTalon config

```yaml
channels:
  http:
    enabled: true
    # Local binary (after `go build -o http-channel .`):
    plugin: "./channels/http-channel/http-channel"
    # Or fetch from GitHub:
    # github: "opentalon/http-channel"
    # ref: "master"
    config:
      addr: "0.0.0.0:9100"   # host:port to listen on
      path: "/chat"           # endpoint path
      timeout: "120s"         # max wait per request for the LLM response
      cors_origins:           # omit to allow all (dev only)
        - "https://mysite.com"
```

## Wire protocol

### Request

```
POST /chat HTTP/1.1
Authorization: Bearer <profile_token>
Content-Type: application/json

{
  "content": "Summarise this document.",
  "conversation_id": "optional — set to resume a previous chat",
  "thread_id": "optional",
  "files": [
    { "name": "report.pdf", "mime_type": "application/pdf", "data": "<base64>" }
  ]
}
```

Token sources tried in order: `?token=…` query, `Authorization: Bearer …`, `X-Profile-Token: …`. Requests without a token return **401**.

Omitting `conversation_id` starts a fresh conversation (the server mints one and returns it). Supplying one triggers the strict-resume path in core: if that session no longer exists the response carries `metadata.error_code = "session_expired"` so the client can drop the stale id and retry.

### Response

```json
{
  "conversation_id": "3f2a1b…",
  "content": "The document says…",
  "metadata": { "...": "..." }
}
```

`content` is Markdown (matches the channel's declared `response_format`). `conversation_id` is what to pass back on the next call to continue the same session.

### Errors

| Status | When |
|--------|------|
| 400 | Missing `content`/`files`, malformed JSON, bad base64 |
| 401 | No token |
| 405 | Non-POST |
| 409 | Another request for the same `conversation_id` is in flight (serialise turns client-side) |
| 503 | Channel is shutting down |
| 504 | Timeout waiting for the LLM response (see `timeout` config) |

Application-level errors (auth failure at WhoAmI, token-limit exceeded, session expired, etc.) come back as a **200** with a normal response body — `metadata.error_code` carries the machine-readable code. This mirrors the contract every other OpenTalon channel uses so frontend recovery code can stay channel-agnostic.

## Build

```bash
git clone https://github.com/opentalon/http-channel
cd http-channel
go build -o http-channel .
go test -race ./...
```

## Standalone flags

```
-addr     string     Listen address (default "0.0.0.0:9100")
-path     string     Endpoint path (default "/chat")
-origins  string     Comma-separated CORS origins (default: allow all)
-timeout  duration   Per-request response wait (default 2m)
```

## Example requests

Each example assumes `TOKEN=<profile_token>` in the shell. The channel returns JSON; pipe to `jq` to read fields.

### Hello world — fresh conversation

```bash
curl -s -X POST http://localhost:9100/chat \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"content":"hello"}'
```

```json
{
  "conversation_id": "3f2a1b9c8d7e6f5a4b3c2d1e0f9a8b7c",
  "content": "Hi! How can I help?"
}
```

### Multi-turn — resume an existing conversation

Save the `conversation_id` from the first response and pass it back on every follow-up turn. The orchestrator resumes the session, so the LLM sees the prior messages.

```bash
CONV=$(curl -s -X POST http://localhost:9100/chat \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"content":"my favourite colour is teal"}' | jq -r .conversation_id)

curl -s -X POST http://localhost:9100/chat \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"content\":\"what did i just tell you?\",\"conversation_id\":\"$CONV\"}"
```

If `conversation_id` no longer exists in core (expired/pruned), the response carries `metadata.error_code = "session_expired"` — drop the id and start a fresh turn without it.

### Token in the query string (curl, browser fetch, anything that can't set headers)

```bash
curl -s -X POST "http://localhost:9100/chat?token=$TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"content":"hello"}'
```

`X-Profile-Token: <token>` works too — same effect as the Bearer header.

### File attachment — summarise a PDF

`files[].data` is base64-encoded raw bytes. `mime_type` is forwarded as-is.

```bash
B64=$(base64 < report.pdf | tr -d '\n')
curl -s -X POST http://localhost:9100/chat \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "$(jq -nc --arg b64 "$B64" '{
        content: "Summarise this report in three bullets.",
        files: [{ name: "report.pdf", mime_type: "application/pdf", data: $b64 }]
      }')"
```

Image (`image/png`, `image/jpeg`) and plain text (`text/plain`) work the same way; the multimodal capabilities of the configured LLM determine what actually gets used.

### Just file, no text

```bash
B64=$(base64 < screenshot.png | tr -d '\n')
curl -s -X POST http://localhost:9100/chat \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "$(jq -nc --arg b64 "$B64" '{
        files: [{ name: "screenshot.png", mime_type: "image/png", data: $b64 }]
      }')"
```

### Inspecting full response with metadata

The orchestrator may attach metadata (e.g. confirmation prompts, error codes, tool-call hints). Pretty-print the whole frame to see it:

```bash
curl -s -X POST http://localhost:9100/chat \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"content":"delete record 42"}' | jq
```

```json
{
  "conversation_id": "3f2a1b…",
  "content": "Are you sure you want to delete record 42?",
  "metadata": {
    "type": "confirmation",
    "options": "approve,reject"
  }
}
```

To answer a confirmation prompt, post the option back as the next turn's `content` on the same `conversation_id`:

```bash
curl -s -X POST http://localhost:9100/chat \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"content\":\"approve\",\"conversation_id\":\"$CONV\"}"
```

### Common errors

```bash
# 401 — missing or unrecognised token
curl -s -i -X POST http://localhost:9100/chat \
  -H "Content-Type: application/json" -d '{"content":"hi"}' | head -1
# HTTP/1.1 401 Unauthorized

# 400 — neither content nor files
curl -s -i -X POST "http://localhost:9100/chat?token=$TOKEN" \
  -H "Content-Type: application/json" -d '{}' | head -1
# HTTP/1.1 400 Bad Request

# 409 — same conversation_id is already mid-turn (serialise client-side)
# 504 — LLM took longer than `timeout` to respond (raise it or retry)
```

### Tiny Python client

```python
import base64, json, requests

API   = "http://localhost:9100/chat"
TOKEN = "…"

def chat(content, conversation_id=None, files=None):
    body = {"content": content}
    if conversation_id:
        body["conversation_id"] = conversation_id
    if files:
        body["files"] = [
            {"name": f["name"], "mime_type": f["mime_type"],
             "data": base64.b64encode(f["bytes"]).decode()}
            for f in files
        ]
    r = requests.post(API, headers={"Authorization": f"Bearer {TOKEN}"}, json=body, timeout=180)
    r.raise_for_status()
    return r.json()

first  = chat("explain HTTP in one paragraph")
second = chat("now in one sentence", conversation_id=first["conversation_id"])
print(second["content"])
```
