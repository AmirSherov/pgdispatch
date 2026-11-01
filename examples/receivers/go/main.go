// Command pgdispatch-receiver is a minimal webhook receiver that verifies
// pgdispatch / Standard Webhooks signatures.
//
// Standard library only. Run it with:
//
//	WEBHOOK_SECRET=whsec_... PORT=8080 go run .
//
// If you can add a dependency, the official package
// github.com/standard-webhooks/standard-webhooks/libraries/go performs the
// same verification. The important part is Verify; copy it into your own
// service and pass it the raw request body.
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	tolerance    = 5 * time.Minute
	maxBodyBytes = 1 << 20
)

// ErrInvalidWebhook is returned when a request is not an authentic, fresh webhook.
var ErrInvalidWebhook = errors.New("invalid webhook")

// Verify returns an error unless body was signed with secret ("whsec_<base64>").
// body must be the exact bytes received: re-encoded JSON would not match.
func Verify(secret string, header http.Header, body []byte) error {
	msgID := header.Get("webhook-id")
	timestamp := header.Get("webhook-timestamp")
	signatures := header.Get("webhook-signature")
	if msgID == "" || timestamp == "" || signatures == "" {
		return fmt.Errorf("%w: missing webhook headers", ErrInvalidWebhook)
	}

	// Replay protection: reject requests signed too far in the past or future.
	sentAt, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: invalid webhook-timestamp", ErrInvalidWebhook)
	}
	if age := time.Since(time.Unix(sentAt, 0)); age > tolerance || age < -tolerance {
		return fmt.Errorf("%w: timestamp outside the tolerance window", ErrInvalidWebhook)
	}

	// The key is the base64 part of the secret, after the "whsec_" prefix.
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, "whsec_"))
	if err != nil {
		return fmt.Errorf("decoding secret: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(msgID + "." + timestamp + "."))
	mac.Write(body)
	expected := mac.Sum(nil)

	// During a secret rotation the header holds several "v1,<sig>" entries
	// separated by spaces; one match is enough.
	for _, entry := range strings.Fields(signatures) {
		version, signature, ok := strings.Cut(entry, ",")
		if !ok || version != "v1" {
			continue
		}
		received, err := base64.StdEncoding.DecodeString(signature)
		if err == nil && hmac.Equal(received, expected) {
			return nil
		}
	}
	return fmt.Errorf("%w: no matching signature", ErrInvalidWebhook)
}

// event is the envelope pgdispatch sends. Data is kept raw so each event type
// can decode it into its own struct.
type event struct {
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

func webhookHandler(secret string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil {
			http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
			return
		}

		if err := Verify(secret, r.Header, body); err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		var evt event
		if err := json.Unmarshal(body, &evt); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		// Delivery is at-least-once: the same webhook-id may arrive again after
		// a timeout or a retry. Store processed ids (e.g. a unique column) and
		// skip duplicates. Keep this handler fast: enqueue the work and answer
		// 2xx right away, then process it in the background.
		log.Printf("received %s (%s)", evt.Type, r.Header.Get("webhook-id"))

		w.WriteHeader(http.StatusNoContent)
	}
}

func main() {
	secret := os.Getenv("WEBHOOK_SECRET")
	if secret == "" {
		log.Fatal("WEBHOOK_SECRET is not set")
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()
	mux.Handle("/", webhookHandler(secret))

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("listening on :%s", port)
	log.Fatal(server.ListenAndServe())
}
