package timestamp

import (
	"encoding/json"
	"testing"
	"time"
)

func TestPayloadUsesRFC3339NanoUTC(t *testing.T) {
	now := time.Date(2026, 5, 6, 12, 34, 56, 789, time.FixedZone("EDT", -4*60*60))

	data, err := Payload(now)
	if err != nil {
		t.Fatalf("Payload returned error: %v", err)
	}

	var got struct {
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}

	want := "2026-05-06T16:34:56.000000789Z"
	if got.Timestamp != want {
		t.Fatalf("timestamp = %q, want %q", got.Timestamp, want)
	}
}
