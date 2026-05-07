package timestamp

import (
	"encoding/json"
	"time"
)

type response struct {
	Timestamp string `json:"timestamp"`
}

func Payload(now time.Time) ([]byte, error) {
	return json.Marshal(response{
		Timestamp: now.UTC().Format(time.RFC3339Nano),
	})
}
