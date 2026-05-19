package app

import (
	"os"
	"testing"
)

func TestNATSConnectTokenUsesExplicitToken(t *testing.T) {
	t.Setenv("NATS_TOKEN", "from-env")

	if got := natsConnectToken("from-flag"); got != "from-flag" {
		t.Fatalf("natsConnectToken = %q, want explicit token", got)
	}
}

func TestNATSConnectTokenFallsBackToEnvironment(t *testing.T) {
	t.Setenv("NATS_TOKEN", "from-env")

	if got := natsConnectToken(""); got != "from-env" {
		t.Fatalf("natsConnectToken = %q, want env token", got)
	}
}

func TestNATSConnectTokenAllowsNoToken(t *testing.T) {
	t.Setenv("NATS_TOKEN", "")
	t.Setenv("NATS_TOKEN_FILE", "")

	if got := natsConnectToken(""); got != "" {
		t.Fatalf("natsConnectToken = %q, want empty token", got)
	}
}

func TestNATSConnectTokenFallsBackToTokenFile(t *testing.T) {
	path := t.TempDir() + "/token"
	if err := os.WriteFile(path, []byte("$USR:AAABBB\n"), 0o600); err != nil {
		t.Fatalf("WriteFile token returned error: %v", err)
	}
	t.Setenv("NATS_TOKEN", "")
	t.Setenv("NATS_TOKEN_FILE", path)

	if got := natsConnectToken(""); got != "$USR:AAABBB" {
		t.Fatalf("natsConnectToken = %q, want token file value", got)
	}
}
