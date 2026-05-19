package app

import "testing"

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

	if got := natsConnectToken(""); got != "" {
		t.Fatalf("natsConnectToken = %q, want empty token", got)
	}
}
