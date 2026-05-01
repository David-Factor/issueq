package workflow

import (
	"testing"

	"issueq/internal/supervisor"
)

func TestObservationToDecision(t *testing.T) {
	tests := []struct {
		name string
		obs  supervisor.Observation
		want ObservationDecision
	}{
		{name: "starting", obs: supervisor.Observation{State: supervisor.RunStarting}, want: DecisionKeepRunning},
		{name: "running", obs: supervisor.Observation{State: supervisor.RunRunning}, want: DecisionKeepRunning},
		{name: "exit zero", obs: supervisor.Observation{State: supervisor.RunExited, HasExitCode: true, ExitCode: 0}, want: DecisionSucceeded},
		{name: "exit nonzero", obs: supervisor.Observation{State: supervisor.RunExited, HasExitCode: true, ExitCode: 2}, want: DecisionFailed},
		{name: "failed", obs: supervisor.Observation{State: supervisor.RunFailed}, want: DecisionFailed},
		{name: "timed out", obs: supervisor.Observation{State: supervisor.RunTimedOut}, want: DecisionFailed},
		{name: "cancelled", obs: supervisor.Observation{State: supervisor.RunCancelled}, want: DecisionCancelled},
		{name: "unknown", obs: supervisor.Observation{State: supervisor.RunUnknown}, want: DecisionUnknown},
		{name: "empty", obs: supervisor.Observation{}, want: DecisionUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ObservationToDecision(tt.obs); got != tt.want {
				t.Fatalf("ObservationToDecision() = %q, want %q", got, tt.want)
			}
		})
	}
}
