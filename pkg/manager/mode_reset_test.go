package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ljh740/quectel-qmi-go/pkg/qmi"
)

func TestModeResetCancelledBeforeServiceAllocation(t *testing.T) {
	m := newRecoveryTestManager()
	m.ensureDMSServiceHook = func() (*qmi.DMSService, error) {
		t.Fatal("cancelled reset must not allocate a service")
		return nil, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.SetOperatingMode(ctx, qmi.ModeReset); !errors.Is(err, context.Canceled) {
		t.Fatalf("reset error = %v", err)
	}
}

func TestModeResetAllocationLockHonorsDeadline(t *testing.T) {
	m := newRecoveryTestManager()
	m.client = &qmi.Client{}
	m.dmsRecoveryMu.Lock()
	defer m.dmsRecoveryMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := m.SetOperatingMode(ctx, qmi.ModeReset); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reset error = %v", err)
	}
}
