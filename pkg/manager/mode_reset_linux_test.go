//go:build linux

package manager

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ljh740/quectel-qmi-go/pkg/qmi"
)

// 使用真实 Client 和 DMS 编码路径，统计线上的请求，防止只覆盖包装层的假实现。
func TestOperatingModeWireReplayPolicy(t *testing.T) {
	for _, tc := range []struct {
		name         string
		mode         qmi.OperatingMode
		failure      string
		wantRequests int32
	}{
		{"reset_success", qmi.ModeReset, "", 1},
		{"reset_invalid_client", qmi.ModeReset, "invalid_id", 1},
		{"reset_timeout", qmi.ModeReset, "timeout", 1},
		{"reset_disconnect", qmi.ModeReset, "disconnect", 1},
		{"online_still_rebinds", qmi.ModeOnline, "invalid_id", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := fmt.Sprintf("@qmi-reset-%d-%d", os.Getpid(), time.Now().UnixNano())
			listener, err := net.Listen("unix", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			var requests atomic.Int32
			done := make(chan struct{})
			go func() {
				defer close(done)
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				for {
					header := make([]byte, 3)
					if _, err := io.ReadFull(conn, header); err != nil {
						return
					}
					frame := make([]byte, 1+int(binary.LittleEndian.Uint16(header[1:])))
					copy(frame, header)
					if _, err := io.ReadFull(conn, frame[3:]); err != nil {
						return
					}
					req, err := qmi.UnmarshalPacket(frame)
					if err != nil {
						t.Error(err)
						return
					}
					tlv := qmi.TLV{Type: 2, Value: []byte{0, 0, 0, 0}}
					resp := *req
					resp.TLVs = []qmi.TLV{tlv}
					if req.ServiceType == qmi.ServiceControl && req.MessageID == qmi.CTLGetClientID {
						resp.TLVs = append(resp.TLVs, qmi.TLV{Type: 1, Value: []byte{qmi.ServiceDMS, 7}})
					}
					if req.ServiceType == qmi.ServiceDMS && req.MessageID == qmi.DMSSetOperatingMode {
						n := requests.Add(1)
						if n == 1 {
							switch tc.failure {
							case "timeout":
								continue
							case "disconnect":
								return
							case "invalid_id":
								resp.TLVs[0].Value = []byte{1, 0, byte(qmi.QMIErrInvalidID), 0}
							}
						}
					}
					encoded := resp.Marshal()
					encoded[6] = 2 // service response
					if req.ServiceType == qmi.ServiceControl {
						encoded[6] = 1
					}
					if _, err := conn.Write(encoded); err != nil {
						return
					}
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			client, err := qmi.NewClientWithOptions(ctx, "/dev/test-reset", qmi.ClientOptions{
				UseProxy: true, ProxyPath: addr, ReadDeadline: 10 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close(); <-done }()
			m := newRecoveryTestManager()
			m.cfg = normalizeConfig(Config{RecoveryPolicy: RecoveryPolicy{ServiceTimeoutThreshold: 1}})
			m.client = client
			resetCtx, resetCancel := context.WithTimeout(ctx, 150*time.Millisecond)
			defer resetCancel()
			err = m.SetOperatingMode(resetCtx, tc.mode)
			if tc.mode == qmi.ModeReset && tc.failure != "" {
				if err == nil {
					t.Fatal("expected original failure")
				}
				if tc.failure == "timeout" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("error = %v", err)
				}
				if tc.failure == "invalid_id" && qmi.GetQMIError(err) == nil {
					t.Fatalf("lost QMI error: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if got := requests.Load(); got != tc.wantRequests {
				t.Fatalf("wire requests = %d, want %d", got, tc.wantRequests)
			}
			select {
			case event := <-m.eventCh:
				t.Fatalf("unexpected extra core recovery: %v", event)
			default:
			}
		})
	}
}
