package manager

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ljh740/quectel-qmi-go/pkg/qmi"
	"github.com/sirupsen/logrus"
)

func TestHandleIndicationVoiceCallEndReasons(t *testing.T) {
	m := &Manager{log: NewNopLogger(), events: NewEventEmitter()}
	t.Cleanup(m.events.Close)
	ch := make(chan Event, 1)
	m.OnVoiceCallStatus(func(info *qmi.VoiceAllCallInfo) {
		// 首个订阅者修改自己的副本，不能污染后续订阅者。
		info.CallEndReasons[0].Reason = qmi.VoiceCallEndReasonOffline
	})
	m.OnVoiceCallStatus(func(info *qmi.VoiceAllCallInfo) {
		ch <- Event{VoiceCalls: info}
	})
	metaCh := make(chan Event, 1)
	m.OnEvent(func(evt Event) { metaCh <- evt })
	m.handleIndication(qmi.Event{
		Type: qmi.EventVoiceCallStatus, ServiceID: qmi.ServiceVOICE, MessageID: qmi.VOICEAllCallStatusInd,
		Packet: &qmi.Packet{TLVs: []qmi.TLV{
			{Type: 0x01, Value: []byte{1, 7, 9, 0, 1, 3, 0, 0}},
			{Type: 0x14, Value: []byte{1, 7, 0x9c, 0}},
			{Type: 0xf0, Value: []byte{0xff}},
		}},
	})
	info := waitManagerEvent(t, ch).VoiceCalls
	if info == nil || len(info.CallEndReasons) != 1 || info.CallEndReasons[0] != (qmi.VoiceCallEndReasonInfo{CallID: 7, Reason: qmi.VoiceCallEndReasonNormalUnspecified}) {
		t.Fatalf("call end reason not propagated or shared across callbacks: %+v", info)
	}
	evt := waitManagerEvent(t, metaCh)
	wantMeta := []qmi.TLVMeta{{Type: 0x01, Length: 8}, {Type: 0x14, Length: 4}, {Type: 0xf0, Length: 1}}
	if !reflect.DeepEqual(evt.TLVMeta, wantMeta) || evt.ServiceID != qmi.ServiceVOICE || evt.MessageID != qmi.VOICEAllCallStatusInd {
		t.Fatalf("indication metadata lost: %+v", evt)
	}
}

func TestCloneVoiceCallEndReasons(t *testing.T) {
	info := &qmi.VoiceAllCallInfo{CallEndReasons: []qmi.VoiceCallEndReasonInfo{{CallID: 3, Reason: qmi.VoiceCallEndReasonClientEnd}}}
	copy := cloneVoiceAllCallInfo(info)
	info.CallEndReasons[0].CallID = 8
	info.CallEndReasons[0].Reason = qmi.VoiceCallEndReasonNetworkEnd
	if copy.CallEndReasons[0] != (qmi.VoiceCallEndReasonInfo{CallID: 3, Reason: qmi.VoiceCallEndReasonClientEnd}) {
		t.Fatalf("clone shares reason storage: %+v", copy.CallEndReasons)
	}
	if cloneVoiceAllCallInfo(nil) != nil {
		t.Fatal("nil clone is not nil")
	}
}

func TestHandleIndicationVoiceMalformedEndReasonStillEmits(t *testing.T) {
	var output bytes.Buffer
	log := logrus.New()
	log.SetOutput(&output)
	log.SetLevel(logrus.WarnLevel)
	m := &Manager{log: NewLogrusLogger(log), events: NewEventEmitter()}
	t.Cleanup(m.events.Close)
	ch := make(chan Event, 1)
	m.OnVoiceCallStatus(func(info *qmi.VoiceAllCallInfo) { ch <- Event{VoiceCalls: info} })
	m.handleIndication(qmi.Event{
		Type: qmi.EventVoiceCallStatus,
		Packet: &qmi.Packet{TLVs: []qmi.TLV{
			{Type: 0x01, Value: []byte{2, 4, 8, 0, 1, 3, 0, 0, 7, 9, 0, 1, 3, 0, 0}},
			{Type: 0x10, Value: []byte{2, 4, 0, 1, 'a', 7, 1, 1, 'b'}},
			{Type: 0x14, Value: []byte{2, 4, 0x9c, 0, 7}},
		}},
	})
	select {
	case evt := <-ch:
		info := evt.VoiceCalls
		wantCalls := []qmi.VoiceCallInfo{
			{ID: 4, State: qmi.VoiceCallStateDisconnecting, Direction: qmi.VoiceCallDirectionMO, Mode: 3},
			{ID: 7, State: qmi.VoiceCallStateEnd, Direction: qmi.VoiceCallDirectionMO, Mode: 3},
		}
		wantNumbers := []qmi.VoiceRemotePartyNumber{
			{CallID: 4, Number: "a", RawNumber: []byte{'a'}},
			{CallID: 7, PresentationIndicator: 1, Number: "b", RawNumber: []byte{'b'}},
		}
		if info == nil || !reflect.DeepEqual(info.Calls, wantCalls) || !reflect.DeepEqual(info.RemotePartyNumbers, wantNumbers) || info.CallEndReasons != nil {
			t.Fatalf("partial indication lost state or number: %+v", info)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("malformed optional end reason suppressed call status event")
	}
	if !strings.Contains(output.String(), "level=warning") || !strings.Contains(output.String(), "call end reason TLV 0x14") {
		t.Fatalf("partial failure warning missing: %s", output.String())
	}
}

func TestVOICERecoveryPreservesPartialCallInfo(t *testing.T) {
	m := newRecoveryTestManager()
	t.Cleanup(m.events.Close)
	m.ensureVOICEServiceHook = func() (*qmi.VOICEService, error) { return &qmi.VOICEService{}, nil }
	rebinds := 0
	m.rebindVOICEServiceHook = func(string) (*qmi.VOICEService, error) {
		rebinds++
		return &qmi.VOICEService{}, nil
	}
	partial, parseErr := qmi.ParseVoiceAllCallStatus(&qmi.Packet{TLVs: []qmi.TLV{
		{Type: 0x01, Value: []byte{1, 7, 9, 0, 1, 3, 0, 0}},
		{Type: 0x14, Value: []byte{1}},
	}})
	if partial == nil || parseErr == nil {
		t.Fatalf("fixture: expected partial parse failure, info=%+v err=%v", partial, parseErr)
	}
	// 查询路径使用同一恢复包装；可选字段错误不能触发重绑或抹掉结果。
	info, err := withVOICERecoveryValue(m, "VOICEGetAllCallInfo", func(*qmi.VOICEService) (*qmi.VoiceAllCallInfo, error) {
		return partial, parseErr
	})
	if info != partial || err != parseErr || rebinds != 0 {
		t.Fatalf("partial query result lost or triggered recovery: info=%+v err=%v rebinds=%d", info, err, rebinds)
	}
}

func TestVoiceCallStatusDiagnostics(t *testing.T) {
	for _, tt := range []struct {
		name  string
		level logrus.Level
		value []byte
	}{
		{"debug_valid", logrus.DebugLevel, []byte{1, 7, 0x9c, 0}},
		{"debug_malformed", logrus.DebugLevel, []byte{1, 7}},
		{"info", logrus.InfoLevel, []byte{1, 7, 0x9c, 0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			log := logrus.New()
			log.SetOutput(&output)
			log.SetFormatter(&logrus.JSONFormatter{})
			log.SetLevel(tt.level)
			m := &Manager{log: NewLogrusLogger(log), events: NewEventEmitter()}
			t.Cleanup(m.events.Close)
			m.handleIndication(qmi.Event{
				Type: qmi.EventVoiceCallStatus,
				Packet: &qmi.Packet{TLVs: []qmi.TLV{
					{Type: 0x01, Value: []byte{0}},
					{Type: 0x14, Value: tt.value},
					{Type: 0x10, Value: []byte{1, 7, 0, 6, 's', 'e', 'c', 'r', 'e', 't'}},
					{Type: 0xf0, Value: []byte("private-value")},
				}},
			})
			if strings.Contains(output.String(), "secret") || strings.Contains(output.String(), "private-value") || strings.Contains(output.String(), "736563726574") {
				t.Fatalf("sensitive TLV value leaked: %s", output.String())
			}
			decoder := json.NewDecoder(&output)
			found := false
			for {
				var entry struct {
					Message string        `json:"msg"`
					Level   string        `json:"level"`
					TLVs    []qmi.TLVMeta `json:"tlvs"`
					Raw     string        `json:"call_end_reason_raw"`
				}
				if err := decoder.Decode(&entry); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				if entry.Message != "VOICE call status TLVs" {
					continue
				}
				found = true
				wantRaw := "01079c00"
				if tt.name == "debug_malformed" {
					wantRaw = "0107"
				}
				if entry.Level != "debug" || len(entry.TLVs) != 4 || entry.TLVs[1].Type != 0x14 || entry.TLVs[3].Type != 0xf0 || entry.Raw != wantRaw {
					t.Fatalf("incomplete raw TLV diagnostics: %+v", entry)
				}
			}
			if found != (tt.level == logrus.DebugLevel) {
				t.Fatalf("diagnostics found=%v at level %s", found, tt.level)
			}
		})
	}
}
