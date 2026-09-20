package manager

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"testing"

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
