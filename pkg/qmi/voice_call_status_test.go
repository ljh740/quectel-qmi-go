package qmi

import (
	"reflect"
	"strings"
	"testing"
)

func TestVoiceCallStateAndDirectionValues(t *testing.T) {
	states := []VoiceCallState{
		VoiceCallStateUnknown, VoiceCallStateOrigination, VoiceCallStateIncoming,
		VoiceCallStateConversation, VoiceCallStateCCInProgress, VoiceCallStateAlerting,
		VoiceCallStateHold, VoiceCallStateWaiting, VoiceCallStateDisconnecting,
		VoiceCallStateEnd, VoiceCallStateSetup,
	}
	for want, got := range states {
		if got != VoiceCallState(want) {
			t.Fatalf("state=%d, want %d", got, want)
		}
	}
	if VoiceCallDirectionUnknown != 0 || VoiceCallDirectionMO != 1 || VoiceCallDirectionMT != 2 {
		t.Fatal("unexpected call direction values")
	}
	for _, tt := range []struct {
		reason VoiceCallEndReason
		want   uint16
	}{
		{VoiceCallEndReasonOffline, 0},
		{VoiceCallEndReasonClientEnd, 29},
		{VoiceCallEndReasonNetworkEnd, 104},
		{VoiceCallEndReasonNoGWService, 106},
		{VoiceCallEndReasonNormalCallClearing, 145},
		{VoiceCallEndReasonUserBusy, 146},
		{VoiceCallEndReasonNormalUnspecified, 156},
		{VoiceCallEndReasonInterworkingUnspecified, 188},
	} {
		if uint16(tt.reason) != tt.want {
			t.Errorf("QMI reason=%d, want %d (not a Q.850 cause)", tt.reason, tt.want)
		}
	}
}

func TestParseVoiceCallEndReasons(t *testing.T) {
	for _, tt := range []struct {
		name       string
		parse      func(*Packet) (*VoiceAllCallInfo, error)
		callsTLV   uint8
		reasonsTLV uint8
		otherTLV   uint8
	}{
		{"indication", ParseVoiceAllCallStatus, 0x01, 0x14, 0x18},
		{"response", parseVoiceAllCallInfoResponse, 0x10, 0x18, 0x14},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// 原因数组故意不按呼叫数组排列，并带一个厂商扩展的 16 位值。
			packet := &Packet{TLVs: []TLV{
				{Type: tt.callsTLV, Value: []byte{
					2,
					7, 9, 0, 1, 3, 0, 0,
					2, 3, 0, 2, 3, 0, 0,
				}},
				{Type: tt.reasonsTLV, Value: []byte{3, 2, 0x92, 0, 7, 0x1d, 0, 9, 0xdc, 0xfe}},
				{Type: 0xf0, Value: []byte{0xff}},
				// 不把另一类消息中的相同编号误认成原因数组。
				{Type: tt.otherTLV, Value: []byte{0xff}},
				successResultTLV(),
			}}
			info, err := tt.parse(packet)
			if err != nil {
				t.Fatal(err)
			}
			want := []VoiceCallEndReasonInfo{
				{CallID: 2, Reason: VoiceCallEndReasonUserBusy},
				{CallID: 7, Reason: VoiceCallEndReasonClientEnd},
				{CallID: 9, Reason: VoiceCallEndReason(0xfedc)},
			}
			if !reflect.DeepEqual(info.CallEndReasons, want) {
				t.Fatalf("reasons=%+v, want %+v", info.CallEndReasons, want)
			}
			if len(info.Calls) != 2 || info.Calls[0].State != VoiceCallStateEnd || info.Calls[1].Direction != VoiceCallDirectionMT {
				t.Fatalf("call status lost: %+v", info.Calls)
			}
			packet.TLVs[1].Value[2] = 0
			if !reflect.DeepEqual(info.CallEndReasons, want) {
				t.Fatal("parsed reasons alias the packet buffer")
			}
		})
	}
}

func TestParseVoiceCallEndReasonsOptional(t *testing.T) {
	for _, tt := range []struct {
		name string
		tlvs []TLV
		want []VoiceCallEndReasonInfo
	}{
		{"missing", nil, nil},
		{"unknown_only", []TLV{{Type: 0xfe, Value: []byte{0xff}}}, nil},
		{"empty", []TLV{{Type: 0x14, Value: []byte{0}}}, []VoiceCallEndReasonInfo{}},
		// 原因值 0 是 OFFLINE，不等于没有上报原因；没有 Calls 也不能丢掉原因。
		{"offline", []TLV{{Type: 0x14, Value: []byte{1, 8, 0, 0}}}, []VoiceCallEndReasonInfo{{CallID: 8, Reason: VoiceCallEndReasonOffline}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			info, err := ParseVoiceAllCallStatus(&Packet{TLVs: tt.tlvs})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(info.CallEndReasons, tt.want) {
				t.Fatalf("reasons=%#v, want %#v", info.CallEndReasons, tt.want)
			}
		})
	}
}

func TestParseVoiceCallEndReasonsMalformed(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value []byte
	}{
		{"missing_count", nil},
		{"missing_entry", []byte{1}},
		{"missing_high_byte", []byte{1, 4, 0x9c}},
		{"truncated_second_entry", []byte{2, 4, 0x9c, 0, 5}},
		{"trailing_bytes", []byte{1, 4, 0x9c, 0, 0}},
		{"zero_count_with_data", []byte{0, 4, 0x9c, 0}},
		{"large_count", []byte{255, 4, 0x9c, 0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			info, err := ParseVoiceAllCallStatus(&Packet{TLVs: []TLV{{Type: 0x14, Value: tt.value}}})
			if err == nil || info != nil || !strings.Contains(err.Error(), "call end reason TLV 0x14") {
				t.Fatalf("malformed array accepted: info=%+v err=%v", info, err)
			}
		})
	}
	for _, parse := range []func(*Packet) (*VoiceAllCallInfo, error){ParseVoiceAllCallStatus, parseVoiceAllCallInfoResponse} {
		if info, err := parse(nil); info != nil || err == nil {
			t.Fatalf("nil packet: info=%+v err=%v", info, err)
		}
	}
}

func TestParseVoiceCallEndReasonsKeepsResultError(t *testing.T) {
	packet := &Packet{TLVs: []TLV{
		{Type: 0x02, Value: []byte{1, 0, 3, 0}},
		{Type: 0x18, Value: []byte{1, 7, 0x1d, 0}},
	}}
	if info, err := parseVoiceAllCallInfoResponse(packet); info != nil || err == nil {
		t.Fatalf("QMI failure ignored: info=%+v err=%v", info, err)
	}
}

func FuzzParseVoiceCallEndReasons(f *testing.F) {
	for _, seed := range [][]byte{nil, {0}, {1, 1, 0x9c, 0}, {2, 1, 0x1d, 0, 2, 0xdc, 0xfe}, {255}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value []byte) {
		info, err := ParseVoiceAllCallStatus(&Packet{TLVs: []TLV{{Type: 0x14, Value: value}}})
		valid := len(value) > 0 && len(value) == 1+3*int(value[0])
		if (err == nil) != valid {
			t.Fatalf("valid=%v err=%v", valid, err)
		}
		if !valid {
			return
		}
		if len(info.CallEndReasons) != int(value[0]) {
			t.Fatalf("decoded count=%d, wire count=%d", len(info.CallEndReasons), value[0])
		}
		for i, reason := range info.CallEndReasons {
			offset := 1 + 3*i
			want := uint16(value[offset+1]) | uint16(value[offset+2])<<8
			if reason.CallID != value[offset] || uint16(reason.Reason) != want {
				t.Fatalf("entry %d differs from wire: %+v", i, reason)
			}
		}
	})
}
