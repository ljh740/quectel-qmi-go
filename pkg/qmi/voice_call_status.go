package qmi

// VOICE 呼叫状态与方向是线上的枚举值，未知值仍按原值向上层传递。
const (
	VoiceCallStateUnknown       VoiceCallState = 0
	VoiceCallStateOrigination   VoiceCallState = 1
	VoiceCallStateIncoming      VoiceCallState = 2
	VoiceCallStateConversation  VoiceCallState = 3
	VoiceCallStateCCInProgress  VoiceCallState = 4
	VoiceCallStateAlerting      VoiceCallState = 5
	VoiceCallStateHold          VoiceCallState = 6
	VoiceCallStateWaiting       VoiceCallState = 7
	VoiceCallStateDisconnecting VoiceCallState = 8
	VoiceCallStateEnd           VoiceCallState = 9
	VoiceCallStateSetup         VoiceCallState = 10
)

const (
	VoiceCallDirectionUnknown VoiceCallDirection = 0
	VoiceCallDirectionMO      VoiceCallDirection = 1 // 本端发起。
	VoiceCallDirectionMT      VoiceCallDirection = 2 // 网络侧来电。
)

// VoiceCallEndReason 是 QMI 原始结束原因，不是 Q.850 cause。
// 例如 UserBusy=146 对应 Q.850 17，不能用固定偏移换算。
type VoiceCallEndReason uint16

// 常用本端、网络及 3GPP CC 原因，数值与 QmiVoiceCallEndReason 一致。
// 未列出的厂商扩展值仍可由 VoiceCallEndReason 无损承载。
const (
	VoiceCallEndReasonOffline           VoiceCallEndReason = 0
	VoiceCallEndReasonNoService         VoiceCallEndReason = 21
	VoiceCallEndReasonFade              VoiceCallEndReason = 22
	VoiceCallEndReasonReleaseNormal     VoiceCallEndReason = 25
	VoiceCallEndReasonClientEnd         VoiceCallEndReason = 29
	VoiceCallEndReasonUIMNotPresent     VoiceCallEndReason = 34
	VoiceCallEndReasonIncomingRejected  VoiceCallEndReason = 102
	VoiceCallEndReasonSetupRejected     VoiceCallEndReason = 103
	VoiceCallEndReasonNetworkEnd        VoiceCallEndReason = 104
	VoiceCallEndReasonNoFunds           VoiceCallEndReason = 105
	VoiceCallEndReasonNoGWService       VoiceCallEndReason = 106
	VoiceCallEndReasonNoCDMAService     VoiceCallEndReason = 107
	VoiceCallEndReasonNoFullService     VoiceCallEndReason = 108
	VoiceCallEndReasonCallBarred        VoiceCallEndReason = 115
	VoiceCallEndReasonAbsentSubscriber  VoiceCallEndReason = 122
	VoiceCallEndReasonRejectedByUser    VoiceCallEndReason = 134
	VoiceCallEndReasonRejectedByNetwork VoiceCallEndReason = 135

	VoiceCallEndReasonUnassignedNumber                          VoiceCallEndReason = 141
	VoiceCallEndReasonNoRouteToDestination                      VoiceCallEndReason = 142
	VoiceCallEndReasonChannelUnacceptable                       VoiceCallEndReason = 143
	VoiceCallEndReasonOperatorDeterminedBarring                 VoiceCallEndReason = 144
	VoiceCallEndReasonNormalCallClearing                        VoiceCallEndReason = 145
	VoiceCallEndReasonUserBusy                                  VoiceCallEndReason = 146
	VoiceCallEndReasonNoUserResponding                          VoiceCallEndReason = 147
	VoiceCallEndReasonUserAlertingNoAnswer                      VoiceCallEndReason = 148
	VoiceCallEndReasonCallRejected                              VoiceCallEndReason = 149
	VoiceCallEndReasonNumberChanged                             VoiceCallEndReason = 150
	VoiceCallEndReasonPreemption                                VoiceCallEndReason = 151
	VoiceCallEndReasonDestinationOutOfOrder                     VoiceCallEndReason = 152
	VoiceCallEndReasonInvalidNumberFormat                       VoiceCallEndReason = 153
	VoiceCallEndReasonFacilityRejected                          VoiceCallEndReason = 154
	VoiceCallEndReasonResponseToStatusEnquiry                   VoiceCallEndReason = 155
	VoiceCallEndReasonNormalUnspecified                         VoiceCallEndReason = 156
	VoiceCallEndReasonNoCircuitOrChannelAvailable               VoiceCallEndReason = 157
	VoiceCallEndReasonNetworkOutOfOrder                         VoiceCallEndReason = 158
	VoiceCallEndReasonTemporaryFailure                          VoiceCallEndReason = 159
	VoiceCallEndReasonSwitchingEquipmentCongestion              VoiceCallEndReason = 160
	VoiceCallEndReasonAccessInformationDiscarded                VoiceCallEndReason = 161
	VoiceCallEndReasonRequestedCircuitOrChannelNotAvailable     VoiceCallEndReason = 162
	VoiceCallEndReasonResourcesUnavailableOrUnspecified         VoiceCallEndReason = 163
	VoiceCallEndReasonQoSUnavailable                            VoiceCallEndReason = 164
	VoiceCallEndReasonRequestedFacilityNotSubscribed            VoiceCallEndReason = 165
	VoiceCallEndReasonIncomingCallsBarredWithinCUG              VoiceCallEndReason = 166
	VoiceCallEndReasonBearerCapabilityNotAuth                   VoiceCallEndReason = 167
	VoiceCallEndReasonBearerCapabilityUnavailable               VoiceCallEndReason = 168
	VoiceCallEndReasonServiceOptionNotAvailable                 VoiceCallEndReason = 169
	VoiceCallEndReasonACMLimitExceeded                          VoiceCallEndReason = 170
	VoiceCallEndReasonBearerServiceNotImplemented               VoiceCallEndReason = 171
	VoiceCallEndReasonRequestedFacilityNotImplemented           VoiceCallEndReason = 172
	VoiceCallEndReasonOnlyDigitalInformationBearerAvailable     VoiceCallEndReason = 173
	VoiceCallEndReasonServiceOrOptionNotImplemented             VoiceCallEndReason = 174
	VoiceCallEndReasonInvalidTransactionIdentifier              VoiceCallEndReason = 175
	VoiceCallEndReasonUserNotMemberOfCUG                        VoiceCallEndReason = 176
	VoiceCallEndReasonIncompatibleDestination                   VoiceCallEndReason = 177
	VoiceCallEndReasonInvalidTransitNetworkSelection            VoiceCallEndReason = 178
	VoiceCallEndReasonSemanticallyIncorrectMessage              VoiceCallEndReason = 179
	VoiceCallEndReasonInvalidMandatoryInformation               VoiceCallEndReason = 180
	VoiceCallEndReasonMessageTypeNotImplemented                 VoiceCallEndReason = 181
	VoiceCallEndReasonMessageTypeNotCompatibleWithProtocolState VoiceCallEndReason = 182
	VoiceCallEndReasonInformationElementNonExistent             VoiceCallEndReason = 183
	VoiceCallEndReasonConditionalIEError                        VoiceCallEndReason = 184
	VoiceCallEndReasonMessageNotCompatibleWithProtocolState     VoiceCallEndReason = 185
	VoiceCallEndReasonRecoveryOnTimerExpired                    VoiceCallEndReason = 186
	VoiceCallEndReasonProtocolErrorUnspecified                  VoiceCallEndReason = 187
	VoiceCallEndReasonInterworkingUnspecified                   VoiceCallEndReason = 188
	VoiceCallEndReasonOutgoingCallsBarredWithinCUG              VoiceCallEndReason = 189
)

// VoiceCallEndReasonInfo 通过 CallID 关联呼叫，不依赖 Calls 数组的顺序或数量。
type VoiceCallEndReasonInfo struct {
	CallID uint8
	Reason VoiceCallEndReason
}
