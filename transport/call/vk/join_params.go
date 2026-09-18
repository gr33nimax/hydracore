package vk

// Параметры входа в звонок и ответ signalling-сервера.
//
// Живут вне with_call_legacy: их читает control-plane половина пакета — выдача
// TURN-кредов и авторизация, — которая нужна vk_parasite и не тянет webrtc.
type VKAuthParams struct {
	SessionKey      string `json:"sessionKey"`
	ApplicationKey  string `json:"applicationKey"`
	APIBaseURL      string `json:"apiBaseURL"`
	JoinLink        string `json:"joinLink"`
	AnonymToken     string `json:"anonymToken"`
	AppVersion      string `json:"appVersion"`
	ProtocolVersion string `json:"protocolVersion"`
	TunnelMode      string `json:"tunnelMode"`
	VP8FPS          int    `json:"vp8Fps"`
	VP8Batch        int    `json:"vp8Batch"`
	DualTrack       bool   `json:"dualTrack"`
}

type VKJoinResponse struct {
	Endpoint   string `json:"endpoint"`
	WtEndpoint string `json:"wt_endpoint"`
	Token      string `json:"token"`
	TurnServer struct {
		URLs       []string `json:"urls"`
		Username   string   `json:"username"`
		Credential string   `json:"credential"`
	} `json:"turn_server"`
	StunServer struct {
		URLs []string `json:"urls"`
	} `json:"stun_server"`
}
