// Package rtc держит точки расширения WebRTC-транспортов.
//
// Они вынесены из transport/call/common в отдельный пакет, потому что их
// подписи тянут за собой весь pion/webrtc, а common импортирует и
// control-plane половина vk, которая нужна vk_parasite и в webrtc не
// нуждается. Пакетная граница здесь дешевле build tag: собирается всё, а
// линкуется только то, что кто-то импортирует.
package rtc

import (
	"github.com/pion/webrtc/v4"
	"github.com/sagernet/sing/common/logger"
)

type PeerConnectionConfigurer interface {
	ConfigureSettingEngine(settingEngine *webrtc.SettingEngine)
}

type AddTunnelTracksFunc func(pc *webrtc.PeerConnection, logger logger.ContextLogger, prefix string) *webrtc.TrackLocalStaticSample

type ReadTrackFunc func(track *webrtc.TrackRemote, handler func([]byte), logger logger.ContextLogger, prefix string)
