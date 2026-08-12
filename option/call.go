package option

import (
	"strings"

	"github.com/sagernet/sing/common/json/badoption"
)

// CallCookie is a single cookie entry provided inline in the configuration as
// JSON, matching the browser-exported {name, value} format.
type CallCookie struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// CallCookieList is the list of cookies provided in the configuration.
type CallCookieList []CallCookie

// Header renders the cookie list into a single "name=value; name=value"
// header string. Entries with an empty name are skipped.
func (l CallCookieList) Header() string {
	if len(l) == 0 {
		return ""
	}
	parts := make([]string, 0, len(l))
	for _, c := range l {
		if c.Name == "" {
			continue
		}
		parts = append(parts, c.Name+"="+c.Value)
	}
	return strings.Join(parts, "; ")
}

// ParseCookieHeader parses a "name=value; name=value" cookie header string into
// a CallCookieList.
func ParseCookieHeader(header string) CallCookieList {
	header = strings.TrimSpace(header)
	if header == "" {
		return nil
	}
	var list CallCookieList
	for _, part := range strings.Split(header, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, value, _ := strings.Cut(part, "=")
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		list = append(list, CallCookie{Name: name, Value: strings.TrimSpace(value)})
	}
	return list
}

type CallCommonOptions struct {
	Platform          string `json:"platform,omitempty"`
	Mode              string `json:"mode,omitempty"`
	ReadBuffer        int    `json:"read_buffer,omitempty"`
	MaxBufferedAmount int    `json:"max_buffered_amount,omitempty"`
	MemoryLimit       int64  `json:"memory_limit,omitempty"`
	MultipathProfile  string `json:"multipath_profile,omitempty"`
}

type CallInboundOptions struct {
	DialerOptions
	CallCommonOptions
	Listen               *badoption.Addr    `json:"listen,omitempty"`
	ListenPort           uint16             `json:"listen_port,omitempty"`
	Cookies              CallCookieList     `json:"cookies,omitempty"`
	JoinLink             string             `json:"join_link,omitempty"`
	ObfsPassword         string             `json:"obfs_password,omitempty"`
	Users                []CallUser         `json:"users,omitempty"`
	MaxSessions          int                `json:"max_sessions,omitempty"`
	MaxWorkersPerSession int                `json:"max_workers_per_session,omitempty"`
	MaxPendingHandshakes int                `json:"max_pending_handshakes,omitempty"`
	HandshakeTimeout     badoption.Duration `json:"handshake_timeout,omitempty"`
	SessionIdleTimeout   badoption.Duration `json:"session_idle_timeout,omitempty"`
	UDPReceiveBufferBytes int               `json:"udp_receive_buffer_bytes,omitempty"`
	UDPSendBufferBytes    int               `json:"udp_send_buffer_bytes,omitempty"`
	IngressWorkers        int               `json:"ingress_workers,omitempty"`
	IngressQueuePackets   int               `json:"ingress_queue_packets,omitempty"`
	PeerReadQueuePackets  int               `json:"peer_read_queue_packets,omitempty"`
	// Email and Password are used to re-authenticate with the dion.vc
	// platform when the refresh cookie is missing or rejected.
	Email    string `json:"email,omitempty"`
	Password string `json:"password,omitempty"`
}

type CallUser struct {
	Name        string `json:"name"`
	Password    string `json:"password"`
	MaxSessions int    `json:"max_sessions,omitempty"`
}

type CallOutboundOptions struct {
	DialerOptions
	ServerOptions
	CallCommonOptions
	JoinLink            string             `json:"join_link,omitempty"`
	JoinLinks           []string           `json:"join_links,omitempty"`
	User                string             `json:"user,omitempty"`
	Password            string             `json:"password,omitempty"`
	ObfsPassword        string             `json:"obfs_password,omitempty"`
	Workers             int                `json:"workers,omitempty"`
	WorkerConnectTimeout badoption.Duration `json:"worker_connect_timeout,omitempty"`
	Cookies             CallCookieList     `json:"cookies,omitempty"`
}
