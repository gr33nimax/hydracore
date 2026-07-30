package xhttp

import (
	"context"
	"crypto/rand"
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/option"
)

const hardXmuxPoolLimit = 16

type XmuxConn interface {
	Close()
	IsClosed() bool
}

type XmuxClient struct {
	XmuxConn     XmuxConn
	openUsage    int
	leftUsage    int
	LeftRequests atomic.Int32
	UnreusableAt time.Time

	closed bool
	mtx    sync.Mutex
}

func (c *XmuxClient) Close() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.closed = true
	if c.openUsage <= 0 {
		c.XmuxConn.Close()
	}
}

func (c *XmuxClient) ForceClose() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.closed = true
	c.XmuxConn.Close()
}

func (c *XmuxClient) AddOpenUsage(delta int) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.openUsage += delta
	if c.closed && c.openUsage <= 0 {
		c.XmuxConn.Close()
	}
}

func (c *XmuxClient) GetOpenUsage() int {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.openUsage
}

type XmuxManager struct {
	options     option.V2RayXHTTPXmuxOptions
	concurrency int
	connections int
	newConnFunc func() XmuxConn
	xmuxClients []*XmuxClient
	closed      bool
	mtx         sync.Mutex
}

func NewXmuxManager(options option.V2RayXHTTPXmuxOptions, newConnFunc func() XmuxConn) *XmuxManager {
	connections := options.GetNormalizedMaxConnections().Rand()
	if connections > hardXmuxPoolLimit {
		connections = hardXmuxPoolLimit
	}
	return &XmuxManager{
		options:     options,
		concurrency: options.GetNormalizedMaxConcurrency().Rand(),
		connections: connections,
		newConnFunc: newConnFunc,
		xmuxClients: make([]*XmuxClient, 0),
	}
}

func (m *XmuxManager) Close() {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	for _, xmuxClient := range m.xmuxClients {
		xmuxClient.ForceClose()
	}
	m.xmuxClients = m.xmuxClients[:0]
}

func (m *XmuxManager) newXmuxClient() *XmuxClient {
	xmuxClient := &XmuxClient{
		XmuxConn:  m.newConnFunc(),
		leftUsage: -1,
	}
	if x := m.options.GetNormalizedCMaxReuseTimes().Rand(); x > 0 {
		xmuxClient.leftUsage = x - 1
	}
	xmuxClient.LeftRequests.Store(math.MaxInt32)
	if x := m.options.GetNormalizedHMaxRequestTimes().Rand(); x > 0 {
		xmuxClient.LeftRequests.Store(int32(x))
	}
	if x := m.options.GetNormalizedHMaxReusableSecs().Rand(); x > 0 {
		xmuxClient.UnreusableAt = time.Now().Add(time.Duration(x) * time.Second)
	}
	m.xmuxClients = append(m.xmuxClients, xmuxClient)
	return xmuxClient
}

func (m *XmuxManager) GetXmuxClient(ctx context.Context) *XmuxClient {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.closed {
		return nil
	}
	var evicted []*XmuxClient
	for i := 0; i < len(m.xmuxClients); {
		xmuxClient := m.xmuxClients[i]
		if xmuxClient.XmuxConn.IsClosed() ||
			xmuxClient.leftUsage == 0 ||
			xmuxClient.LeftRequests.Load() <= 0 ||
			(xmuxClient.UnreusableAt != time.Time{} && time.Now().After(xmuxClient.UnreusableAt)) {
			m.xmuxClients = append(m.xmuxClients[:i], m.xmuxClients[i+1:]...)
			evicted = append(evicted, xmuxClient)
		} else {
			i++
		}
	}
	for _, c := range evicted {
		c.Close()
	}
	if len(m.xmuxClients) == 0 {
		return m.newXmuxClient()
	}
	if m.connections > 0 && len(m.xmuxClients) < m.connections {
		return m.newXmuxClient()
	}
	xmuxClients := make([]*XmuxClient, 0)
	if m.concurrency > 0 {
		for _, xmuxClient := range m.xmuxClients {
			if xmuxClient.GetOpenUsage() < m.concurrency {
				xmuxClients = append(xmuxClients, xmuxClient)
			}
		}
	} else {
		xmuxClients = m.xmuxClients
	}
	if len(xmuxClients) == 0 {
		if len(m.xmuxClients) < hardXmuxPoolLimit &&
			(m.connections <= 0 || len(m.xmuxClients) < m.connections) {
			return m.newXmuxClient()
		}
		// The extended API is synchronous and cannot wait for capacity. Once
		// the hard pool limit is reached, reuse the least-loaded client
		// instead of creating an unbounded number of physical transports.
		xmuxClient := m.xmuxClients[0]
		for _, candidate := range m.xmuxClients[1:] {
			if candidate.GetOpenUsage() < xmuxClient.GetOpenUsage() {
				xmuxClient = candidate
			}
		}
		return xmuxClient
	}
	i, _ := rand.Int(rand.Reader, big.NewInt(int64(len(xmuxClients))))
	xmuxClient := xmuxClients[i.Int64()]
	if xmuxClient.leftUsage > 0 {
		xmuxClient.leftUsage -= 1
	}
	return xmuxClient
}
