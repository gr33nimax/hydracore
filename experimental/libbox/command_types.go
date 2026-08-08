package libbox

import (
	"slices"
	"strings"
	"time"

	"github.com/sagernet/sing-box/daemon"
	M "github.com/sagernet/sing/common/metadata"
)

type StatusMessage struct {
	Memory           int64
	Goroutines       int32
	ConnectionsIn    int32
	ConnectionsOut   int32
	TrafficAvailable bool
	Uplink           int64
	Downlink         int64
	UplinkTotal      int64
	DownlinkTotal    int64
}

const (
	URLTestSessionQueued int32 = iota
	URLTestSessionRunning
	URLTestSessionSucceeded
	URLTestSessionFailed
	URLTestSessionCancelled
)

const (
	RuntimeEventService int32 = iota
	RuntimeEventStatus
	RuntimeEventGroups
	RuntimeEventClashMode
	RuntimeEventURLTestSessions
)

type URLTestResult struct {
	OutboundTag  string
	DelayMillis  int64
	ObservedAt   int64
	Status       string
	ErrorCode    string
	ErrorMessage string
}

type URLTestResultIterator interface {
	Next() *URLTestResult
	HasNext() bool
}

type URLTestSession struct {
	ID           string
	GroupTag     string
	State        int32
	StartedAt    int64
	CompletedAt  int64
	Total        int32
	Completed    int32
	Succeeded    int32
	Failed       int32
	ErrorCode    string
	ErrorMessage string
	results      []*URLTestResult
}

func (s *URLTestSession) Results() URLTestResultIterator {
	return newIterator(s.results)
}

type URLTestSessionIterator interface {
	Next() *URLTestSession
	HasNext() bool
}

type RuntimeServiceStatus struct {
	Status       int32
	ErrorMessage string
}

type RuntimeClashMode struct {
	CurrentMode string
	modeList    []string
}

func (m *RuntimeClashMode) ModeList() StringIterator {
	return newIterator(m.modeList)
}

type RuntimeSnapshot struct {
	SchemaVersion int32
	Sequence      int64
	ObservedAt    int64
	Service       *RuntimeServiceStatus
	StartedAt     int64
	Status        *StatusMessage
	ClashMode     *RuntimeClashMode
	groups        []*OutboundGroup
	urlTests      []*URLTestSession
}

func (s *RuntimeSnapshot) Groups() OutboundGroupIterator {
	return newIterator(s.groups)
}

func (s *RuntimeSnapshot) URLTestSessions() URLTestSessionIterator {
	return newIterator(s.urlTests)
}

type RuntimeEvent struct {
	Type      int32
	Service   *RuntimeServiceStatus
	Status    *StatusMessage
	ClashMode *RuntimeClashMode
	StartedAt int64
	groups    []*OutboundGroup
	urlTests  []*URLTestSession
}

func (e *RuntimeEvent) Groups() OutboundGroupIterator {
	return newIterator(e.groups)
}

func (e *RuntimeEvent) URLTestSessions() URLTestSessionIterator {
	return newIterator(e.urlTests)
}

type RuntimeEventIterator interface {
	Next() *RuntimeEvent
	HasNext() bool
}

type RuntimeEvents struct {
	Sequence int64
	Reset    bool
	Snapshot *RuntimeSnapshot
	events   []*RuntimeEvent
}

func (e *RuntimeEvents) Events() RuntimeEventIterator {
	return newIterator(e.events)
}

type URLTestEvents struct {
	Sequence int64
	Reset    bool
	sessions []*URLTestSession
}

func (e *URLTestEvents) Sessions() URLTestSessionIterator {
	return newIterator(e.sessions)
}

type SystemProxyStatus struct {
	Available bool
	Enabled   bool
}

type OutboundGroup struct {
	Tag        string
	Type       string
	Selectable bool
	Selected   string
	IsExpand   bool
	itemList   []*OutboundGroupItem
}

func (g *OutboundGroup) GetItems() OutboundGroupItemIterator {
	return newIterator(g.itemList)
}

type OutboundGroupIterator interface {
	Next() *OutboundGroup
	HasNext() bool
}

type OutboundGroupItem struct {
	Tag              string
	Type             string
	URLTestTime      int64
	URLTestDelay     int32
	URLTestStatus    string
	URLTestError     string
	URLTestErrorCode string
}

type OutboundExternalInfo struct {
	Ip          string
	CountryCode string
}

type OutboundGroupItemIterator interface {
	Next() *OutboundGroupItem
	HasNext() bool
}

const (
	ConnectionStateAll = iota
	ConnectionStateActive
	ConnectionStateClosed
)

const (
	ConnectionEventNew = iota
	ConnectionEventUpdate
	ConnectionEventClosed
)

const (
	closedConnectionMaxAge = int64((5 * time.Minute) / time.Millisecond)
)

type ConnectionEvent struct {
	Type          int32
	ID            string
	Connection    *Connection
	UplinkDelta   int64
	DownlinkDelta int64
	ClosedAt      int64
}

type ConnectionEvents struct {
	Reset  bool
	events []*ConnectionEvent
}

func (c *ConnectionEvents) Iterator() ConnectionEventIterator {
	return newIterator(c.events)
}

type ConnectionEventIterator interface {
	Next() *ConnectionEvent
	HasNext() bool
}

type Connections struct {
	connectionMap map[string]*Connection
	input         []Connection
	filtered      []Connection
	filterState   int32
	filterApplied bool
}

func NewConnections() *Connections {
	return &Connections{
		connectionMap: make(map[string]*Connection),
	}
}

func (c *Connections) ApplyEvents(events *ConnectionEvents) {
	if events == nil {
		return
	}
	if events.Reset {
		c.connectionMap = make(map[string]*Connection)
	}

	for _, event := range events.events {
		switch event.Type {
		case ConnectionEventNew:
			if event.Connection != nil {
				conn := *event.Connection
				c.connectionMap[event.ID] = &conn
			}
		case ConnectionEventUpdate:
			if conn, ok := c.connectionMap[event.ID]; ok {
				conn.Uplink = event.UplinkDelta
				conn.Downlink = event.DownlinkDelta
				conn.UplinkTotal += event.UplinkDelta
				conn.DownlinkTotal += event.DownlinkDelta
			}
		case ConnectionEventClosed:
			if event.Connection != nil {
				conn := *event.Connection
				conn.ClosedAt = event.ClosedAt
				conn.Uplink = 0
				conn.Downlink = 0
				c.connectionMap[event.ID] = &conn
				continue
			}
			if conn, ok := c.connectionMap[event.ID]; ok {
				conn.ClosedAt = event.ClosedAt
				conn.Uplink = 0
				conn.Downlink = 0
			}
		}
	}

	c.evictClosedConnections(time.Now().UnixMilli())
	c.input = c.input[:0]
	for _, conn := range c.connectionMap {
		c.input = append(c.input, *conn)
	}
	if c.filterApplied {
		c.FilterState(c.filterState)
	} else {
		c.filtered = c.filtered[:0]
		c.filtered = append(c.filtered, c.input...)
	}
}

func (c *Connections) evictClosedConnections(nowMilliseconds int64) {
	for id, conn := range c.connectionMap {
		if conn.ClosedAt == 0 {
			continue
		}
		if nowMilliseconds-conn.ClosedAt > closedConnectionMaxAge {
			delete(c.connectionMap, id)
		}
	}
}

func (c *Connections) FilterState(state int32) {
	c.filterApplied = true
	c.filterState = state
	c.filtered = c.filtered[:0]
	switch state {
	case ConnectionStateAll:
		c.filtered = append(c.filtered, c.input...)
	case ConnectionStateActive:
		for _, connection := range c.input {
			if connection.ClosedAt == 0 {
				c.filtered = append(c.filtered, connection)
			}
		}
	case ConnectionStateClosed:
		for _, connection := range c.input {
			if connection.ClosedAt != 0 {
				c.filtered = append(c.filtered, connection)
			}
		}
	}
}

func (c *Connections) SortByDate() {
	slices.SortStableFunc(c.filtered, func(x, y Connection) int {
		if x.CreatedAt < y.CreatedAt {
			return 1
		} else if x.CreatedAt > y.CreatedAt {
			return -1
		} else {
			return strings.Compare(y.ID, x.ID)
		}
	})
}

func (c *Connections) SortByTraffic() {
	slices.SortStableFunc(c.filtered, func(x, y Connection) int {
		xTraffic := x.Uplink + x.Downlink
		yTraffic := y.Uplink + y.Downlink
		if xTraffic < yTraffic {
			return 1
		} else if xTraffic > yTraffic {
			return -1
		} else {
			return strings.Compare(y.ID, x.ID)
		}
	})
}

func (c *Connections) SortByTrafficTotal() {
	slices.SortStableFunc(c.filtered, func(x, y Connection) int {
		xTraffic := x.UplinkTotal + x.DownlinkTotal
		yTraffic := y.UplinkTotal + y.DownlinkTotal
		if xTraffic < yTraffic {
			return 1
		} else if xTraffic > yTraffic {
			return -1
		} else {
			return strings.Compare(y.ID, x.ID)
		}
	})
}

func (c *Connections) Iterator() ConnectionIterator {
	return newPtrIterator(c.filtered)
}

type ProcessInfo struct {
	ProcessID    int64
	UserID       int32
	UserName     string
	ProcessPath  string
	packageNames []string
}

func (p *ProcessInfo) PackageNames() StringIterator {
	return newIterator(p.packageNames)
}

type Connection struct {
	ID            string
	Inbound       string
	InboundType   string
	IPVersion     int32
	Network       string
	Source        string
	Destination   string
	Domain        string
	Protocol      string
	User          string
	FromOutbound  string
	CreatedAt     int64
	ClosedAt      int64
	Uplink        int64
	Downlink      int64
	UplinkTotal   int64
	DownlinkTotal int64
	Rule          string
	Outbound      string
	OutboundType  string
	chainList     []string
	ProcessInfo   *ProcessInfo
}

func (c *Connection) Chain() StringIterator {
	return newIterator(c.chainList)
}

func (c *Connection) DisplayDestination() string {
	destination := M.ParseSocksaddr(c.Destination)
	if destination.IsIP() && c.Domain != "" {
		destination = M.Socksaddr{
			Fqdn: c.Domain,
			Port: destination.Port,
		}
		return destination.String()
	}
	return c.Destination
}

type ConnectionIterator interface {
	Next() *Connection
	HasNext() bool
}

func statusMessageFromGRPC(status *daemon.Status) *StatusMessage {
	if status == nil {
		return nil
	}
	return &StatusMessage{
		Memory:           int64(status.Memory),
		Goroutines:       status.Goroutines,
		ConnectionsIn:    status.ConnectionsIn,
		ConnectionsOut:   status.ConnectionsOut,
		TrafficAvailable: status.TrafficAvailable,
		Uplink:           status.Uplink,
		Downlink:         status.Downlink,
		UplinkTotal:      status.UplinkTotal,
		DownlinkTotal:    status.DownlinkTotal,
	}
}

func outboundGroupIteratorFromGRPC(groups *daemon.Groups) OutboundGroupIterator {
	return newIterator(outboundGroupListFromGRPC(groups))
}

func outboundGroupListFromGRPC(groups *daemon.Groups) []*OutboundGroup {
	if groups == nil || len(groups.Group) == 0 {
		return []*OutboundGroup{}
	}
	var libboxGroups []*OutboundGroup
	for _, g := range groups.Group {
		libboxGroup := &OutboundGroup{
			Tag:        g.Tag,
			Type:       g.Type,
			Selectable: g.Selectable,
			Selected:   g.Selected,
			IsExpand:   g.IsExpand,
		}
		for _, item := range g.Items {
			libboxGroup.itemList = append(libboxGroup.itemList, &OutboundGroupItem{
				Tag:              item.Tag,
				Type:             item.Type,
				URLTestTime:      item.UrlTestTime,
				URLTestDelay:     item.UrlTestDelay,
				URLTestStatus:    item.UrlTestStatus,
				URLTestError:     item.UrlTestError,
				URLTestErrorCode: item.UrlTestErrorCode,
			})
		}
		libboxGroups = append(libboxGroups, libboxGroup)
	}
	return libboxGroups
}

func connectionFromGRPC(conn *daemon.Connection) Connection {
	var processInfo *ProcessInfo
	if conn.ProcessInfo != nil {
		processInfo = &ProcessInfo{
			ProcessID:    int64(conn.ProcessInfo.ProcessId),
			UserID:       conn.ProcessInfo.UserId,
			UserName:     conn.ProcessInfo.UserName,
			ProcessPath:  conn.ProcessInfo.ProcessPath,
			packageNames: conn.ProcessInfo.PackageNames,
		}
	}
	return Connection{
		ID:            conn.Id,
		Inbound:       conn.Inbound,
		InboundType:   conn.InboundType,
		IPVersion:     conn.IpVersion,
		Network:       conn.Network,
		Source:        conn.Source,
		Destination:   conn.Destination,
		Domain:        conn.Domain,
		Protocol:      conn.Protocol,
		User:          conn.User,
		FromOutbound:  conn.FromOutbound,
		CreatedAt:     conn.CreatedAt,
		ClosedAt:      conn.ClosedAt,
		Uplink:        conn.Uplink,
		Downlink:      conn.Downlink,
		UplinkTotal:   conn.UplinkTotal,
		DownlinkTotal: conn.DownlinkTotal,
		Rule:          conn.Rule,
		Outbound:      conn.Outbound,
		OutboundType:  conn.OutboundType,
		chainList:     conn.ChainList,
		ProcessInfo:   processInfo,
	}
}

func connectionEventFromGRPC(event *daemon.ConnectionEvent) *ConnectionEvent {
	if event == nil {
		return nil
	}
	libboxEvent := &ConnectionEvent{
		Type:          int32(event.Type),
		ID:            event.Id,
		UplinkDelta:   event.UplinkDelta,
		DownlinkDelta: event.DownlinkDelta,
		ClosedAt:      event.ClosedAt,
	}
	if event.Connection != nil {
		conn := connectionFromGRPC(event.Connection)
		libboxEvent.Connection = &conn
	}
	return libboxEvent
}

func connectionEventsFromGRPC(events *daemon.ConnectionEvents) *ConnectionEvents {
	if events == nil {
		return nil
	}
	libboxEvents := &ConnectionEvents{
		Reset: events.Reset_,
	}
	for _, event := range events.Events {
		if libboxEvent := connectionEventFromGRPC(event); libboxEvent != nil {
			libboxEvents.events = append(libboxEvents.events, libboxEvent)
		}
	}
	return libboxEvents
}

func systemProxyStatusFromGRPC(status *daemon.SystemProxyStatus) *SystemProxyStatus {
	if status == nil {
		return nil
	}
	return &SystemProxyStatus{
		Available: status.Available,
		Enabled:   status.Enabled,
	}
}

func urlTestResultFromGRPC(result *daemon.URLTestResult) *URLTestResult {
	if result == nil {
		return nil
	}
	return &URLTestResult{
		OutboundTag:  result.OutboundTag,
		DelayMillis:  result.DelayMillis,
		ObservedAt:   result.ObservedAt,
		Status:       result.Status,
		ErrorCode:    result.ErrorCode,
		ErrorMessage: result.ErrorMessage,
	}
}

func urlTestSessionFromGRPC(session *daemon.URLTestSession) *URLTestSession {
	if session == nil {
		return nil
	}
	result := &URLTestSession{
		ID:           session.Id,
		GroupTag:     session.GroupTag,
		State:        int32(session.State),
		StartedAt:    session.StartedAt,
		CompletedAt:  session.CompletedAt,
		Total:        session.Total,
		Completed:    session.Completed,
		Succeeded:    session.Succeeded,
		Failed:       session.Failed,
		ErrorCode:    session.ErrorCode,
		ErrorMessage: session.ErrorMessage,
	}
	for _, item := range session.Results {
		if converted := urlTestResultFromGRPC(item); converted != nil {
			result.results = append(result.results, converted)
		}
	}
	return result
}

func urlTestSessionListFromGRPC(sessions []*daemon.URLTestSession) []*URLTestSession {
	result := make([]*URLTestSession, 0, len(sessions))
	for _, session := range sessions {
		if converted := urlTestSessionFromGRPC(session); converted != nil {
			result = append(result, converted)
		}
	}
	return result
}

func runtimeServiceStatusFromGRPC(status *daemon.ServiceStatus) *RuntimeServiceStatus {
	if status == nil {
		return nil
	}
	return &RuntimeServiceStatus{Status: int32(status.Status), ErrorMessage: status.ErrorMessage}
}

func runtimeClashModeFromGRPC(mode *daemon.ClashModeStatus) *RuntimeClashMode {
	if mode == nil {
		return nil
	}
	return &RuntimeClashMode{
		CurrentMode: mode.CurrentMode,
		modeList:    append([]string(nil), mode.ModeList...),
	}
}

func runtimeSnapshotFromGRPC(snapshot *daemon.RuntimeSnapshot) *RuntimeSnapshot {
	if snapshot == nil {
		return nil
	}
	return &RuntimeSnapshot{
		SchemaVersion: snapshot.SchemaVersion,
		Sequence:      int64(snapshot.Sequence),
		ObservedAt:    snapshot.ObservedAt,
		Service:       runtimeServiceStatusFromGRPC(snapshot.Service),
		StartedAt:     snapshot.StartedAt,
		Status:        statusMessageFromGRPC(snapshot.Status),
		ClashMode:     runtimeClashModeFromGRPC(snapshot.ClashMode),
		groups:        outboundGroupListFromGRPC(snapshot.Groups),
		urlTests:      urlTestSessionListFromGRPC(snapshot.UrlTestSessions),
	}
}

func runtimeEventFromGRPC(event *daemon.RuntimeEvent) *RuntimeEvent {
	if event == nil {
		return nil
	}
	return &RuntimeEvent{
		Type:      int32(event.Type),
		Service:   runtimeServiceStatusFromGRPC(event.Service),
		Status:    statusMessageFromGRPC(event.Status),
		ClashMode: runtimeClashModeFromGRPC(event.ClashMode),
		StartedAt: event.StartedAt,
		groups:    outboundGroupListFromGRPC(event.Groups),
		urlTests:  urlTestSessionListFromGRPC(event.UrlTestSessions),
	}
}

func runtimeEventsFromGRPC(events *daemon.RuntimeEvents) *RuntimeEvents {
	if events == nil {
		return nil
	}
	result := &RuntimeEvents{
		Sequence: int64(events.Sequence),
		Reset:    events.Reset_,
		Snapshot: runtimeSnapshotFromGRPC(events.Snapshot),
	}
	for _, event := range events.Events {
		if converted := runtimeEventFromGRPC(event); converted != nil {
			result.events = append(result.events, converted)
		}
	}
	return result
}

func urlTestEventsFromGRPC(events *daemon.URLTestEvents) *URLTestEvents {
	if events == nil {
		return nil
	}
	return &URLTestEvents{
		Sequence: int64(events.Sequence),
		Reset:    events.Reset_,
		sessions: urlTestSessionListFromGRPC(events.Sessions),
	}
}
