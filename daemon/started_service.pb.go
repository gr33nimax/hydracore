package daemon

import (
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type LogLevel int32

const (
	LogLevel_PANIC  LogLevel = 0
	LogLevel_FATAL  LogLevel = 1
	LogLevel_ERROR  LogLevel = 2
	LogLevel_WARN   LogLevel = 3
	LogLevel_NOTICE LogLevel = 4
	LogLevel_INFO   LogLevel = 5
	LogLevel_DEBUG  LogLevel = 6
	LogLevel_TRACE  LogLevel = 7
)

// Enum value maps for LogLevel.
var (
	LogLevel_name = map[int32]string{
		0: "PANIC",
		1: "FATAL",
		2: "ERROR",
		3: "WARN",
		4: "NOTICE",
		5: "INFO",
		6: "DEBUG",
		7: "TRACE",
	}
	LogLevel_value = map[string]int32{
		"PANIC":  0,
		"FATAL":  1,
		"ERROR":  2,
		"WARN":   3,
		"NOTICE": 4,
		"INFO":   5,
		"DEBUG":  6,
		"TRACE":  7,
	}
)

func (x LogLevel) Enum() *LogLevel {
	p := new(LogLevel)
	*p = x
	return p
}

func (x LogLevel) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (LogLevel) Descriptor() protoreflect.EnumDescriptor {
	return file_daemon_started_service_proto_enumTypes[0].Descriptor()
}

func (LogLevel) Type() protoreflect.EnumType {
	return &file_daemon_started_service_proto_enumTypes[0]
}

func (x LogLevel) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use LogLevel.Descriptor instead.
func (LogLevel) EnumDescriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{0}
}

type URLTestSessionState int32

const (
	URLTestSessionState_URL_TEST_SESSION_QUEUED    URLTestSessionState = 0
	URLTestSessionState_URL_TEST_SESSION_RUNNING   URLTestSessionState = 1
	URLTestSessionState_URL_TEST_SESSION_SUCCEEDED URLTestSessionState = 2
	URLTestSessionState_URL_TEST_SESSION_FAILED    URLTestSessionState = 3
	URLTestSessionState_URL_TEST_SESSION_CANCELLED URLTestSessionState = 4
)

// Enum value maps for URLTestSessionState.
var (
	URLTestSessionState_name = map[int32]string{
		0: "URL_TEST_SESSION_QUEUED",
		1: "URL_TEST_SESSION_RUNNING",
		2: "URL_TEST_SESSION_SUCCEEDED",
		3: "URL_TEST_SESSION_FAILED",
		4: "URL_TEST_SESSION_CANCELLED",
	}
	URLTestSessionState_value = map[string]int32{
		"URL_TEST_SESSION_QUEUED":    0,
		"URL_TEST_SESSION_RUNNING":   1,
		"URL_TEST_SESSION_SUCCEEDED": 2,
		"URL_TEST_SESSION_FAILED":    3,
		"URL_TEST_SESSION_CANCELLED": 4,
	}
)

func (x URLTestSessionState) Enum() *URLTestSessionState {
	p := new(URLTestSessionState)
	*p = x
	return p
}

func (x URLTestSessionState) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (URLTestSessionState) Descriptor() protoreflect.EnumDescriptor {
	return file_daemon_started_service_proto_enumTypes[1].Descriptor()
}

func (URLTestSessionState) Type() protoreflect.EnumType {
	return &file_daemon_started_service_proto_enumTypes[1]
}

func (x URLTestSessionState) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use URLTestSessionState.Descriptor instead.
func (URLTestSessionState) EnumDescriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{1}
}

type RuntimeEventType int32

const (
	RuntimeEventType_RUNTIME_EVENT_SERVICE           RuntimeEventType = 0
	RuntimeEventType_RUNTIME_EVENT_STATUS            RuntimeEventType = 1
	RuntimeEventType_RUNTIME_EVENT_GROUPS            RuntimeEventType = 2
	RuntimeEventType_RUNTIME_EVENT_CLASH_MODE        RuntimeEventType = 3
	RuntimeEventType_RUNTIME_EVENT_URL_TEST_SESSIONS RuntimeEventType = 4
)

// Enum value maps for RuntimeEventType.
var (
	RuntimeEventType_name = map[int32]string{
		0: "RUNTIME_EVENT_SERVICE",
		1: "RUNTIME_EVENT_STATUS",
		2: "RUNTIME_EVENT_GROUPS",
		3: "RUNTIME_EVENT_CLASH_MODE",
		4: "RUNTIME_EVENT_URL_TEST_SESSIONS",
	}
	RuntimeEventType_value = map[string]int32{
		"RUNTIME_EVENT_SERVICE":           0,
		"RUNTIME_EVENT_STATUS":            1,
		"RUNTIME_EVENT_GROUPS":            2,
		"RUNTIME_EVENT_CLASH_MODE":        3,
		"RUNTIME_EVENT_URL_TEST_SESSIONS": 4,
	}
)

func (x RuntimeEventType) Enum() *RuntimeEventType {
	p := new(RuntimeEventType)
	*p = x
	return p
}

func (x RuntimeEventType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (RuntimeEventType) Descriptor() protoreflect.EnumDescriptor {
	return file_daemon_started_service_proto_enumTypes[2].Descriptor()
}

func (RuntimeEventType) Type() protoreflect.EnumType {
	return &file_daemon_started_service_proto_enumTypes[2]
}

func (x RuntimeEventType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use RuntimeEventType.Descriptor instead.
func (RuntimeEventType) EnumDescriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{2}
}

type ConnectionEventType int32

const (
	ConnectionEventType_CONNECTION_EVENT_NEW    ConnectionEventType = 0
	ConnectionEventType_CONNECTION_EVENT_UPDATE ConnectionEventType = 1
	ConnectionEventType_CONNECTION_EVENT_CLOSED ConnectionEventType = 2
)

// Enum value maps for ConnectionEventType.
var (
	ConnectionEventType_name = map[int32]string{
		0: "CONNECTION_EVENT_NEW",
		1: "CONNECTION_EVENT_UPDATE",
		2: "CONNECTION_EVENT_CLOSED",
	}
	ConnectionEventType_value = map[string]int32{
		"CONNECTION_EVENT_NEW":    0,
		"CONNECTION_EVENT_UPDATE": 1,
		"CONNECTION_EVENT_CLOSED": 2,
	}
)

func (x ConnectionEventType) Enum() *ConnectionEventType {
	p := new(ConnectionEventType)
	*p = x
	return p
}

func (x ConnectionEventType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ConnectionEventType) Descriptor() protoreflect.EnumDescriptor {
	return file_daemon_started_service_proto_enumTypes[3].Descriptor()
}

func (ConnectionEventType) Type() protoreflect.EnumType {
	return &file_daemon_started_service_proto_enumTypes[3]
}

func (x ConnectionEventType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ConnectionEventType.Descriptor instead.
func (ConnectionEventType) EnumDescriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{3}
}

type ServiceStatus_Type int32

const (
	ServiceStatus_IDLE     ServiceStatus_Type = 0
	ServiceStatus_STARTING ServiceStatus_Type = 1
	ServiceStatus_STARTED  ServiceStatus_Type = 2
	ServiceStatus_STOPPING ServiceStatus_Type = 3
	ServiceStatus_FATAL    ServiceStatus_Type = 4
)

// Enum value maps for ServiceStatus_Type.
var (
	ServiceStatus_Type_name = map[int32]string{
		0: "IDLE",
		1: "STARTING",
		2: "STARTED",
		3: "STOPPING",
		4: "FATAL",
	}
	ServiceStatus_Type_value = map[string]int32{
		"IDLE":     0,
		"STARTING": 1,
		"STARTED":  2,
		"STOPPING": 3,
		"FATAL":    4,
	}
)

func (x ServiceStatus_Type) Enum() *ServiceStatus_Type {
	p := new(ServiceStatus_Type)
	*p = x
	return p
}

func (x ServiceStatus_Type) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ServiceStatus_Type) Descriptor() protoreflect.EnumDescriptor {
	return file_daemon_started_service_proto_enumTypes[4].Descriptor()
}

func (ServiceStatus_Type) Type() protoreflect.EnumType {
	return &file_daemon_started_service_proto_enumTypes[4]
}

func (x ServiceStatus_Type) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ServiceStatus_Type.Descriptor instead.
func (ServiceStatus_Type) EnumDescriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{0, 0}
}

type ServiceStatus struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Status        ServiceStatus_Type     `protobuf:"varint,1,opt,name=status,proto3,enum=daemon.ServiceStatus_Type" json:"status,omitempty"`
	ErrorMessage  string                 `protobuf:"bytes,2,opt,name=errorMessage,proto3" json:"errorMessage,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ServiceStatus) Reset() {
	*x = ServiceStatus{}
	mi := &file_daemon_started_service_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ServiceStatus) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ServiceStatus) ProtoMessage() {}

func (x *ServiceStatus) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ServiceStatus.ProtoReflect.Descriptor instead.
func (*ServiceStatus) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{0}
}

func (x *ServiceStatus) GetStatus() ServiceStatus_Type {
	if x != nil {
		return x.Status
	}
	return ServiceStatus_IDLE
}

func (x *ServiceStatus) GetErrorMessage() string {
	if x != nil {
		return x.ErrorMessage
	}
	return ""
}

type ReloadServiceRequest struct {
	state             protoimpl.MessageState `protogen:"open.v1"`
	NewProfileContent string                 `protobuf:"bytes,1,opt,name=newProfileContent,proto3" json:"newProfileContent,omitempty"`
	unknownFields     protoimpl.UnknownFields
	sizeCache         protoimpl.SizeCache
}

func (x *ReloadServiceRequest) Reset() {
	*x = ReloadServiceRequest{}
	mi := &file_daemon_started_service_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ReloadServiceRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReloadServiceRequest) ProtoMessage() {}

func (x *ReloadServiceRequest) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReloadServiceRequest.ProtoReflect.Descriptor instead.
func (*ReloadServiceRequest) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{1}
}

func (x *ReloadServiceRequest) GetNewProfileContent() string {
	if x != nil {
		return x.NewProfileContent
	}
	return ""
}

type SubscribeStatusRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Interval      int64                  `protobuf:"varint,1,opt,name=interval,proto3" json:"interval,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SubscribeStatusRequest) Reset() {
	*x = SubscribeStatusRequest{}
	mi := &file_daemon_started_service_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SubscribeStatusRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SubscribeStatusRequest) ProtoMessage() {}

func (x *SubscribeStatusRequest) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SubscribeStatusRequest.ProtoReflect.Descriptor instead.
func (*SubscribeStatusRequest) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{2}
}

func (x *SubscribeStatusRequest) GetInterval() int64 {
	if x != nil {
		return x.Interval
	}
	return 0
}

type Log struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Messages      []*Log_Message         `protobuf:"bytes,1,rep,name=messages,proto3" json:"messages,omitempty"`
	Reset_        bool                   `protobuf:"varint,2,opt,name=reset,proto3" json:"reset,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Log) Reset() {
	*x = Log{}
	mi := &file_daemon_started_service_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Log) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Log) ProtoMessage() {}

func (x *Log) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Log.ProtoReflect.Descriptor instead.
func (*Log) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{3}
}

func (x *Log) GetMessages() []*Log_Message {
	if x != nil {
		return x.Messages
	}
	return nil
}

func (x *Log) GetReset_() bool {
	if x != nil {
		return x.Reset_
	}
	return false
}

type DefaultLogLevel struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Level         LogLevel               `protobuf:"varint,1,opt,name=level,proto3,enum=daemon.LogLevel" json:"level,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *DefaultLogLevel) Reset() {
	*x = DefaultLogLevel{}
	mi := &file_daemon_started_service_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DefaultLogLevel) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DefaultLogLevel) ProtoMessage() {}

func (x *DefaultLogLevel) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DefaultLogLevel.ProtoReflect.Descriptor instead.
func (*DefaultLogLevel) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{4}
}

func (x *DefaultLogLevel) GetLevel() LogLevel {
	if x != nil {
		return x.Level
	}
	return LogLevel_PANIC
}

type Status struct {
	state            protoimpl.MessageState `protogen:"open.v1"`
	Memory           uint64                 `protobuf:"varint,1,opt,name=memory,proto3" json:"memory,omitempty"`
	Goroutines       int32                  `protobuf:"varint,2,opt,name=goroutines,proto3" json:"goroutines,omitempty"`
	ConnectionsIn    int32                  `protobuf:"varint,3,opt,name=connectionsIn,proto3" json:"connectionsIn,omitempty"`
	ConnectionsOut   int32                  `protobuf:"varint,4,opt,name=connectionsOut,proto3" json:"connectionsOut,omitempty"`
	TrafficAvailable bool                   `protobuf:"varint,5,opt,name=trafficAvailable,proto3" json:"trafficAvailable,omitempty"`
	Uplink           int64                  `protobuf:"varint,6,opt,name=uplink,proto3" json:"uplink,omitempty"`
	Downlink         int64                  `protobuf:"varint,7,opt,name=downlink,proto3" json:"downlink,omitempty"`
	UplinkTotal      int64                  `protobuf:"varint,8,opt,name=uplinkTotal,proto3" json:"uplinkTotal,omitempty"`
	DownlinkTotal    int64                  `protobuf:"varint,9,opt,name=downlinkTotal,proto3" json:"downlinkTotal,omitempty"`
	unknownFields    protoimpl.UnknownFields
	sizeCache        protoimpl.SizeCache
}

func (x *Status) Reset() {
	*x = Status{}
	mi := &file_daemon_started_service_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Status) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Status) ProtoMessage() {}

func (x *Status) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Status.ProtoReflect.Descriptor instead.
func (*Status) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{5}
}

func (x *Status) GetMemory() uint64 {
	if x != nil {
		return x.Memory
	}
	return 0
}

func (x *Status) GetGoroutines() int32 {
	if x != nil {
		return x.Goroutines
	}
	return 0
}

func (x *Status) GetConnectionsIn() int32 {
	if x != nil {
		return x.ConnectionsIn
	}
	return 0
}

func (x *Status) GetConnectionsOut() int32 {
	if x != nil {
		return x.ConnectionsOut
	}
	return 0
}

func (x *Status) GetTrafficAvailable() bool {
	if x != nil {
		return x.TrafficAvailable
	}
	return false
}

func (x *Status) GetUplink() int64 {
	if x != nil {
		return x.Uplink
	}
	return 0
}

func (x *Status) GetDownlink() int64 {
	if x != nil {
		return x.Downlink
	}
	return 0
}

func (x *Status) GetUplinkTotal() int64 {
	if x != nil {
		return x.UplinkTotal
	}
	return 0
}

func (x *Status) GetDownlinkTotal() int64 {
	if x != nil {
		return x.DownlinkTotal
	}
	return 0
}

type Groups struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Group         []*Group               `protobuf:"bytes,1,rep,name=group,proto3" json:"group,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Groups) Reset() {
	*x = Groups{}
	mi := &file_daemon_started_service_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Groups) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Groups) ProtoMessage() {}

func (x *Groups) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Groups.ProtoReflect.Descriptor instead.
func (*Groups) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{6}
}

func (x *Groups) GetGroup() []*Group {
	if x != nil {
		return x.Group
	}
	return nil
}

type Group struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Tag           string                 `protobuf:"bytes,1,opt,name=tag,proto3" json:"tag,omitempty"`
	Type          string                 `protobuf:"bytes,2,opt,name=type,proto3" json:"type,omitempty"`
	Selectable    bool                   `protobuf:"varint,3,opt,name=selectable,proto3" json:"selectable,omitempty"`
	Selected      string                 `protobuf:"bytes,4,opt,name=selected,proto3" json:"selected,omitempty"`
	IsExpand      bool                   `protobuf:"varint,5,opt,name=isExpand,proto3" json:"isExpand,omitempty"`
	Items         []*GroupItem           `protobuf:"bytes,6,rep,name=items,proto3" json:"items,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Group) Reset() {
	*x = Group{}
	mi := &file_daemon_started_service_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Group) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Group) ProtoMessage() {}

func (x *Group) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Group.ProtoReflect.Descriptor instead.
func (*Group) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{7}
}

func (x *Group) GetTag() string {
	if x != nil {
		return x.Tag
	}
	return ""
}

func (x *Group) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

func (x *Group) GetSelectable() bool {
	if x != nil {
		return x.Selectable
	}
	return false
}

func (x *Group) GetSelected() string {
	if x != nil {
		return x.Selected
	}
	return ""
}

func (x *Group) GetIsExpand() bool {
	if x != nil {
		return x.IsExpand
	}
	return false
}

func (x *Group) GetItems() []*GroupItem {
	if x != nil {
		return x.Items
	}
	return nil
}

type GroupItem struct {
	state            protoimpl.MessageState `protogen:"open.v1"`
	Tag              string                 `protobuf:"bytes,1,opt,name=tag,proto3" json:"tag,omitempty"`
	Type             string                 `protobuf:"bytes,2,opt,name=type,proto3" json:"type,omitempty"`
	UrlTestTime      int64                  `protobuf:"varint,3,opt,name=urlTestTime,proto3" json:"urlTestTime,omitempty"`
	UrlTestDelay     int32                  `protobuf:"varint,4,opt,name=urlTestDelay,proto3" json:"urlTestDelay,omitempty"`
	UrlTestStatus    string                 `protobuf:"bytes,5,opt,name=urlTestStatus,proto3" json:"urlTestStatus,omitempty"`
	UrlTestError     string                 `protobuf:"bytes,6,opt,name=urlTestError,proto3" json:"urlTestError,omitempty"`
	UrlTestErrorCode string                 `protobuf:"bytes,7,opt,name=urlTestErrorCode,proto3" json:"urlTestErrorCode,omitempty"`
	unknownFields    protoimpl.UnknownFields
	sizeCache        protoimpl.SizeCache
}

func (x *GroupItem) Reset() {
	*x = GroupItem{}
	mi := &file_daemon_started_service_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GroupItem) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GroupItem) ProtoMessage() {}

func (x *GroupItem) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GroupItem.ProtoReflect.Descriptor instead.
func (*GroupItem) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{8}
}

func (x *GroupItem) GetTag() string {
	if x != nil {
		return x.Tag
	}
	return ""
}

func (x *GroupItem) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

func (x *GroupItem) GetUrlTestTime() int64 {
	if x != nil {
		return x.UrlTestTime
	}
	return 0
}

func (x *GroupItem) GetUrlTestDelay() int32 {
	if x != nil {
		return x.UrlTestDelay
	}
	return 0
}

func (x *GroupItem) GetUrlTestStatus() string {
	if x != nil {
		return x.UrlTestStatus
	}
	return ""
}

func (x *GroupItem) GetUrlTestError() string {
	if x != nil {
		return x.UrlTestError
	}
	return ""
}

func (x *GroupItem) GetUrlTestErrorCode() string {
	if x != nil {
		return x.UrlTestErrorCode
	}
	return ""
}

type URLTestRequest struct {
	state               protoimpl.MessageState `protogen:"open.v1"`
	GroupTag            string                 `protobuf:"bytes,1,opt,name=groupTag,proto3" json:"groupTag,omitempty"`
	UrlTestUrl          string                 `protobuf:"bytes,2,opt,name=urlTestUrl,proto3" json:"urlTestUrl,omitempty"`
	TargetOutboundTag   string                 `protobuf:"bytes,3,opt,name=targetOutboundTag,proto3" json:"targetOutboundTag,omitempty"`
	PriorityOutboundTag string                 `protobuf:"bytes,4,opt,name=priorityOutboundTag,proto3" json:"priorityOutboundTag,omitempty"`
	ExcludeOutboundTag  string                 `protobuf:"bytes,5,opt,name=excludeOutboundTag,proto3" json:"excludeOutboundTag,omitempty"`
	TimeoutMillis       int32                  `protobuf:"varint,6,opt,name=timeoutMillis,proto3" json:"timeoutMillis,omitempty"`
	Concurrency         int32                  `protobuf:"varint,7,opt,name=concurrency,proto3" json:"concurrency,omitempty"`
	DeadlineMillis      int32                  `protobuf:"varint,8,opt,name=deadlineMillis,proto3" json:"deadlineMillis,omitempty"`
	Force               bool                   `protobuf:"varint,9,opt,name=force,proto3" json:"force,omitempty"`
	unknownFields       protoimpl.UnknownFields
	sizeCache           protoimpl.SizeCache
}

func (x *URLTestRequest) Reset() {
	*x = URLTestRequest{}
	mi := &file_daemon_started_service_proto_msgTypes[9]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *URLTestRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*URLTestRequest) ProtoMessage() {}

func (x *URLTestRequest) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[9]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use URLTestRequest.ProtoReflect.Descriptor instead.
func (*URLTestRequest) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{9}
}

func (x *URLTestRequest) GetGroupTag() string {
	if x != nil {
		return x.GroupTag
	}
	return ""
}

func (x *URLTestRequest) GetUrlTestUrl() string {
	if x != nil {
		return x.UrlTestUrl
	}
	return ""
}

func (x *URLTestRequest) GetTargetOutboundTag() string {
	if x != nil {
		return x.TargetOutboundTag
	}
	return ""
}

func (x *URLTestRequest) GetPriorityOutboundTag() string {
	if x != nil {
		return x.PriorityOutboundTag
	}
	return ""
}

func (x *URLTestRequest) GetExcludeOutboundTag() string {
	if x != nil {
		return x.ExcludeOutboundTag
	}
	return ""
}

func (x *URLTestRequest) GetTimeoutMillis() int32 {
	if x != nil {
		return x.TimeoutMillis
	}
	return 0
}

func (x *URLTestRequest) GetConcurrency() int32 {
	if x != nil {
		return x.Concurrency
	}
	return 0
}

func (x *URLTestRequest) GetDeadlineMillis() int32 {
	if x != nil {
		return x.DeadlineMillis
	}
	return 0
}

func (x *URLTestRequest) GetForce() bool {
	if x != nil {
		return x.Force
	}
	return false
}

type URLTestResult struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	OutboundTag   string                 `protobuf:"bytes,1,opt,name=outboundTag,proto3" json:"outboundTag,omitempty"`
	DelayMillis   int64                  `protobuf:"varint,2,opt,name=delayMillis,proto3" json:"delayMillis,omitempty"`
	ObservedAt    int64                  `protobuf:"varint,3,opt,name=observedAt,proto3" json:"observedAt,omitempty"`
	Status        string                 `protobuf:"bytes,4,opt,name=status,proto3" json:"status,omitempty"`
	ErrorCode     string                 `protobuf:"bytes,5,opt,name=errorCode,proto3" json:"errorCode,omitempty"`
	ErrorMessage  string                 `protobuf:"bytes,6,opt,name=errorMessage,proto3" json:"errorMessage,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *URLTestResult) Reset() {
	*x = URLTestResult{}
	mi := &file_daemon_started_service_proto_msgTypes[10]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *URLTestResult) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*URLTestResult) ProtoMessage() {}

func (x *URLTestResult) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[10]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use URLTestResult.ProtoReflect.Descriptor instead.
func (*URLTestResult) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{10}
}

func (x *URLTestResult) GetOutboundTag() string {
	if x != nil {
		return x.OutboundTag
	}
	return ""
}

func (x *URLTestResult) GetDelayMillis() int64 {
	if x != nil {
		return x.DelayMillis
	}
	return 0
}

func (x *URLTestResult) GetObservedAt() int64 {
	if x != nil {
		return x.ObservedAt
	}
	return 0
}

func (x *URLTestResult) GetStatus() string {
	if x != nil {
		return x.Status
	}
	return ""
}

func (x *URLTestResult) GetErrorCode() string {
	if x != nil {
		return x.ErrorCode
	}
	return ""
}

func (x *URLTestResult) GetErrorMessage() string {
	if x != nil {
		return x.ErrorMessage
	}
	return ""
}

type URLTestSession struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	GroupTag      string                 `protobuf:"bytes,2,opt,name=groupTag,proto3" json:"groupTag,omitempty"`
	State         URLTestSessionState    `protobuf:"varint,3,opt,name=state,proto3,enum=daemon.URLTestSessionState" json:"state,omitempty"`
	StartedAt     int64                  `protobuf:"varint,4,opt,name=startedAt,proto3" json:"startedAt,omitempty"`
	CompletedAt   int64                  `protobuf:"varint,5,opt,name=completedAt,proto3" json:"completedAt,omitempty"`
	Total         int32                  `protobuf:"varint,6,opt,name=total,proto3" json:"total,omitempty"`
	Completed     int32                  `protobuf:"varint,7,opt,name=completed,proto3" json:"completed,omitempty"`
	Succeeded     int32                  `protobuf:"varint,8,opt,name=succeeded,proto3" json:"succeeded,omitempty"`
	Failed        int32                  `protobuf:"varint,9,opt,name=failed,proto3" json:"failed,omitempty"`
	Results       []*URLTestResult       `protobuf:"bytes,10,rep,name=results,proto3" json:"results,omitempty"`
	ErrorCode     string                 `protobuf:"bytes,11,opt,name=errorCode,proto3" json:"errorCode,omitempty"`
	ErrorMessage  string                 `protobuf:"bytes,12,opt,name=errorMessage,proto3" json:"errorMessage,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *URLTestSession) Reset() {
	*x = URLTestSession{}
	mi := &file_daemon_started_service_proto_msgTypes[11]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *URLTestSession) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*URLTestSession) ProtoMessage() {}

func (x *URLTestSession) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[11]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use URLTestSession.ProtoReflect.Descriptor instead.
func (*URLTestSession) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{11}
}

func (x *URLTestSession) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *URLTestSession) GetGroupTag() string {
	if x != nil {
		return x.GroupTag
	}
	return ""
}

func (x *URLTestSession) GetState() URLTestSessionState {
	if x != nil {
		return x.State
	}
	return URLTestSessionState_URL_TEST_SESSION_QUEUED
}

func (x *URLTestSession) GetStartedAt() int64 {
	if x != nil {
		return x.StartedAt
	}
	return 0
}

func (x *URLTestSession) GetCompletedAt() int64 {
	if x != nil {
		return x.CompletedAt
	}
	return 0
}

func (x *URLTestSession) GetTotal() int32 {
	if x != nil {
		return x.Total
	}
	return 0
}

func (x *URLTestSession) GetCompleted() int32 {
	if x != nil {
		return x.Completed
	}
	return 0
}

func (x *URLTestSession) GetSucceeded() int32 {
	if x != nil {
		return x.Succeeded
	}
	return 0
}

func (x *URLTestSession) GetFailed() int32 {
	if x != nil {
		return x.Failed
	}
	return 0
}

func (x *URLTestSession) GetResults() []*URLTestResult {
	if x != nil {
		return x.Results
	}
	return nil
}

func (x *URLTestSession) GetErrorCode() string {
	if x != nil {
		return x.ErrorCode
	}
	return ""
}

func (x *URLTestSession) GetErrorMessage() string {
	if x != nil {
		return x.ErrorMessage
	}
	return ""
}

type URLTestSessionRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *URLTestSessionRequest) Reset() {
	*x = URLTestSessionRequest{}
	mi := &file_daemon_started_service_proto_msgTypes[12]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *URLTestSessionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*URLTestSessionRequest) ProtoMessage() {}

func (x *URLTestSessionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[12]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use URLTestSessionRequest.ProtoReflect.Descriptor instead.
func (*URLTestSessionRequest) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{12}
}

func (x *URLTestSessionRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

type URLTestEventRequest struct {
	state          protoimpl.MessageState `protogen:"open.v1"`
	IntervalMillis int64                  `protobuf:"varint,1,opt,name=intervalMillis,proto3" json:"intervalMillis,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *URLTestEventRequest) Reset() {
	*x = URLTestEventRequest{}
	mi := &file_daemon_started_service_proto_msgTypes[13]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *URLTestEventRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*URLTestEventRequest) ProtoMessage() {}

func (x *URLTestEventRequest) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[13]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use URLTestEventRequest.ProtoReflect.Descriptor instead.
func (*URLTestEventRequest) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{13}
}

func (x *URLTestEventRequest) GetIntervalMillis() int64 {
	if x != nil {
		return x.IntervalMillis
	}
	return 0
}

type URLTestEvents struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Sequence      uint64                 `protobuf:"varint,1,opt,name=sequence,proto3" json:"sequence,omitempty"`
	Reset_        bool                   `protobuf:"varint,2,opt,name=reset,proto3" json:"reset,omitempty"`
	Sessions      []*URLTestSession      `protobuf:"bytes,3,rep,name=sessions,proto3" json:"sessions,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *URLTestEvents) Reset() {
	*x = URLTestEvents{}
	mi := &file_daemon_started_service_proto_msgTypes[14]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *URLTestEvents) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*URLTestEvents) ProtoMessage() {}

func (x *URLTestEvents) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[14]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use URLTestEvents.ProtoReflect.Descriptor instead.
func (*URLTestEvents) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{14}
}

func (x *URLTestEvents) GetSequence() uint64 {
	if x != nil {
		return x.Sequence
	}
	return 0
}

func (x *URLTestEvents) GetReset_() bool {
	if x != nil {
		return x.Reset_
	}
	return false
}

func (x *URLTestEvents) GetSessions() []*URLTestSession {
	if x != nil {
		return x.Sessions
	}
	return nil
}

type RuntimeSnapshot struct {
	state           protoimpl.MessageState `protogen:"open.v1"`
	SchemaVersion   int32                  `protobuf:"varint,1,opt,name=schemaVersion,proto3" json:"schemaVersion,omitempty"`
	Sequence        uint64                 `protobuf:"varint,2,opt,name=sequence,proto3" json:"sequence,omitempty"`
	ObservedAt      int64                  `protobuf:"varint,3,opt,name=observedAt,proto3" json:"observedAt,omitempty"`
	Service         *ServiceStatus         `protobuf:"bytes,4,opt,name=service,proto3" json:"service,omitempty"`
	StartedAt       int64                  `protobuf:"varint,5,opt,name=startedAt,proto3" json:"startedAt,omitempty"`
	Status          *Status                `protobuf:"bytes,6,opt,name=status,proto3" json:"status,omitempty"`
	Groups          *Groups                `protobuf:"bytes,7,opt,name=groups,proto3" json:"groups,omitempty"`
	ClashMode       *ClashModeStatus       `protobuf:"bytes,8,opt,name=clashMode,proto3" json:"clashMode,omitempty"`
	UrlTestSessions []*URLTestSession      `protobuf:"bytes,9,rep,name=urlTestSessions,proto3" json:"urlTestSessions,omitempty"`
	unknownFields   protoimpl.UnknownFields
	sizeCache       protoimpl.SizeCache
}

func (x *RuntimeSnapshot) Reset() {
	*x = RuntimeSnapshot{}
	mi := &file_daemon_started_service_proto_msgTypes[15]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RuntimeSnapshot) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RuntimeSnapshot) ProtoMessage() {}

func (x *RuntimeSnapshot) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[15]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RuntimeSnapshot.ProtoReflect.Descriptor instead.
func (*RuntimeSnapshot) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{15}
}

func (x *RuntimeSnapshot) GetSchemaVersion() int32 {
	if x != nil {
		return x.SchemaVersion
	}
	return 0
}

func (x *RuntimeSnapshot) GetSequence() uint64 {
	if x != nil {
		return x.Sequence
	}
	return 0
}

func (x *RuntimeSnapshot) GetObservedAt() int64 {
	if x != nil {
		return x.ObservedAt
	}
	return 0
}

func (x *RuntimeSnapshot) GetService() *ServiceStatus {
	if x != nil {
		return x.Service
	}
	return nil
}

func (x *RuntimeSnapshot) GetStartedAt() int64 {
	if x != nil {
		return x.StartedAt
	}
	return 0
}

func (x *RuntimeSnapshot) GetStatus() *Status {
	if x != nil {
		return x.Status
	}
	return nil
}

func (x *RuntimeSnapshot) GetGroups() *Groups {
	if x != nil {
		return x.Groups
	}
	return nil
}

func (x *RuntimeSnapshot) GetClashMode() *ClashModeStatus {
	if x != nil {
		return x.ClashMode
	}
	return nil
}

func (x *RuntimeSnapshot) GetUrlTestSessions() []*URLTestSession {
	if x != nil {
		return x.UrlTestSessions
	}
	return nil
}

type RuntimeEventRequest struct {
	state          protoimpl.MessageState `protogen:"open.v1"`
	IntervalMillis int64                  `protobuf:"varint,1,opt,name=intervalMillis,proto3" json:"intervalMillis,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *RuntimeEventRequest) Reset() {
	*x = RuntimeEventRequest{}
	mi := &file_daemon_started_service_proto_msgTypes[16]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RuntimeEventRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RuntimeEventRequest) ProtoMessage() {}

func (x *RuntimeEventRequest) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[16]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RuntimeEventRequest.ProtoReflect.Descriptor instead.
func (*RuntimeEventRequest) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{16}
}

func (x *RuntimeEventRequest) GetIntervalMillis() int64 {
	if x != nil {
		return x.IntervalMillis
	}
	return 0
}

type RuntimeEvent struct {
	state           protoimpl.MessageState `protogen:"open.v1"`
	Type            RuntimeEventType       `protobuf:"varint,1,opt,name=type,proto3,enum=daemon.RuntimeEventType" json:"type,omitempty"`
	Service         *ServiceStatus         `protobuf:"bytes,2,opt,name=service,proto3" json:"service,omitempty"`
	Status          *Status                `protobuf:"bytes,3,opt,name=status,proto3" json:"status,omitempty"`
	Groups          *Groups                `protobuf:"bytes,4,opt,name=groups,proto3" json:"groups,omitempty"`
	ClashMode       *ClashModeStatus       `protobuf:"bytes,5,opt,name=clashMode,proto3" json:"clashMode,omitempty"`
	UrlTestSessions []*URLTestSession      `protobuf:"bytes,6,rep,name=urlTestSessions,proto3" json:"urlTestSessions,omitempty"`
	StartedAt       int64                  `protobuf:"varint,7,opt,name=startedAt,proto3" json:"startedAt,omitempty"`
	unknownFields   protoimpl.UnknownFields
	sizeCache       protoimpl.SizeCache
}

func (x *RuntimeEvent) Reset() {
	*x = RuntimeEvent{}
	mi := &file_daemon_started_service_proto_msgTypes[17]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RuntimeEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RuntimeEvent) ProtoMessage() {}

func (x *RuntimeEvent) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[17]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RuntimeEvent.ProtoReflect.Descriptor instead.
func (*RuntimeEvent) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{17}
}

func (x *RuntimeEvent) GetType() RuntimeEventType {
	if x != nil {
		return x.Type
	}
	return RuntimeEventType_RUNTIME_EVENT_SERVICE
}

func (x *RuntimeEvent) GetService() *ServiceStatus {
	if x != nil {
		return x.Service
	}
	return nil
}

func (x *RuntimeEvent) GetStatus() *Status {
	if x != nil {
		return x.Status
	}
	return nil
}

func (x *RuntimeEvent) GetGroups() *Groups {
	if x != nil {
		return x.Groups
	}
	return nil
}

func (x *RuntimeEvent) GetClashMode() *ClashModeStatus {
	if x != nil {
		return x.ClashMode
	}
	return nil
}

func (x *RuntimeEvent) GetUrlTestSessions() []*URLTestSession {
	if x != nil {
		return x.UrlTestSessions
	}
	return nil
}

func (x *RuntimeEvent) GetStartedAt() int64 {
	if x != nil {
		return x.StartedAt
	}
	return 0
}

type RuntimeEvents struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Sequence      uint64                 `protobuf:"varint,1,opt,name=sequence,proto3" json:"sequence,omitempty"`
	Reset_        bool                   `protobuf:"varint,2,opt,name=reset,proto3" json:"reset,omitempty"`
	Snapshot      *RuntimeSnapshot       `protobuf:"bytes,3,opt,name=snapshot,proto3" json:"snapshot,omitempty"`
	Events        []*RuntimeEvent        `protobuf:"bytes,4,rep,name=events,proto3" json:"events,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RuntimeEvents) Reset() {
	*x = RuntimeEvents{}
	mi := &file_daemon_started_service_proto_msgTypes[18]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RuntimeEvents) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RuntimeEvents) ProtoMessage() {}

func (x *RuntimeEvents) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[18]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RuntimeEvents.ProtoReflect.Descriptor instead.
func (*RuntimeEvents) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{18}
}

func (x *RuntimeEvents) GetSequence() uint64 {
	if x != nil {
		return x.Sequence
	}
	return 0
}

func (x *RuntimeEvents) GetReset_() bool {
	if x != nil {
		return x.Reset_
	}
	return false
}

func (x *RuntimeEvents) GetSnapshot() *RuntimeSnapshot {
	if x != nil {
		return x.Snapshot
	}
	return nil
}

func (x *RuntimeEvents) GetEvents() []*RuntimeEvent {
	if x != nil {
		return x.Events
	}
	return nil
}

type OutboundExternalInfoRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	OutboundTag   string                 `protobuf:"bytes,1,opt,name=outboundTag,proto3" json:"outboundTag,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *OutboundExternalInfoRequest) Reset() {
	*x = OutboundExternalInfoRequest{}
	mi := &file_daemon_started_service_proto_msgTypes[19]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *OutboundExternalInfoRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OutboundExternalInfoRequest) ProtoMessage() {}

func (x *OutboundExternalInfoRequest) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[19]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OutboundExternalInfoRequest.ProtoReflect.Descriptor instead.
func (*OutboundExternalInfoRequest) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{19}
}

func (x *OutboundExternalInfoRequest) GetOutboundTag() string {
	if x != nil {
		return x.OutboundTag
	}
	return ""
}

type OutboundExternalInfoResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Ip            string                 `protobuf:"bytes,1,opt,name=ip,proto3" json:"ip,omitempty"`
	CountryCode   string                 `protobuf:"bytes,2,opt,name=countryCode,proto3" json:"countryCode,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *OutboundExternalInfoResponse) Reset() {
	*x = OutboundExternalInfoResponse{}
	mi := &file_daemon_started_service_proto_msgTypes[20]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *OutboundExternalInfoResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OutboundExternalInfoResponse) ProtoMessage() {}

func (x *OutboundExternalInfoResponse) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[20]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OutboundExternalInfoResponse.ProtoReflect.Descriptor instead.
func (*OutboundExternalInfoResponse) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{20}
}

func (x *OutboundExternalInfoResponse) GetIp() string {
	if x != nil {
		return x.Ip
	}
	return ""
}

func (x *OutboundExternalInfoResponse) GetCountryCode() string {
	if x != nil {
		return x.CountryCode
	}
	return ""
}

type SelectOutboundRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	GroupTag      string                 `protobuf:"bytes,1,opt,name=groupTag,proto3" json:"groupTag,omitempty"`
	OutboundTag   string                 `protobuf:"bytes,2,opt,name=outboundTag,proto3" json:"outboundTag,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SelectOutboundRequest) Reset() {
	*x = SelectOutboundRequest{}
	mi := &file_daemon_started_service_proto_msgTypes[21]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SelectOutboundRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SelectOutboundRequest) ProtoMessage() {}

func (x *SelectOutboundRequest) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[21]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SelectOutboundRequest.ProtoReflect.Descriptor instead.
func (*SelectOutboundRequest) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{21}
}

func (x *SelectOutboundRequest) GetGroupTag() string {
	if x != nil {
		return x.GroupTag
	}
	return ""
}

func (x *SelectOutboundRequest) GetOutboundTag() string {
	if x != nil {
		return x.OutboundTag
	}
	return ""
}

type SetGroupExpandRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	GroupTag      string                 `protobuf:"bytes,1,opt,name=groupTag,proto3" json:"groupTag,omitempty"`
	IsExpand      bool                   `protobuf:"varint,2,opt,name=isExpand,proto3" json:"isExpand,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SetGroupExpandRequest) Reset() {
	*x = SetGroupExpandRequest{}
	mi := &file_daemon_started_service_proto_msgTypes[22]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SetGroupExpandRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SetGroupExpandRequest) ProtoMessage() {}

func (x *SetGroupExpandRequest) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[22]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SetGroupExpandRequest.ProtoReflect.Descriptor instead.
func (*SetGroupExpandRequest) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{22}
}

func (x *SetGroupExpandRequest) GetGroupTag() string {
	if x != nil {
		return x.GroupTag
	}
	return ""
}

func (x *SetGroupExpandRequest) GetIsExpand() bool {
	if x != nil {
		return x.IsExpand
	}
	return false
}

type ClashMode struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Mode          string                 `protobuf:"bytes,3,opt,name=mode,proto3" json:"mode,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ClashMode) Reset() {
	*x = ClashMode{}
	mi := &file_daemon_started_service_proto_msgTypes[23]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ClashMode) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ClashMode) ProtoMessage() {}

func (x *ClashMode) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[23]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ClashMode.ProtoReflect.Descriptor instead.
func (*ClashMode) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{23}
}

func (x *ClashMode) GetMode() string {
	if x != nil {
		return x.Mode
	}
	return ""
}

type ClashModeStatus struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	ModeList      []string               `protobuf:"bytes,1,rep,name=modeList,proto3" json:"modeList,omitempty"`
	CurrentMode   string                 `protobuf:"bytes,2,opt,name=currentMode,proto3" json:"currentMode,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ClashModeStatus) Reset() {
	*x = ClashModeStatus{}
	mi := &file_daemon_started_service_proto_msgTypes[24]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ClashModeStatus) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ClashModeStatus) ProtoMessage() {}

func (x *ClashModeStatus) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[24]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ClashModeStatus.ProtoReflect.Descriptor instead.
func (*ClashModeStatus) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{24}
}

func (x *ClashModeStatus) GetModeList() []string {
	if x != nil {
		return x.ModeList
	}
	return nil
}

func (x *ClashModeStatus) GetCurrentMode() string {
	if x != nil {
		return x.CurrentMode
	}
	return ""
}

type SystemProxyStatus struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Available     bool                   `protobuf:"varint,1,opt,name=available,proto3" json:"available,omitempty"`
	Enabled       bool                   `protobuf:"varint,2,opt,name=enabled,proto3" json:"enabled,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SystemProxyStatus) Reset() {
	*x = SystemProxyStatus{}
	mi := &file_daemon_started_service_proto_msgTypes[25]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SystemProxyStatus) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SystemProxyStatus) ProtoMessage() {}

func (x *SystemProxyStatus) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[25]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SystemProxyStatus.ProtoReflect.Descriptor instead.
func (*SystemProxyStatus) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{25}
}

func (x *SystemProxyStatus) GetAvailable() bool {
	if x != nil {
		return x.Available
	}
	return false
}

func (x *SystemProxyStatus) GetEnabled() bool {
	if x != nil {
		return x.Enabled
	}
	return false
}

type SetSystemProxyEnabledRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Enabled       bool                   `protobuf:"varint,1,opt,name=enabled,proto3" json:"enabled,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SetSystemProxyEnabledRequest) Reset() {
	*x = SetSystemProxyEnabledRequest{}
	mi := &file_daemon_started_service_proto_msgTypes[26]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SetSystemProxyEnabledRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SetSystemProxyEnabledRequest) ProtoMessage() {}

func (x *SetSystemProxyEnabledRequest) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[26]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SetSystemProxyEnabledRequest.ProtoReflect.Descriptor instead.
func (*SetSystemProxyEnabledRequest) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{26}
}

func (x *SetSystemProxyEnabledRequest) GetEnabled() bool {
	if x != nil {
		return x.Enabled
	}
	return false
}

type SubscribeConnectionsRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Interval      int64                  `protobuf:"varint,1,opt,name=interval,proto3" json:"interval,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SubscribeConnectionsRequest) Reset() {
	*x = SubscribeConnectionsRequest{}
	mi := &file_daemon_started_service_proto_msgTypes[27]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SubscribeConnectionsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SubscribeConnectionsRequest) ProtoMessage() {}

func (x *SubscribeConnectionsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[27]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SubscribeConnectionsRequest.ProtoReflect.Descriptor instead.
func (*SubscribeConnectionsRequest) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{27}
}

func (x *SubscribeConnectionsRequest) GetInterval() int64 {
	if x != nil {
		return x.Interval
	}
	return 0
}

type ConnectionEvent struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Type          ConnectionEventType    `protobuf:"varint,1,opt,name=type,proto3,enum=daemon.ConnectionEventType" json:"type,omitempty"`
	Id            string                 `protobuf:"bytes,2,opt,name=id,proto3" json:"id,omitempty"`
	Connection    *Connection            `protobuf:"bytes,3,opt,name=connection,proto3" json:"connection,omitempty"`
	UplinkDelta   int64                  `protobuf:"varint,4,opt,name=uplinkDelta,proto3" json:"uplinkDelta,omitempty"`
	DownlinkDelta int64                  `protobuf:"varint,5,opt,name=downlinkDelta,proto3" json:"downlinkDelta,omitempty"`
	ClosedAt      int64                  `protobuf:"varint,6,opt,name=closedAt,proto3" json:"closedAt,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ConnectionEvent) Reset() {
	*x = ConnectionEvent{}
	mi := &file_daemon_started_service_proto_msgTypes[28]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ConnectionEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConnectionEvent) ProtoMessage() {}

func (x *ConnectionEvent) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[28]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ConnectionEvent.ProtoReflect.Descriptor instead.
func (*ConnectionEvent) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{28}
}

func (x *ConnectionEvent) GetType() ConnectionEventType {
	if x != nil {
		return x.Type
	}
	return ConnectionEventType_CONNECTION_EVENT_NEW
}

func (x *ConnectionEvent) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *ConnectionEvent) GetConnection() *Connection {
	if x != nil {
		return x.Connection
	}
	return nil
}

func (x *ConnectionEvent) GetUplinkDelta() int64 {
	if x != nil {
		return x.UplinkDelta
	}
	return 0
}

func (x *ConnectionEvent) GetDownlinkDelta() int64 {
	if x != nil {
		return x.DownlinkDelta
	}
	return 0
}

func (x *ConnectionEvent) GetClosedAt() int64 {
	if x != nil {
		return x.ClosedAt
	}
	return 0
}

type ConnectionEvents struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Events        []*ConnectionEvent     `protobuf:"bytes,1,rep,name=events,proto3" json:"events,omitempty"`
	Reset_        bool                   `protobuf:"varint,2,opt,name=reset,proto3" json:"reset,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ConnectionEvents) Reset() {
	*x = ConnectionEvents{}
	mi := &file_daemon_started_service_proto_msgTypes[29]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ConnectionEvents) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConnectionEvents) ProtoMessage() {}

func (x *ConnectionEvents) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[29]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ConnectionEvents.ProtoReflect.Descriptor instead.
func (*ConnectionEvents) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{29}
}

func (x *ConnectionEvents) GetEvents() []*ConnectionEvent {
	if x != nil {
		return x.Events
	}
	return nil
}

func (x *ConnectionEvents) GetReset_() bool {
	if x != nil {
		return x.Reset_
	}
	return false
}

type Connection struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Inbound       string                 `protobuf:"bytes,2,opt,name=inbound,proto3" json:"inbound,omitempty"`
	InboundType   string                 `protobuf:"bytes,3,opt,name=inboundType,proto3" json:"inboundType,omitempty"`
	IpVersion     int32                  `protobuf:"varint,4,opt,name=ipVersion,proto3" json:"ipVersion,omitempty"`
	Network       string                 `protobuf:"bytes,5,opt,name=network,proto3" json:"network,omitempty"`
	Source        string                 `protobuf:"bytes,6,opt,name=source,proto3" json:"source,omitempty"`
	Destination   string                 `protobuf:"bytes,7,opt,name=destination,proto3" json:"destination,omitempty"`
	Domain        string                 `protobuf:"bytes,8,opt,name=domain,proto3" json:"domain,omitempty"`
	Protocol      string                 `protobuf:"bytes,9,opt,name=protocol,proto3" json:"protocol,omitempty"`
	User          string                 `protobuf:"bytes,10,opt,name=user,proto3" json:"user,omitempty"`
	FromOutbound  string                 `protobuf:"bytes,11,opt,name=fromOutbound,proto3" json:"fromOutbound,omitempty"`
	CreatedAt     int64                  `protobuf:"varint,12,opt,name=createdAt,proto3" json:"createdAt,omitempty"`
	ClosedAt      int64                  `protobuf:"varint,13,opt,name=closedAt,proto3" json:"closedAt,omitempty"`
	Uplink        int64                  `protobuf:"varint,14,opt,name=uplink,proto3" json:"uplink,omitempty"`
	Downlink      int64                  `protobuf:"varint,15,opt,name=downlink,proto3" json:"downlink,omitempty"`
	UplinkTotal   int64                  `protobuf:"varint,16,opt,name=uplinkTotal,proto3" json:"uplinkTotal,omitempty"`
	DownlinkTotal int64                  `protobuf:"varint,17,opt,name=downlinkTotal,proto3" json:"downlinkTotal,omitempty"`
	Rule          string                 `protobuf:"bytes,18,opt,name=rule,proto3" json:"rule,omitempty"`
	Outbound      string                 `protobuf:"bytes,19,opt,name=outbound,proto3" json:"outbound,omitempty"`
	OutboundType  string                 `protobuf:"bytes,20,opt,name=outboundType,proto3" json:"outboundType,omitempty"`
	ChainList     []string               `protobuf:"bytes,21,rep,name=chainList,proto3" json:"chainList,omitempty"`
	ProcessInfo   *ProcessInfo           `protobuf:"bytes,22,opt,name=processInfo,proto3" json:"processInfo,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Connection) Reset() {
	*x = Connection{}
	mi := &file_daemon_started_service_proto_msgTypes[30]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Connection) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Connection) ProtoMessage() {}

func (x *Connection) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[30]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Connection.ProtoReflect.Descriptor instead.
func (*Connection) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{30}
}

func (x *Connection) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Connection) GetInbound() string {
	if x != nil {
		return x.Inbound
	}
	return ""
}

func (x *Connection) GetInboundType() string {
	if x != nil {
		return x.InboundType
	}
	return ""
}

func (x *Connection) GetIpVersion() int32 {
	if x != nil {
		return x.IpVersion
	}
	return 0
}

func (x *Connection) GetNetwork() string {
	if x != nil {
		return x.Network
	}
	return ""
}

func (x *Connection) GetSource() string {
	if x != nil {
		return x.Source
	}
	return ""
}

func (x *Connection) GetDestination() string {
	if x != nil {
		return x.Destination
	}
	return ""
}

func (x *Connection) GetDomain() string {
	if x != nil {
		return x.Domain
	}
	return ""
}

func (x *Connection) GetProtocol() string {
	if x != nil {
		return x.Protocol
	}
	return ""
}

func (x *Connection) GetUser() string {
	if x != nil {
		return x.User
	}
	return ""
}

func (x *Connection) GetFromOutbound() string {
	if x != nil {
		return x.FromOutbound
	}
	return ""
}

func (x *Connection) GetCreatedAt() int64 {
	if x != nil {
		return x.CreatedAt
	}
	return 0
}

func (x *Connection) GetClosedAt() int64 {
	if x != nil {
		return x.ClosedAt
	}
	return 0
}

func (x *Connection) GetUplink() int64 {
	if x != nil {
		return x.Uplink
	}
	return 0
}

func (x *Connection) GetDownlink() int64 {
	if x != nil {
		return x.Downlink
	}
	return 0
}

func (x *Connection) GetUplinkTotal() int64 {
	if x != nil {
		return x.UplinkTotal
	}
	return 0
}

func (x *Connection) GetDownlinkTotal() int64 {
	if x != nil {
		return x.DownlinkTotal
	}
	return 0
}

func (x *Connection) GetRule() string {
	if x != nil {
		return x.Rule
	}
	return ""
}

func (x *Connection) GetOutbound() string {
	if x != nil {
		return x.Outbound
	}
	return ""
}

func (x *Connection) GetOutboundType() string {
	if x != nil {
		return x.OutboundType
	}
	return ""
}

func (x *Connection) GetChainList() []string {
	if x != nil {
		return x.ChainList
	}
	return nil
}

func (x *Connection) GetProcessInfo() *ProcessInfo {
	if x != nil {
		return x.ProcessInfo
	}
	return nil
}

type ProcessInfo struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	ProcessId     uint32                 `protobuf:"varint,1,opt,name=processId,proto3" json:"processId,omitempty"`
	UserId        int32                  `protobuf:"varint,2,opt,name=userId,proto3" json:"userId,omitempty"`
	UserName      string                 `protobuf:"bytes,3,opt,name=userName,proto3" json:"userName,omitempty"`
	ProcessPath   string                 `protobuf:"bytes,4,opt,name=processPath,proto3" json:"processPath,omitempty"`
	PackageNames  []string               `protobuf:"bytes,5,rep,name=packageNames,proto3" json:"packageNames,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ProcessInfo) Reset() {
	*x = ProcessInfo{}
	mi := &file_daemon_started_service_proto_msgTypes[31]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ProcessInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProcessInfo) ProtoMessage() {}

func (x *ProcessInfo) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[31]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProcessInfo.ProtoReflect.Descriptor instead.
func (*ProcessInfo) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{31}
}

func (x *ProcessInfo) GetProcessId() uint32 {
	if x != nil {
		return x.ProcessId
	}
	return 0
}

func (x *ProcessInfo) GetUserId() int32 {
	if x != nil {
		return x.UserId
	}
	return 0
}

func (x *ProcessInfo) GetUserName() string {
	if x != nil {
		return x.UserName
	}
	return ""
}

func (x *ProcessInfo) GetProcessPath() string {
	if x != nil {
		return x.ProcessPath
	}
	return ""
}

func (x *ProcessInfo) GetPackageNames() []string {
	if x != nil {
		return x.PackageNames
	}
	return nil
}

type CloseConnectionRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CloseConnectionRequest) Reset() {
	*x = CloseConnectionRequest{}
	mi := &file_daemon_started_service_proto_msgTypes[32]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CloseConnectionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CloseConnectionRequest) ProtoMessage() {}

func (x *CloseConnectionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[32]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CloseConnectionRequest.ProtoReflect.Descriptor instead.
func (*CloseConnectionRequest) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{32}
}

func (x *CloseConnectionRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

type DeprecatedWarnings struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Warnings      []*DeprecatedWarning   `protobuf:"bytes,1,rep,name=warnings,proto3" json:"warnings,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *DeprecatedWarnings) Reset() {
	*x = DeprecatedWarnings{}
	mi := &file_daemon_started_service_proto_msgTypes[33]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DeprecatedWarnings) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeprecatedWarnings) ProtoMessage() {}

func (x *DeprecatedWarnings) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[33]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeprecatedWarnings.ProtoReflect.Descriptor instead.
func (*DeprecatedWarnings) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{33}
}

func (x *DeprecatedWarnings) GetWarnings() []*DeprecatedWarning {
	if x != nil {
		return x.Warnings
	}
	return nil
}

type DeprecatedWarning struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Message       string                 `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
	Impending     bool                   `protobuf:"varint,2,opt,name=impending,proto3" json:"impending,omitempty"`
	MigrationLink string                 `protobuf:"bytes,3,opt,name=migrationLink,proto3" json:"migrationLink,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *DeprecatedWarning) Reset() {
	*x = DeprecatedWarning{}
	mi := &file_daemon_started_service_proto_msgTypes[34]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DeprecatedWarning) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeprecatedWarning) ProtoMessage() {}

func (x *DeprecatedWarning) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[34]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeprecatedWarning.ProtoReflect.Descriptor instead.
func (*DeprecatedWarning) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{34}
}

func (x *DeprecatedWarning) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

func (x *DeprecatedWarning) GetImpending() bool {
	if x != nil {
		return x.Impending
	}
	return false
}

func (x *DeprecatedWarning) GetMigrationLink() string {
	if x != nil {
		return x.MigrationLink
	}
	return ""
}

type StartedAt struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	StartedAt     int64                  `protobuf:"varint,1,opt,name=startedAt,proto3" json:"startedAt,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *StartedAt) Reset() {
	*x = StartedAt{}
	mi := &file_daemon_started_service_proto_msgTypes[35]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StartedAt) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StartedAt) ProtoMessage() {}

func (x *StartedAt) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[35]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StartedAt.ProtoReflect.Descriptor instead.
func (*StartedAt) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{35}
}

func (x *StartedAt) GetStartedAt() int64 {
	if x != nil {
		return x.StartedAt
	}
	return 0
}

type Log_Message struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Level         LogLevel               `protobuf:"varint,1,opt,name=level,proto3,enum=daemon.LogLevel" json:"level,omitempty"`
	Message       string                 `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Log_Message) Reset() {
	*x = Log_Message{}
	mi := &file_daemon_started_service_proto_msgTypes[36]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Log_Message) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Log_Message) ProtoMessage() {}

func (x *Log_Message) ProtoReflect() protoreflect.Message {
	mi := &file_daemon_started_service_proto_msgTypes[36]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Log_Message.ProtoReflect.Descriptor instead.
func (*Log_Message) Descriptor() ([]byte, []int) {
	return file_daemon_started_service_proto_rawDescGZIP(), []int{3, 0}
}

func (x *Log_Message) GetLevel() LogLevel {
	if x != nil {
		return x.Level
	}
	return LogLevel_PANIC
}

func (x *Log_Message) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

var File_daemon_started_service_proto protoreflect.FileDescriptor

const file_daemon_started_service_proto_rawDesc = "" +
	"\n" +
	"\x1cdaemon/started_service.proto\x12\x06daemon\x1a\x1bgoogle/protobuf/empty.proto\"\xad\x01\n" +
	"\rServiceStatus\x122\n" +
	"\x06status\x18\x01 \x01(\x0e2\x1a.daemon.ServiceStatus.TypeR\x06status\x12\"\n" +
	"\ferrorMessage\x18\x02 \x01(\tR\ferrorMessage\"D\n" +
	"\x04Type\x12\b\n" +
	"\x04IDLE\x10\x00\x12\f\n" +
	"\bSTARTING\x10\x01\x12\v\n" +
	"\aSTARTED\x10\x02\x12\f\n" +
	"\bSTOPPING\x10\x03\x12\t\n" +
	"\x05FATAL\x10\x04\"D\n" +
	"\x14ReloadServiceRequest\x12,\n" +
	"\x11newProfileContent\x18\x01 \x01(\tR\x11newProfileContent\"4\n" +
	"\x16SubscribeStatusRequest\x12\x1a\n" +
	"\binterval\x18\x01 \x01(\x03R\binterval\"\x99\x01\n" +
	"\x03Log\x12/\n" +
	"\bmessages\x18\x01 \x03(\v2\x13.daemon.Log.MessageR\bmessages\x12\x14\n" +
	"\x05reset\x18\x02 \x01(\bR\x05reset\x1aK\n" +
	"\aMessage\x12&\n" +
	"\x05level\x18\x01 \x01(\x0e2\x10.daemon.LogLevelR\x05level\x12\x18\n" +
	"\amessage\x18\x02 \x01(\tR\amessage\"9\n" +
	"\x0fDefaultLogLevel\x12&\n" +
	"\x05level\x18\x01 \x01(\x0e2\x10.daemon.LogLevelR\x05level\"\xb6\x02\n" +
	"\x06Status\x12\x16\n" +
	"\x06memory\x18\x01 \x01(\x04R\x06memory\x12\x1e\n" +
	"\n" +
	"goroutines\x18\x02 \x01(\x05R\n" +
	"goroutines\x12$\n" +
	"\rconnectionsIn\x18\x03 \x01(\x05R\rconnectionsIn\x12&\n" +
	"\x0econnectionsOut\x18\x04 \x01(\x05R\x0econnectionsOut\x12*\n" +
	"\x10trafficAvailable\x18\x05 \x01(\bR\x10trafficAvailable\x12\x16\n" +
	"\x06uplink\x18\x06 \x01(\x03R\x06uplink\x12\x1a\n" +
	"\bdownlink\x18\a \x01(\x03R\bdownlink\x12 \n" +
	"\vuplinkTotal\x18\b \x01(\x03R\vuplinkTotal\x12$\n" +
	"\rdownlinkTotal\x18\t \x01(\x03R\rdownlinkTotal\"-\n" +
	"\x06Groups\x12#\n" +
	"\x05group\x18\x01 \x03(\v2\r.daemon.GroupR\x05group\"\xae\x01\n" +
	"\x05Group\x12\x10\n" +
	"\x03tag\x18\x01 \x01(\tR\x03tag\x12\x12\n" +
	"\x04type\x18\x02 \x01(\tR\x04type\x12\x1e\n" +
	"\n" +
	"selectable\x18\x03 \x01(\bR\n" +
	"selectable\x12\x1a\n" +
	"\bselected\x18\x04 \x01(\tR\bselected\x12\x1a\n" +
	"\bisExpand\x18\x05 \x01(\bR\bisExpand\x12'\n" +
	"\x05items\x18\x06 \x03(\v2\x11.daemon.GroupItemR\x05items\"\xed\x01\n" +
	"\tGroupItem\x12\x10\n" +
	"\x03tag\x18\x01 \x01(\tR\x03tag\x12\x12\n" +
	"\x04type\x18\x02 \x01(\tR\x04type\x12 \n" +
	"\vurlTestTime\x18\x03 \x01(\x03R\vurlTestTime\x12\"\n" +
	"\furlTestDelay\x18\x04 \x01(\x05R\furlTestDelay\x12$\n" +
	"\rurlTestStatus\x18\x05 \x01(\tR\rurlTestStatus\x12\"\n" +
	"\furlTestError\x18\x06 \x01(\tR\furlTestError\x12*\n" +
	"\x10urlTestErrorCode\x18\a \x01(\tR\x10urlTestErrorCode\"\xe2\x02\n" +
	"\x0eURLTestRequest\x12\x1a\n" +
	"\bgroupTag\x18\x01 \x01(\tR\bgroupTag\x12\x1e\n" +
	"\n" +
	"urlTestUrl\x18\x02 \x01(\tR\n" +
	"urlTestUrl\x12,\n" +
	"\x11targetOutboundTag\x18\x03 \x01(\tR\x11targetOutboundTag\x120\n" +
	"\x13priorityOutboundTag\x18\x04 \x01(\tR\x13priorityOutboundTag\x12.\n" +
	"\x12excludeOutboundTag\x18\x05 \x01(\tR\x12excludeOutboundTag\x12$\n" +
	"\rtimeoutMillis\x18\x06 \x01(\x05R\rtimeoutMillis\x12 \n" +
	"\vconcurrency\x18\a \x01(\x05R\vconcurrency\x12&\n" +
	"\x0edeadlineMillis\x18\b \x01(\x05R\x0edeadlineMillis\x12\x14\n" +
	"\x05force\x18\t \x01(\bR\x05force\"\xcd\x01\n" +
	"\rURLTestResult\x12 \n" +
	"\voutboundTag\x18\x01 \x01(\tR\voutboundTag\x12 \n" +
	"\vdelayMillis\x18\x02 \x01(\x03R\vdelayMillis\x12\x1e\n" +
	"\n" +
	"observedAt\x18\x03 \x01(\x03R\n" +
	"observedAt\x12\x16\n" +
	"\x06status\x18\x04 \x01(\tR\x06status\x12\x1c\n" +
	"\terrorCode\x18\x05 \x01(\tR\terrorCode\x12\"\n" +
	"\ferrorMessage\x18\x06 \x01(\tR\ferrorMessage\"\x8c\x03\n" +
	"\x0eURLTestSession\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12\x1a\n" +
	"\bgroupTag\x18\x02 \x01(\tR\bgroupTag\x121\n" +
	"\x05state\x18\x03 \x01(\x0e2\x1b.daemon.URLTestSessionStateR\x05state\x12\x1c\n" +
	"\tstartedAt\x18\x04 \x01(\x03R\tstartedAt\x12 \n" +
	"\vcompletedAt\x18\x05 \x01(\x03R\vcompletedAt\x12\x14\n" +
	"\x05total\x18\x06 \x01(\x05R\x05total\x12\x1c\n" +
	"\tcompleted\x18\a \x01(\x05R\tcompleted\x12\x1c\n" +
	"\tsucceeded\x18\b \x01(\x05R\tsucceeded\x12\x16\n" +
	"\x06failed\x18\t \x01(\x05R\x06failed\x12/\n" +
	"\aresults\x18\n" +
	" \x03(\v2\x15.daemon.URLTestResultR\aresults\x12\x1c\n" +
	"\terrorCode\x18\v \x01(\tR\terrorCode\x12\"\n" +
	"\ferrorMessage\x18\f \x01(\tR\ferrorMessage\"'\n" +
	"\x15URLTestSessionRequest\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\"=\n" +
	"\x13URLTestEventRequest\x12&\n" +
	"\x0eintervalMillis\x18\x01 \x01(\x03R\x0eintervalMillis\"u\n" +
	"\rURLTestEvents\x12\x1a\n" +
	"\bsequence\x18\x01 \x01(\x04R\bsequence\x12\x14\n" +
	"\x05reset\x18\x02 \x01(\bR\x05reset\x122\n" +
	"\bsessions\x18\x03 \x03(\v2\x16.daemon.URLTestSessionR\bsessions\"\x8b\x03\n" +
	"\x0fRuntimeSnapshot\x12$\n" +
	"\rschemaVersion\x18\x01 \x01(\x05R\rschemaVersion\x12\x1a\n" +
	"\bsequence\x18\x02 \x01(\x04R\bsequence\x12\x1e\n" +
	"\n" +
	"observedAt\x18\x03 \x01(\x03R\n" +
	"observedAt\x12/\n" +
	"\aservice\x18\x04 \x01(\v2\x15.daemon.ServiceStatusR\aservice\x12\x1c\n" +
	"\tstartedAt\x18\x05 \x01(\x03R\tstartedAt\x12&\n" +
	"\x06status\x18\x06 \x01(\v2\x0e.daemon.StatusR\x06status\x12&\n" +
	"\x06groups\x18\a \x01(\v2\x0e.daemon.GroupsR\x06groups\x125\n" +
	"\tclashMode\x18\b \x01(\v2\x17.daemon.ClashModeStatusR\tclashMode\x12@\n" +
	"\x0furlTestSessions\x18\t \x03(\v2\x16.daemon.URLTestSessionR\x0furlTestSessions\"=\n" +
	"\x13RuntimeEventRequest\x12&\n" +
	"\x0eintervalMillis\x18\x01 \x01(\x03R\x0eintervalMillis\"\xd4\x02\n" +
	"\fRuntimeEvent\x12,\n" +
	"\x04type\x18\x01 \x01(\x0e2\x18.daemon.RuntimeEventTypeR\x04type\x12/\n" +
	"\aservice\x18\x02 \x01(\v2\x15.daemon.ServiceStatusR\aservice\x12&\n" +
	"\x06status\x18\x03 \x01(\v2\x0e.daemon.StatusR\x06status\x12&\n" +
	"\x06groups\x18\x04 \x01(\v2\x0e.daemon.GroupsR\x06groups\x125\n" +
	"\tclashMode\x18\x05 \x01(\v2\x17.daemon.ClashModeStatusR\tclashMode\x12@\n" +
	"\x0furlTestSessions\x18\x06 \x03(\v2\x16.daemon.URLTestSessionR\x0furlTestSessions\x12\x1c\n" +
	"\tstartedAt\x18\a \x01(\x03R\tstartedAt\"\xa4\x01\n" +
	"\rRuntimeEvents\x12\x1a\n" +
	"\bsequence\x18\x01 \x01(\x04R\bsequence\x12\x14\n" +
	"\x05reset\x18\x02 \x01(\bR\x05reset\x123\n" +
	"\bsnapshot\x18\x03 \x01(\v2\x17.daemon.RuntimeSnapshotR\bsnapshot\x12,\n" +
	"\x06events\x18\x04 \x03(\v2\x14.daemon.RuntimeEventR\x06events\"?\n" +
	"\x1bOutboundExternalInfoRequest\x12 \n" +
	"\voutboundTag\x18\x01 \x01(\tR\voutboundTag\"P\n" +
	"\x1cOutboundExternalInfoResponse\x12\x0e\n" +
	"\x02ip\x18\x01 \x01(\tR\x02ip\x12 \n" +
	"\vcountryCode\x18\x02 \x01(\tR\vcountryCode\"U\n" +
	"\x15SelectOutboundRequest\x12\x1a\n" +
	"\bgroupTag\x18\x01 \x01(\tR\bgroupTag\x12 \n" +
	"\voutboundTag\x18\x02 \x01(\tR\voutboundTag\"O\n" +
	"\x15SetGroupExpandRequest\x12\x1a\n" +
	"\bgroupTag\x18\x01 \x01(\tR\bgroupTag\x12\x1a\n" +
	"\bisExpand\x18\x02 \x01(\bR\bisExpand\"\x1f\n" +
	"\tClashMode\x12\x12\n" +
	"\x04mode\x18\x03 \x01(\tR\x04mode\"O\n" +
	"\x0fClashModeStatus\x12\x1a\n" +
	"\bmodeList\x18\x01 \x03(\tR\bmodeList\x12 \n" +
	"\vcurrentMode\x18\x02 \x01(\tR\vcurrentMode\"K\n" +
	"\x11SystemProxyStatus\x12\x1c\n" +
	"\tavailable\x18\x01 \x01(\bR\tavailable\x12\x18\n" +
	"\aenabled\x18\x02 \x01(\bR\aenabled\"8\n" +
	"\x1cSetSystemProxyEnabledRequest\x12\x18\n" +
	"\aenabled\x18\x01 \x01(\bR\aenabled\"9\n" +
	"\x1bSubscribeConnectionsRequest\x12\x1a\n" +
	"\binterval\x18\x01 \x01(\x03R\binterval\"\xea\x01\n" +
	"\x0fConnectionEvent\x12/\n" +
	"\x04type\x18\x01 \x01(\x0e2\x1b.daemon.ConnectionEventTypeR\x04type\x12\x0e\n" +
	"\x02id\x18\x02 \x01(\tR\x02id\x122\n" +
	"\n" +
	"connection\x18\x03 \x01(\v2\x12.daemon.ConnectionR\n" +
	"connection\x12 \n" +
	"\vuplinkDelta\x18\x04 \x01(\x03R\vuplinkDelta\x12$\n" +
	"\rdownlinkDelta\x18\x05 \x01(\x03R\rdownlinkDelta\x12\x1a\n" +
	"\bclosedAt\x18\x06 \x01(\x03R\bclosedAt\"Y\n" +
	"\x10ConnectionEvents\x12/\n" +
	"\x06events\x18\x01 \x03(\v2\x17.daemon.ConnectionEventR\x06events\x12\x14\n" +
	"\x05reset\x18\x02 \x01(\bR\x05reset\"\x95\x05\n" +
	"\n" +
	"Connection\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12\x18\n" +
	"\ainbound\x18\x02 \x01(\tR\ainbound\x12 \n" +
	"\vinboundType\x18\x03 \x01(\tR\vinboundType\x12\x1c\n" +
	"\tipVersion\x18\x04 \x01(\x05R\tipVersion\x12\x18\n" +
	"\anetwork\x18\x05 \x01(\tR\anetwork\x12\x16\n" +
	"\x06source\x18\x06 \x01(\tR\x06source\x12 \n" +
	"\vdestination\x18\a \x01(\tR\vdestination\x12\x16\n" +
	"\x06domain\x18\b \x01(\tR\x06domain\x12\x1a\n" +
	"\bprotocol\x18\t \x01(\tR\bprotocol\x12\x12\n" +
	"\x04user\x18\n" +
	" \x01(\tR\x04user\x12\"\n" +
	"\ffromOutbound\x18\v \x01(\tR\ffromOutbound\x12\x1c\n" +
	"\tcreatedAt\x18\f \x01(\x03R\tcreatedAt\x12\x1a\n" +
	"\bclosedAt\x18\r \x01(\x03R\bclosedAt\x12\x16\n" +
	"\x06uplink\x18\x0e \x01(\x03R\x06uplink\x12\x1a\n" +
	"\bdownlink\x18\x0f \x01(\x03R\bdownlink\x12 \n" +
	"\vuplinkTotal\x18\x10 \x01(\x03R\vuplinkTotal\x12$\n" +
	"\rdownlinkTotal\x18\x11 \x01(\x03R\rdownlinkTotal\x12\x12\n" +
	"\x04rule\x18\x12 \x01(\tR\x04rule\x12\x1a\n" +
	"\boutbound\x18\x13 \x01(\tR\boutbound\x12\"\n" +
	"\foutboundType\x18\x14 \x01(\tR\foutboundType\x12\x1c\n" +
	"\tchainList\x18\x15 \x03(\tR\tchainList\x125\n" +
	"\vprocessInfo\x18\x16 \x01(\v2\x13.daemon.ProcessInfoR\vprocessInfo\"\xa5\x01\n" +
	"\vProcessInfo\x12\x1c\n" +
	"\tprocessId\x18\x01 \x01(\rR\tprocessId\x12\x16\n" +
	"\x06userId\x18\x02 \x01(\x05R\x06userId\x12\x1a\n" +
	"\buserName\x18\x03 \x01(\tR\buserName\x12 \n" +
	"\vprocessPath\x18\x04 \x01(\tR\vprocessPath\x12\"\n" +
	"\fpackageNames\x18\x05 \x03(\tR\fpackageNames\"(\n" +
	"\x16CloseConnectionRequest\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\"K\n" +
	"\x12DeprecatedWarnings\x125\n" +
	"\bwarnings\x18\x01 \x03(\v2\x19.daemon.DeprecatedWarningR\bwarnings\"q\n" +
	"\x11DeprecatedWarning\x12\x18\n" +
	"\amessage\x18\x01 \x01(\tR\amessage\x12\x1c\n" +
	"\timpending\x18\x02 \x01(\bR\timpending\x12$\n" +
	"\rmigrationLink\x18\x03 \x01(\tR\rmigrationLink\")\n" +
	"\tStartedAt\x12\x1c\n" +
	"\tstartedAt\x18\x01 \x01(\x03R\tstartedAt*a\n" +
	"\bLogLevel\x12\t\n" +
	"\x05PANIC\x10\x00\x12\t\n" +
	"\x05FATAL\x10\x01\x12\t\n" +
	"\x05ERROR\x10\x02\x12\b\n" +
	"\x04WARN\x10\x03\x12\n" +
	"\n" +
	"\x06NOTICE\x10\x04\x12\b\n" +
	"\x04INFO\x10\x05\x12\t\n" +
	"\x05DEBUG\x10\x06\x12\t\n" +
	"\x05TRACE\x10\a*\xad\x01\n" +
	"\x13URLTestSessionState\x12\x1b\n" +
	"\x17URL_TEST_SESSION_QUEUED\x10\x00\x12\x1c\n" +
	"\x18URL_TEST_SESSION_RUNNING\x10\x01\x12\x1e\n" +
	"\x1aURL_TEST_SESSION_SUCCEEDED\x10\x02\x12\x1b\n" +
	"\x17URL_TEST_SESSION_FAILED\x10\x03\x12\x1e\n" +
	"\x1aURL_TEST_SESSION_CANCELLED\x10\x04*\xa4\x01\n" +
	"\x10RuntimeEventType\x12\x19\n" +
	"\x15RUNTIME_EVENT_SERVICE\x10\x00\x12\x18\n" +
	"\x14RUNTIME_EVENT_STATUS\x10\x01\x12\x18\n" +
	"\x14RUNTIME_EVENT_GROUPS\x10\x02\x12\x1c\n" +
	"\x18RUNTIME_EVENT_CLASH_MODE\x10\x03\x12#\n" +
	"\x1fRUNTIME_EVENT_URL_TEST_SESSIONS\x10\x04*i\n" +
	"\x13ConnectionEventType\x12\x18\n" +
	"\x14CONNECTION_EVENT_NEW\x10\x00\x12\x1b\n" +
	"\x17CONNECTION_EVENT_UPDATE\x10\x01\x12\x1b\n" +
	"\x17CONNECTION_EVENT_CLOSED\x10\x022\xda\x0f\n" +
	"\x0eStartedService\x12=\n" +
	"\vStopService\x12\x16.google.protobuf.Empty\x1a\x16.google.protobuf.Empty\x12?\n" +
	"\rReloadService\x12\x16.google.protobuf.Empty\x1a\x16.google.protobuf.Empty\x12K\n" +
	"\x16SubscribeServiceStatus\x12\x16.google.protobuf.Empty\x1a\x15.daemon.ServiceStatus\"\x000\x01\x127\n" +
	"\fSubscribeLog\x12\x16.google.protobuf.Empty\x1a\v.daemon.Log\"\x000\x01\x12G\n" +
	"\x12GetDefaultLogLevel\x12\x16.google.protobuf.Empty\x1a\x17.daemon.DefaultLogLevel\"\x00\x12=\n" +
	"\tClearLogs\x12\x16.google.protobuf.Empty\x1a\x16.google.protobuf.Empty\"\x00\x12E\n" +
	"\x0fSubscribeStatus\x12\x1e.daemon.SubscribeStatusRequest\x1a\x0e.daemon.Status\"\x000\x01\x12=\n" +
	"\x0fSubscribeGroups\x12\x16.google.protobuf.Empty\x1a\x0e.daemon.Groups\"\x000\x01\x12G\n" +
	"\x12GetClashModeStatus\x12\x16.google.protobuf.Empty\x1a\x17.daemon.ClashModeStatus\"\x00\x12C\n" +
	"\x12SubscribeClashMode\x12\x16.google.protobuf.Empty\x1a\x11.daemon.ClashMode\"\x000\x01\x12;\n" +
	"\fSetClashMode\x12\x11.daemon.ClashMode\x1a\x16.google.protobuf.Empty\"\x00\x12G\n" +
	"\x12GetRuntimeSnapshot\x12\x16.google.protobuf.Empty\x1a\x17.daemon.RuntimeSnapshot\"\x00\x12P\n" +
	"\x16SubscribeRuntimeEvents\x12\x1b.daemon.RuntimeEventRequest\x1a\x15.daemon.RuntimeEvents\"\x000\x01\x12@\n" +
	"\fStartURLTest\x12\x16.daemon.URLTestRequest\x1a\x16.daemon.URLTestSession\"\x00\x12L\n" +
	"\x11GetURLTestSession\x12\x1d.daemon.URLTestSessionRequest\x1a\x16.daemon.URLTestSession\"\x00\x12H\n" +
	"\rCancelURLTest\x12\x1d.daemon.URLTestSessionRequest\x1a\x16.daemon.URLTestSession\"\x00\x12P\n" +
	"\x16SubscribeURLTestEvents\x12\x1b.daemon.URLTestEventRequest\x1a\x15.daemon.URLTestEvents\"\x000\x01\x12i\n" +
	"\x1aLookupOutboundExternalInfo\x12#.daemon.OutboundExternalInfoRequest\x1a$.daemon.OutboundExternalInfoResponse\"\x00\x12I\n" +
	"\x0eSelectOutbound\x12\x1d.daemon.SelectOutboundRequest\x1a\x16.google.protobuf.Empty\"\x00\x12I\n" +
	"\x0eSetGroupExpand\x12\x1d.daemon.SetGroupExpandRequest\x1a\x16.google.protobuf.Empty\"\x00\x12K\n" +
	"\x14GetSystemProxyStatus\x12\x16.google.protobuf.Empty\x1a\x19.daemon.SystemProxyStatus\"\x00\x12W\n" +
	"\x15SetSystemProxyEnabled\x12$.daemon.SetSystemProxyEnabledRequest\x1a\x16.google.protobuf.Empty\"\x00\x12Y\n" +
	"\x14SubscribeConnections\x12#.daemon.SubscribeConnectionsRequest\x1a\x18.daemon.ConnectionEvents\"\x000\x01\x12K\n" +
	"\x0fCloseConnection\x12\x1e.daemon.CloseConnectionRequest\x1a\x16.google.protobuf.Empty\"\x00\x12G\n" +
	"\x13CloseAllConnections\x12\x16.google.protobuf.Empty\x1a\x16.google.protobuf.Empty\"\x00\x12M\n" +
	"\x15GetDeprecatedWarnings\x12\x16.google.protobuf.Empty\x1a\x1a.daemon.DeprecatedWarnings\"\x00\x12;\n" +
	"\fGetStartedAt\x12\x16.google.protobuf.Empty\x1a\x11.daemon.StartedAt\"\x00B%Z#github.com/sagernet/sing-box/daemonb\x06proto3"

var (
	file_daemon_started_service_proto_rawDescOnce sync.Once
	file_daemon_started_service_proto_rawDescData []byte
)

func file_daemon_started_service_proto_rawDescGZIP() []byte {
	file_daemon_started_service_proto_rawDescOnce.Do(func() {
		file_daemon_started_service_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_daemon_started_service_proto_rawDesc), len(file_daemon_started_service_proto_rawDesc)))
	})
	return file_daemon_started_service_proto_rawDescData
}

var file_daemon_started_service_proto_enumTypes = make([]protoimpl.EnumInfo, 5)
var file_daemon_started_service_proto_msgTypes = make([]protoimpl.MessageInfo, 37)
var file_daemon_started_service_proto_goTypes = []any{
	(LogLevel)(0),                        // 0: daemon.LogLevel
	(URLTestSessionState)(0),             // 1: daemon.URLTestSessionState
	(RuntimeEventType)(0),                // 2: daemon.RuntimeEventType
	(ConnectionEventType)(0),             // 3: daemon.ConnectionEventType
	(ServiceStatus_Type)(0),              // 4: daemon.ServiceStatus.Type
	(*ServiceStatus)(nil),                // 5: daemon.ServiceStatus
	(*ReloadServiceRequest)(nil),         // 6: daemon.ReloadServiceRequest
	(*SubscribeStatusRequest)(nil),       // 7: daemon.SubscribeStatusRequest
	(*Log)(nil),                          // 8: daemon.Log
	(*DefaultLogLevel)(nil),              // 9: daemon.DefaultLogLevel
	(*Status)(nil),                       // 10: daemon.Status
	(*Groups)(nil),                       // 11: daemon.Groups
	(*Group)(nil),                        // 12: daemon.Group
	(*GroupItem)(nil),                    // 13: daemon.GroupItem
	(*URLTestRequest)(nil),               // 14: daemon.URLTestRequest
	(*URLTestResult)(nil),                // 15: daemon.URLTestResult
	(*URLTestSession)(nil),               // 16: daemon.URLTestSession
	(*URLTestSessionRequest)(nil),        // 17: daemon.URLTestSessionRequest
	(*URLTestEventRequest)(nil),          // 18: daemon.URLTestEventRequest
	(*URLTestEvents)(nil),                // 19: daemon.URLTestEvents
	(*RuntimeSnapshot)(nil),              // 20: daemon.RuntimeSnapshot
	(*RuntimeEventRequest)(nil),          // 21: daemon.RuntimeEventRequest
	(*RuntimeEvent)(nil),                 // 22: daemon.RuntimeEvent
	(*RuntimeEvents)(nil),                // 23: daemon.RuntimeEvents
	(*OutboundExternalInfoRequest)(nil),  // 24: daemon.OutboundExternalInfoRequest
	(*OutboundExternalInfoResponse)(nil), // 25: daemon.OutboundExternalInfoResponse
	(*SelectOutboundRequest)(nil),        // 26: daemon.SelectOutboundRequest
	(*SetGroupExpandRequest)(nil),        // 27: daemon.SetGroupExpandRequest
	(*ClashMode)(nil),                    // 28: daemon.ClashMode
	(*ClashModeStatus)(nil),              // 29: daemon.ClashModeStatus
	(*SystemProxyStatus)(nil),            // 30: daemon.SystemProxyStatus
	(*SetSystemProxyEnabledRequest)(nil), // 31: daemon.SetSystemProxyEnabledRequest
	(*SubscribeConnectionsRequest)(nil),  // 32: daemon.SubscribeConnectionsRequest
	(*ConnectionEvent)(nil),              // 33: daemon.ConnectionEvent
	(*ConnectionEvents)(nil),             // 34: daemon.ConnectionEvents
	(*Connection)(nil),                   // 35: daemon.Connection
	(*ProcessInfo)(nil),                  // 36: daemon.ProcessInfo
	(*CloseConnectionRequest)(nil),       // 37: daemon.CloseConnectionRequest
	(*DeprecatedWarnings)(nil),           // 38: daemon.DeprecatedWarnings
	(*DeprecatedWarning)(nil),            // 39: daemon.DeprecatedWarning
	(*StartedAt)(nil),                    // 40: daemon.StartedAt
	(*Log_Message)(nil),                  // 41: daemon.Log.Message
	(*emptypb.Empty)(nil),                // 42: google.protobuf.Empty
}
var file_daemon_started_service_proto_depIdxs = []int32{
	4,  // 0: daemon.ServiceStatus.status:type_name -> daemon.ServiceStatus.Type
	41, // 1: daemon.Log.messages:type_name -> daemon.Log.Message
	0,  // 2: daemon.DefaultLogLevel.level:type_name -> daemon.LogLevel
	12, // 3: daemon.Groups.group:type_name -> daemon.Group
	13, // 4: daemon.Group.items:type_name -> daemon.GroupItem
	1,  // 5: daemon.URLTestSession.state:type_name -> daemon.URLTestSessionState
	15, // 6: daemon.URLTestSession.results:type_name -> daemon.URLTestResult
	16, // 7: daemon.URLTestEvents.sessions:type_name -> daemon.URLTestSession
	5,  // 8: daemon.RuntimeSnapshot.service:type_name -> daemon.ServiceStatus
	10, // 9: daemon.RuntimeSnapshot.status:type_name -> daemon.Status
	11, // 10: daemon.RuntimeSnapshot.groups:type_name -> daemon.Groups
	29, // 11: daemon.RuntimeSnapshot.clashMode:type_name -> daemon.ClashModeStatus
	16, // 12: daemon.RuntimeSnapshot.urlTestSessions:type_name -> daemon.URLTestSession
	2,  // 13: daemon.RuntimeEvent.type:type_name -> daemon.RuntimeEventType
	5,  // 14: daemon.RuntimeEvent.service:type_name -> daemon.ServiceStatus
	10, // 15: daemon.RuntimeEvent.status:type_name -> daemon.Status
	11, // 16: daemon.RuntimeEvent.groups:type_name -> daemon.Groups
	29, // 17: daemon.RuntimeEvent.clashMode:type_name -> daemon.ClashModeStatus
	16, // 18: daemon.RuntimeEvent.urlTestSessions:type_name -> daemon.URLTestSession
	20, // 19: daemon.RuntimeEvents.snapshot:type_name -> daemon.RuntimeSnapshot
	22, // 20: daemon.RuntimeEvents.events:type_name -> daemon.RuntimeEvent
	3,  // 21: daemon.ConnectionEvent.type:type_name -> daemon.ConnectionEventType
	35, // 22: daemon.ConnectionEvent.connection:type_name -> daemon.Connection
	33, // 23: daemon.ConnectionEvents.events:type_name -> daemon.ConnectionEvent
	36, // 24: daemon.Connection.processInfo:type_name -> daemon.ProcessInfo
	39, // 25: daemon.DeprecatedWarnings.warnings:type_name -> daemon.DeprecatedWarning
	0,  // 26: daemon.Log.Message.level:type_name -> daemon.LogLevel
	42, // 27: daemon.StartedService.StopService:input_type -> google.protobuf.Empty
	42, // 28: daemon.StartedService.ReloadService:input_type -> google.protobuf.Empty
	42, // 29: daemon.StartedService.SubscribeServiceStatus:input_type -> google.protobuf.Empty
	42, // 30: daemon.StartedService.SubscribeLog:input_type -> google.protobuf.Empty
	42, // 31: daemon.StartedService.GetDefaultLogLevel:input_type -> google.protobuf.Empty
	42, // 32: daemon.StartedService.ClearLogs:input_type -> google.protobuf.Empty
	7,  // 33: daemon.StartedService.SubscribeStatus:input_type -> daemon.SubscribeStatusRequest
	42, // 34: daemon.StartedService.SubscribeGroups:input_type -> google.protobuf.Empty
	42, // 35: daemon.StartedService.GetClashModeStatus:input_type -> google.protobuf.Empty
	42, // 36: daemon.StartedService.SubscribeClashMode:input_type -> google.protobuf.Empty
	28, // 37: daemon.StartedService.SetClashMode:input_type -> daemon.ClashMode
	42, // 38: daemon.StartedService.GetRuntimeSnapshot:input_type -> google.protobuf.Empty
	21, // 39: daemon.StartedService.SubscribeRuntimeEvents:input_type -> daemon.RuntimeEventRequest
	14, // 40: daemon.StartedService.StartURLTest:input_type -> daemon.URLTestRequest
	17, // 41: daemon.StartedService.GetURLTestSession:input_type -> daemon.URLTestSessionRequest
	17, // 42: daemon.StartedService.CancelURLTest:input_type -> daemon.URLTestSessionRequest
	18, // 43: daemon.StartedService.SubscribeURLTestEvents:input_type -> daemon.URLTestEventRequest
	24, // 44: daemon.StartedService.LookupOutboundExternalInfo:input_type -> daemon.OutboundExternalInfoRequest
	26, // 45: daemon.StartedService.SelectOutbound:input_type -> daemon.SelectOutboundRequest
	27, // 46: daemon.StartedService.SetGroupExpand:input_type -> daemon.SetGroupExpandRequest
	42, // 47: daemon.StartedService.GetSystemProxyStatus:input_type -> google.protobuf.Empty
	31, // 48: daemon.StartedService.SetSystemProxyEnabled:input_type -> daemon.SetSystemProxyEnabledRequest
	32, // 49: daemon.StartedService.SubscribeConnections:input_type -> daemon.SubscribeConnectionsRequest
	37, // 50: daemon.StartedService.CloseConnection:input_type -> daemon.CloseConnectionRequest
	42, // 51: daemon.StartedService.CloseAllConnections:input_type -> google.protobuf.Empty
	42, // 52: daemon.StartedService.GetDeprecatedWarnings:input_type -> google.protobuf.Empty
	42, // 53: daemon.StartedService.GetStartedAt:input_type -> google.protobuf.Empty
	42, // 54: daemon.StartedService.StopService:output_type -> google.protobuf.Empty
	42, // 55: daemon.StartedService.ReloadService:output_type -> google.protobuf.Empty
	5,  // 56: daemon.StartedService.SubscribeServiceStatus:output_type -> daemon.ServiceStatus
	8,  // 57: daemon.StartedService.SubscribeLog:output_type -> daemon.Log
	9,  // 58: daemon.StartedService.GetDefaultLogLevel:output_type -> daemon.DefaultLogLevel
	42, // 59: daemon.StartedService.ClearLogs:output_type -> google.protobuf.Empty
	10, // 60: daemon.StartedService.SubscribeStatus:output_type -> daemon.Status
	11, // 61: daemon.StartedService.SubscribeGroups:output_type -> daemon.Groups
	29, // 62: daemon.StartedService.GetClashModeStatus:output_type -> daemon.ClashModeStatus
	28, // 63: daemon.StartedService.SubscribeClashMode:output_type -> daemon.ClashMode
	42, // 64: daemon.StartedService.SetClashMode:output_type -> google.protobuf.Empty
	20, // 65: daemon.StartedService.GetRuntimeSnapshot:output_type -> daemon.RuntimeSnapshot
	23, // 66: daemon.StartedService.SubscribeRuntimeEvents:output_type -> daemon.RuntimeEvents
	16, // 67: daemon.StartedService.StartURLTest:output_type -> daemon.URLTestSession
	16, // 68: daemon.StartedService.GetURLTestSession:output_type -> daemon.URLTestSession
	16, // 69: daemon.StartedService.CancelURLTest:output_type -> daemon.URLTestSession
	19, // 70: daemon.StartedService.SubscribeURLTestEvents:output_type -> daemon.URLTestEvents
	25, // 71: daemon.StartedService.LookupOutboundExternalInfo:output_type -> daemon.OutboundExternalInfoResponse
	42, // 72: daemon.StartedService.SelectOutbound:output_type -> google.protobuf.Empty
	42, // 73: daemon.StartedService.SetGroupExpand:output_type -> google.protobuf.Empty
	30, // 74: daemon.StartedService.GetSystemProxyStatus:output_type -> daemon.SystemProxyStatus
	42, // 75: daemon.StartedService.SetSystemProxyEnabled:output_type -> google.protobuf.Empty
	34, // 76: daemon.StartedService.SubscribeConnections:output_type -> daemon.ConnectionEvents
	42, // 77: daemon.StartedService.CloseConnection:output_type -> google.protobuf.Empty
	42, // 78: daemon.StartedService.CloseAllConnections:output_type -> google.protobuf.Empty
	38, // 79: daemon.StartedService.GetDeprecatedWarnings:output_type -> daemon.DeprecatedWarnings
	40, // 80: daemon.StartedService.GetStartedAt:output_type -> daemon.StartedAt
	54, // [54:81] is the sub-list for method output_type
	27, // [27:54] is the sub-list for method input_type
	27, // [27:27] is the sub-list for extension type_name
	27, // [27:27] is the sub-list for extension extendee
	0,  // [0:27] is the sub-list for field type_name
}

func init() { file_daemon_started_service_proto_init() }
func file_daemon_started_service_proto_init() {
	if File_daemon_started_service_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_daemon_started_service_proto_rawDesc), len(file_daemon_started_service_proto_rawDesc)),
			NumEnums:      5,
			NumMessages:   37,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_daemon_started_service_proto_goTypes,
		DependencyIndexes: file_daemon_started_service_proto_depIdxs,
		EnumInfos:         file_daemon_started_service_proto_enumTypes,
		MessageInfos:      file_daemon_started_service_proto_msgTypes,
	}.Build()
	File_daemon_started_service_proto = out.File
	file_daemon_started_service_proto_goTypes = nil
	file_daemon_started_service_proto_depIdxs = nil
}
