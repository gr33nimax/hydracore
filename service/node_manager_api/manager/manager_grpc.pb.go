package manager

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	Manager_AddNode_FullMethodName         = "/node_manager_api.v1.Manager/AddNode"
	Manager_AcquireLock_FullMethodName     = "/node_manager_api.v1.Manager/AcquireLock"
	Manager_RefreshLock_FullMethodName     = "/node_manager_api.v1.Manager/RefreshLock"
	Manager_ReleaseLock_FullMethodName     = "/node_manager_api.v1.Manager/ReleaseLock"
	Manager_AddTrafficUsage_FullMethodName = "/node_manager_api.v1.Manager/AddTrafficUsage"
)

// ManagerClient is the client API for Manager service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ManagerClient interface {
	AddNode(ctx context.Context, in *Node, opts ...grpc.CallOption) (grpc.ServerStreamingClient[NodeData], error)
	AcquireLock(ctx context.Context, in *AcquireLockRequest, opts ...grpc.CallOption) (*LockData, error)
	RefreshLock(ctx context.Context, in *LockData, opts ...grpc.CallOption) (*Empty, error)
	ReleaseLock(ctx context.Context, in *LockData, opts ...grpc.CallOption) (*Empty, error)
	AddTrafficUsage(ctx context.Context, in *TrafficUsageRequest, opts ...grpc.CallOption) (*TrafficUsageReply, error)
}

type managerClient struct {
	cc grpc.ClientConnInterface
}

func NewManagerClient(cc grpc.ClientConnInterface) ManagerClient {
	return &managerClient{cc}
}

func (c *managerClient) AddNode(ctx context.Context, in *Node, opts ...grpc.CallOption) (grpc.ServerStreamingClient[NodeData], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &Manager_ServiceDesc.Streams[0], Manager_AddNode_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[Node, NodeData]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type Manager_AddNodeClient = grpc.ServerStreamingClient[NodeData]

func (c *managerClient) AcquireLock(ctx context.Context, in *AcquireLockRequest, opts ...grpc.CallOption) (*LockData, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(LockData)
	err := c.cc.Invoke(ctx, Manager_AcquireLock_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *managerClient) RefreshLock(ctx context.Context, in *LockData, opts ...grpc.CallOption) (*Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(Empty)
	err := c.cc.Invoke(ctx, Manager_RefreshLock_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *managerClient) ReleaseLock(ctx context.Context, in *LockData, opts ...grpc.CallOption) (*Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(Empty)
	err := c.cc.Invoke(ctx, Manager_ReleaseLock_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *managerClient) AddTrafficUsage(ctx context.Context, in *TrafficUsageRequest, opts ...grpc.CallOption) (*TrafficUsageReply, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(TrafficUsageReply)
	err := c.cc.Invoke(ctx, Manager_AddTrafficUsage_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ManagerServer is the server API for Manager service.
// All implementations must embed UnimplementedManagerServer
// for forward compatibility.
type ManagerServer interface {
	AddNode(*Node, grpc.ServerStreamingServer[NodeData]) error
	AcquireLock(context.Context, *AcquireLockRequest) (*LockData, error)
	RefreshLock(context.Context, *LockData) (*Empty, error)
	ReleaseLock(context.Context, *LockData) (*Empty, error)
	AddTrafficUsage(context.Context, *TrafficUsageRequest) (*TrafficUsageReply, error)
	mustEmbedUnimplementedManagerServer()
}

// UnimplementedManagerServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedManagerServer struct{}

func (UnimplementedManagerServer) AddNode(*Node, grpc.ServerStreamingServer[NodeData]) error {
	return status.Error(codes.Unimplemented, "method AddNode not implemented")
}
func (UnimplementedManagerServer) AcquireLock(context.Context, *AcquireLockRequest) (*LockData, error) {
	return nil, status.Error(codes.Unimplemented, "method AcquireLock not implemented")
}
func (UnimplementedManagerServer) RefreshLock(context.Context, *LockData) (*Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method RefreshLock not implemented")
}
func (UnimplementedManagerServer) ReleaseLock(context.Context, *LockData) (*Empty, error) {
	return nil, status.Error(codes.Unimplemented, "method ReleaseLock not implemented")
}
func (UnimplementedManagerServer) AddTrafficUsage(context.Context, *TrafficUsageRequest) (*TrafficUsageReply, error) {
	return nil, status.Error(codes.Unimplemented, "method AddTrafficUsage not implemented")
}
func (UnimplementedManagerServer) mustEmbedUnimplementedManagerServer() {}
func (UnimplementedManagerServer) testEmbeddedByValue()                 {}

// UnsafeManagerServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ManagerServer will
// result in compilation errors.
type UnsafeManagerServer interface {
	mustEmbedUnimplementedManagerServer()
}

func RegisterManagerServer(s grpc.ServiceRegistrar, srv ManagerServer) {
	// If the following call panics, it indicates UnimplementedManagerServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&Manager_ServiceDesc, srv)
}

func _Manager_AddNode_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(Node)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(ManagerServer).AddNode(m, &grpc.GenericServerStream[Node, NodeData]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type Manager_AddNodeServer = grpc.ServerStreamingServer[NodeData]

func _Manager_AcquireLock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AcquireLockRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ManagerServer).AcquireLock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Manager_AcquireLock_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ManagerServer).AcquireLock(ctx, req.(*AcquireLockRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Manager_RefreshLock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LockData)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ManagerServer).RefreshLock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Manager_RefreshLock_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ManagerServer).RefreshLock(ctx, req.(*LockData))
	}
	return interceptor(ctx, in, info, handler)
}

func _Manager_ReleaseLock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LockData)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ManagerServer).ReleaseLock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Manager_ReleaseLock_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ManagerServer).ReleaseLock(ctx, req.(*LockData))
	}
	return interceptor(ctx, in, info, handler)
}

func _Manager_AddTrafficUsage_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TrafficUsageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ManagerServer).AddTrafficUsage(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Manager_AddTrafficUsage_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ManagerServer).AddTrafficUsage(ctx, req.(*TrafficUsageRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Manager_ServiceDesc is the grpc.ServiceDesc for Manager service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Manager_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "node_manager_api.v1.Manager",
	HandlerType: (*ManagerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "AcquireLock",
			Handler:    _Manager_AcquireLock_Handler,
		},
		{
			MethodName: "RefreshLock",
			Handler:    _Manager_RefreshLock_Handler,
		},
		{
			MethodName: "ReleaseLock",
			Handler:    _Manager_ReleaseLock_Handler,
		},
		{
			MethodName: "AddTrafficUsage",
			Handler:    _Manager_AddTrafficUsage_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "AddNode",
			Handler:       _Manager_AddNode_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "service/node_manager_api/manager/manager.proto",
}
