// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: network/grpc/service/service.proto

package service

import (
	context "context"
	fmt "fmt"
	types "github.com/dshulyak/rapid/types"
	proto "github.com/gogo/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type JoinResp_Code int32

const (
	JoinResp_NONE     JoinResp_Code = 0
	JoinResp_OK       JoinResp_Code = 1
	JoinResp_OUTDATED JoinResp_Code = 2
)

var JoinResp_Code_name = map[int32]string{
	0: "NONE",
	1: "OK",
	2: "OUTDATED",
}

var JoinResp_Code_value = map[string]int32{
	"NONE":     0,
	"OK":       1,
	"OUTDATED": 2,
}

func (x JoinResp_Code) String() string {
	return proto.EnumName(JoinResp_Code_name, int32(x))
}

func (JoinResp_Code) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_796daa064492246c, []int{3, 0}
}

type Batch struct {
	Messages []*types.Message `protobuf:"bytes,1,rep,name=messages,proto3" json:"messages,omitempty"`
}

func (m *Batch) Reset()         { *m = Batch{} }
func (m *Batch) String() string { return proto.CompactTextString(m) }
func (*Batch) ProtoMessage()    {}
func (*Batch) Descriptor() ([]byte, []int) {
	return fileDescriptor_796daa064492246c, []int{0}
}
func (m *Batch) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Batch) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Batch.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Batch) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Batch.Merge(m, src)
}
func (m *Batch) XXX_Size() int {
	return m.Size()
}
func (m *Batch) XXX_DiscardUnknown() {
	xxx_messageInfo_Batch.DiscardUnknown(m)
}

var xxx_messageInfo_Batch proto.InternalMessageInfo

func (m *Batch) GetMessages() []*types.Message {
	if m != nil {
		return m.Messages
	}
	return nil
}

type BatchResponse struct {
}

func (m *BatchResponse) Reset()         { *m = BatchResponse{} }
func (m *BatchResponse) String() string { return proto.CompactTextString(m) }
func (*BatchResponse) ProtoMessage()    {}
func (*BatchResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_796daa064492246c, []int{1}
}
func (m *BatchResponse) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *BatchResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_BatchResponse.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *BatchResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BatchResponse.Merge(m, src)
}
func (m *BatchResponse) XXX_Size() int {
	return m.Size()
}
func (m *BatchResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_BatchResponse.DiscardUnknown(m)
}

var xxx_messageInfo_BatchResponse proto.InternalMessageInfo

type JoinReq struct {
	InstanceID uint64      `protobuf:"varint,1,opt,name=instanceID,proto3" json:"instanceID,omitempty"`
	Node       *types.Node `protobuf:"bytes,2,opt,name=node,proto3" json:"node,omitempty"`
}

func (m *JoinReq) Reset()         { *m = JoinReq{} }
func (m *JoinReq) String() string { return proto.CompactTextString(m) }
func (*JoinReq) ProtoMessage()    {}
func (*JoinReq) Descriptor() ([]byte, []int) {
	return fileDescriptor_796daa064492246c, []int{2}
}
func (m *JoinReq) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *JoinReq) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_JoinReq.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *JoinReq) XXX_Merge(src proto.Message) {
	xxx_messageInfo_JoinReq.Merge(m, src)
}
func (m *JoinReq) XXX_Size() int {
	return m.Size()
}
func (m *JoinReq) XXX_DiscardUnknown() {
	xxx_messageInfo_JoinReq.DiscardUnknown(m)
}

var xxx_messageInfo_JoinReq proto.InternalMessageInfo

func (m *JoinReq) GetInstanceID() uint64 {
	if m != nil {
		return m.InstanceID
	}
	return 0
}

func (m *JoinReq) GetNode() *types.Node {
	if m != nil {
		return m.Node
	}
	return nil
}

type JoinResp struct {
	Code JoinResp_Code `protobuf:"varint,1,opt,name=code,proto3,enum=JoinResp_Code" json:"code,omitempty"`
}

func (m *JoinResp) Reset()         { *m = JoinResp{} }
func (m *JoinResp) String() string { return proto.CompactTextString(m) }
func (*JoinResp) ProtoMessage()    {}
func (*JoinResp) Descriptor() ([]byte, []int) {
	return fileDescriptor_796daa064492246c, []int{3}
}
func (m *JoinResp) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *JoinResp) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_JoinResp.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *JoinResp) XXX_Merge(src proto.Message) {
	xxx_messageInfo_JoinResp.Merge(m, src)
}
func (m *JoinResp) XXX_Size() int {
	return m.Size()
}
func (m *JoinResp) XXX_DiscardUnknown() {
	xxx_messageInfo_JoinResp.DiscardUnknown(m)
}

var xxx_messageInfo_JoinResp proto.InternalMessageInfo

func (m *JoinResp) GetCode() JoinResp_Code {
	if m != nil {
		return m.Code
	}
	return JoinResp_NONE
}

type ConfigurationReq struct {
}

func (m *ConfigurationReq) Reset()         { *m = ConfigurationReq{} }
func (m *ConfigurationReq) String() string { return proto.CompactTextString(m) }
func (*ConfigurationReq) ProtoMessage()    {}
func (*ConfigurationReq) Descriptor() ([]byte, []int) {
	return fileDescriptor_796daa064492246c, []int{4}
}
func (m *ConfigurationReq) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ConfigurationReq) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ConfigurationReq.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ConfigurationReq) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ConfigurationReq.Merge(m, src)
}
func (m *ConfigurationReq) XXX_Size() int {
	return m.Size()
}
func (m *ConfigurationReq) XXX_DiscardUnknown() {
	xxx_messageInfo_ConfigurationReq.DiscardUnknown(m)
}

var xxx_messageInfo_ConfigurationReq proto.InternalMessageInfo

func init() {
	proto.RegisterEnum("JoinResp_Code", JoinResp_Code_name, JoinResp_Code_value)
	proto.RegisterType((*Batch)(nil), "Batch")
	proto.RegisterType((*BatchResponse)(nil), "BatchResponse")
	proto.RegisterType((*JoinReq)(nil), "JoinReq")
	proto.RegisterType((*JoinResp)(nil), "JoinResp")
	proto.RegisterType((*ConfigurationReq)(nil), "ConfigurationReq")
}

func init() { proto.RegisterFile("network/grpc/service/service.proto", fileDescriptor_796daa064492246c) }

var fileDescriptor_796daa064492246c = []byte{
	// 390 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x6c, 0x92, 0x31, 0x8f, 0xd3, 0x30,
	0x1c, 0xc5, 0xe3, 0x12, 0x4a, 0xee, 0x1f, 0x2e, 0xe4, 0x2c, 0x86, 0x2a, 0x12, 0x21, 0xf2, 0x80,
	0x2a, 0x06, 0x47, 0xca, 0x6d, 0x6c, 0xe4, 0x7a, 0x12, 0x1c, 0xa2, 0x95, 0xc2, 0xc1, 0xc0, 0xe6,
	0x26, 0x6e, 0x1a, 0x95, 0xda, 0xa9, 0xed, 0x82, 0xfa, 0x2d, 0xf8, 0x58, 0x8c, 0x1d, 0x19, 0x51,
	0xfb, 0x45, 0x50, 0x92, 0xb6, 0xa2, 0xe8, 0x96, 0x44, 0x7e, 0xef, 0xf9, 0xfd, 0xfd, 0x93, 0x0d,
	0x44, 0x70, 0xf3, 0x43, 0xaa, 0x45, 0x5c, 0xaa, 0x3a, 0x8f, 0x35, 0x57, 0xdf, 0xab, 0x9c, 0x1f,
	0xff, 0xb4, 0x56, 0xd2, 0xc8, 0xe0, 0xca, 0x6c, 0x6a, 0xae, 0xe3, 0xf6, 0xdb, 0x49, 0xe4, 0x1a,
	0x1e, 0xa7, 0xcc, 0xe4, 0x73, 0xfc, 0x1a, 0x9c, 0x25, 0xd7, 0x9a, 0x95, 0x5c, 0x0f, 0x50, 0xf4,
	0x68, 0xe8, 0x26, 0x1e, 0xed, 0x82, 0x1f, 0x3b, 0x39, 0x3b, 0xf9, 0xe4, 0x19, 0x5c, 0xb6, 0x9b,
	0x32, 0xae, 0x6b, 0x29, 0x34, 0x27, 0x77, 0xf0, 0xe4, 0x4e, 0x56, 0x22, 0xe3, 0x2b, 0x1c, 0x02,
	0x54, 0x42, 0x1b, 0x26, 0x72, 0xfe, 0x7e, 0x34, 0x40, 0x11, 0x1a, 0xda, 0xd9, 0x3f, 0x0a, 0x7e,
	0x09, 0xb6, 0x90, 0x05, 0x1f, 0xf4, 0x22, 0x34, 0x74, 0x13, 0xf7, 0x30, 0x63, 0x2c, 0x0b, 0x9e,
	0xb5, 0x06, 0xf9, 0x02, 0x4e, 0xd7, 0xa5, 0x6b, 0x4c, 0xc0, 0xce, 0x9b, 0x70, 0x53, 0xe3, 0x25,
	0x1e, 0x3d, 0x1a, 0xf4, 0xa6, 0xcd, 0x37, 0x1e, 0x79, 0x05, 0x76, 0xb3, 0xc2, 0x0e, 0xd8, 0xe3,
	0xc9, 0xf8, 0xd6, 0xb7, 0x70, 0x1f, 0x7a, 0x93, 0x0f, 0x3e, 0xc2, 0x4f, 0xc1, 0x99, 0x7c, 0xbe,
	0x1f, 0xbd, 0xbd, 0xbf, 0x1d, 0xf9, 0x3d, 0x82, 0xc1, 0xbf, 0x91, 0x62, 0x56, 0x95, 0x6b, 0xc5,
	0x4c, 0x25, 0x9b, 0xc3, 0x26, 0x31, 0xb8, 0xa9, 0x92, 0xac, 0xc8, 0x99, 0x36, 0x5c, 0xe1, 0x08,
	0xec, 0x4f, 0x5c, 0x14, 0xb8, 0x4f, 0x5b, 0xbc, 0xc0, 0xa3, 0xe7, 0x98, 0x56, 0x32, 0x83, 0x8b,
	0x54, 0x4a, 0xa3, 0x8d, 0x62, 0x35, 0x7e, 0x03, 0x97, 0x67, 0x8d, 0xf8, 0x8a, 0xfe, 0x3f, 0x21,
	0x78, 0x7e, 0x00, 0x3c, 0x33, 0x88, 0x85, 0x5f, 0x80, 0xdd, 0xc0, 0x60, 0xe7, 0xc0, 0xb4, 0x0a,
	0x2e, 0x4e, 0x74, 0xc4, 0x4a, 0xdf, 0xfd, 0xda, 0x85, 0x68, 0xbb, 0x0b, 0xd1, 0x9f, 0x5d, 0x88,
	0x7e, 0xee, 0x43, 0x6b, 0xbb, 0x0f, 0xad, 0xdf, 0xfb, 0xd0, 0xfa, 0x4a, 0xcb, 0xca, 0xcc, 0xd7,
	0x53, 0x9a, 0xcb, 0x65, 0x5c, 0xe8, 0xf9, 0xfa, 0xdb, 0x86, 0x2d, 0x62, 0xc5, 0xea, 0xaa, 0x88,
	0x1f, 0x7a, 0x01, 0xd3, 0x7e, 0x7b, 0xcf, 0xd7, 0x7f, 0x03, 0x00, 0x00, 0xff, 0xff, 0xa1, 0x18,
	0x56, 0x5a, 0x20, 0x02, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// BroadcasterClient is the client API for Broadcaster service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type BroadcasterClient interface {
	Send(ctx context.Context, in *Batch, opts ...grpc.CallOption) (*BatchResponse, error)
}

type broadcasterClient struct {
	cc *grpc.ClientConn
}

func NewBroadcasterClient(cc *grpc.ClientConn) BroadcasterClient {
	return &broadcasterClient{cc}
}

func (c *broadcasterClient) Send(ctx context.Context, in *Batch, opts ...grpc.CallOption) (*BatchResponse, error) {
	out := new(BatchResponse)
	err := c.cc.Invoke(ctx, "/Broadcaster/Send", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BroadcasterServer is the server API for Broadcaster service.
type BroadcasterServer interface {
	Send(context.Context, *Batch) (*BatchResponse, error)
}

// UnimplementedBroadcasterServer can be embedded to have forward compatible implementations.
type UnimplementedBroadcasterServer struct {
}

func (*UnimplementedBroadcasterServer) Send(ctx context.Context, req *Batch) (*BatchResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Send not implemented")
}

func RegisterBroadcasterServer(s *grpc.Server, srv BroadcasterServer) {
	s.RegisterService(&_Broadcaster_serviceDesc, srv)
}

func _Broadcaster_Send_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Batch)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BroadcasterServer).Send(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/Broadcaster/Send",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BroadcasterServer).Send(ctx, req.(*Batch))
	}
	return interceptor(ctx, in, info, handler)
}

var _Broadcaster_serviceDesc = grpc.ServiceDesc{
	ServiceName: "Broadcaster",
	HandlerType: (*BroadcasterServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Send",
			Handler:    _Broadcaster_Send_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "network/grpc/service/service.proto",
}

// BootstrapClient is the client API for Bootstrap service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type BootstrapClient interface {
	Configuration(ctx context.Context, in *ConfigurationReq, opts ...grpc.CallOption) (*types.Configuration, error)
	Join(ctx context.Context, in *JoinReq, opts ...grpc.CallOption) (*JoinResp, error)
}

type bootstrapClient struct {
	cc *grpc.ClientConn
}

func NewBootstrapClient(cc *grpc.ClientConn) BootstrapClient {
	return &bootstrapClient{cc}
}

func (c *bootstrapClient) Configuration(ctx context.Context, in *ConfigurationReq, opts ...grpc.CallOption) (*types.Configuration, error) {
	out := new(types.Configuration)
	err := c.cc.Invoke(ctx, "/Bootstrap/Configuration", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *bootstrapClient) Join(ctx context.Context, in *JoinReq, opts ...grpc.CallOption) (*JoinResp, error) {
	out := new(JoinResp)
	err := c.cc.Invoke(ctx, "/Bootstrap/Join", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BootstrapServer is the server API for Bootstrap service.
type BootstrapServer interface {
	Configuration(context.Context, *ConfigurationReq) (*types.Configuration, error)
	Join(context.Context, *JoinReq) (*JoinResp, error)
}

// UnimplementedBootstrapServer can be embedded to have forward compatible implementations.
type UnimplementedBootstrapServer struct {
}

func (*UnimplementedBootstrapServer) Configuration(ctx context.Context, req *ConfigurationReq) (*types.Configuration, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Configuration not implemented")
}
func (*UnimplementedBootstrapServer) Join(ctx context.Context, req *JoinReq) (*JoinResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Join not implemented")
}

func RegisterBootstrapServer(s *grpc.Server, srv BootstrapServer) {
	s.RegisterService(&_Bootstrap_serviceDesc, srv)
}

func _Bootstrap_Configuration_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ConfigurationReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BootstrapServer).Configuration(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/Bootstrap/Configuration",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BootstrapServer).Configuration(ctx, req.(*ConfigurationReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Bootstrap_Join_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JoinReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BootstrapServer).Join(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/Bootstrap/Join",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BootstrapServer).Join(ctx, req.(*JoinReq))
	}
	return interceptor(ctx, in, info, handler)
}

var _Bootstrap_serviceDesc = grpc.ServiceDesc{
	ServiceName: "Bootstrap",
	HandlerType: (*BootstrapServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Configuration",
			Handler:    _Bootstrap_Configuration_Handler,
		},
		{
			MethodName: "Join",
			Handler:    _Bootstrap_Join_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "network/grpc/service/service.proto",
}

func (m *Batch) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Batch) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Batch) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Messages) > 0 {
		for iNdEx := len(m.Messages) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Messages[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintService(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0xa
		}
	}
	return len(dAtA) - i, nil
}

func (m *BatchResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *BatchResponse) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *BatchResponse) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	return len(dAtA) - i, nil
}

func (m *JoinReq) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *JoinReq) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *JoinReq) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Node != nil {
		{
			size, err := m.Node.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintService(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x12
	}
	if m.InstanceID != 0 {
		i = encodeVarintService(dAtA, i, uint64(m.InstanceID))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *JoinResp) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *JoinResp) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *JoinResp) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Code != 0 {
		i = encodeVarintService(dAtA, i, uint64(m.Code))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *ConfigurationReq) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ConfigurationReq) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *ConfigurationReq) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	return len(dAtA) - i, nil
}

func encodeVarintService(dAtA []byte, offset int, v uint64) int {
	offset -= sovService(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *Batch) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.Messages) > 0 {
		for _, e := range m.Messages {
			l = e.Size()
			n += 1 + l + sovService(uint64(l))
		}
	}
	return n
}

func (m *BatchResponse) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	return n
}

func (m *JoinReq) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.InstanceID != 0 {
		n += 1 + sovService(uint64(m.InstanceID))
	}
	if m.Node != nil {
		l = m.Node.Size()
		n += 1 + l + sovService(uint64(l))
	}
	return n
}

func (m *JoinResp) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Code != 0 {
		n += 1 + sovService(uint64(m.Code))
	}
	return n
}

func (m *ConfigurationReq) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	return n
}

func sovService(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozService(x uint64) (n int) {
	return sovService(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Batch) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowService
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Batch: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Batch: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Messages", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowService
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthService
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthService
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Messages = append(m.Messages, &types.Message{})
			if err := m.Messages[len(m.Messages)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipService(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthService
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthService
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *BatchResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowService
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: BatchResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: BatchResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		default:
			iNdEx = preIndex
			skippy, err := skipService(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthService
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthService
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *JoinReq) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowService
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: JoinReq: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: JoinReq: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field InstanceID", wireType)
			}
			m.InstanceID = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowService
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.InstanceID |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Node", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowService
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthService
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthService
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Node == nil {
				m.Node = &types.Node{}
			}
			if err := m.Node.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipService(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthService
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthService
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *JoinResp) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowService
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: JoinResp: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: JoinResp: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Code", wireType)
			}
			m.Code = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowService
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Code |= JoinResp_Code(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipService(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthService
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthService
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ConfigurationReq) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowService
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ConfigurationReq: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ConfigurationReq: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		default:
			iNdEx = preIndex
			skippy, err := skipService(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthService
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthService
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipService(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowService
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowService
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowService
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthService
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupService
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthService
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthService        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowService          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupService = fmt.Errorf("proto: unexpected end of group")
)
