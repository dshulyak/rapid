// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: consensus/swarms/grpc/server/server.proto

package server

import (
	context "context"
	fmt "fmt"
	types "github.com/dshulyak/rapid/consensus/types"
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

type Empty struct {
}

func (m *Empty) Reset()         { *m = Empty{} }
func (m *Empty) String() string { return proto.CompactTextString(m) }
func (*Empty) ProtoMessage()    {}
func (*Empty) Descriptor() ([]byte, []int) {
	return fileDescriptor_a9fc6822589b2bd5, []int{0}
}
func (m *Empty) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Empty) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Empty.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Empty) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Empty.Merge(m, src)
}
func (m *Empty) XXX_Size() int {
	return m.Size()
}
func (m *Empty) XXX_DiscardUnknown() {
	xxx_messageInfo_Empty.DiscardUnknown(m)
}

var xxx_messageInfo_Empty proto.InternalMessageInfo

func init() {
	proto.RegisterType((*Empty)(nil), "server.Empty")
}

func init() {
	proto.RegisterFile("consensus/swarms/grpc/server/server.proto", fileDescriptor_a9fc6822589b2bd5)
}

var fileDescriptor_a9fc6822589b2bd5 = []byte{
	// 185 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xd2, 0x4c, 0xce, 0xcf, 0x2b,
	0x4e, 0xcd, 0x2b, 0x2e, 0x2d, 0xd6, 0x2f, 0x2e, 0x4f, 0x2c, 0xca, 0x2d, 0xd6, 0x4f, 0x2f, 0x2a,
	0x48, 0xd6, 0x2f, 0x4e, 0x2d, 0x2a, 0x4b, 0x2d, 0x82, 0x52, 0x7a, 0x05, 0x45, 0xf9, 0x25, 0xf9,
	0x42, 0x6c, 0x10, 0x9e, 0x94, 0x34, 0x42, 0x4b, 0x49, 0x65, 0x41, 0x2a, 0x94, 0x84, 0x28, 0x52,
	0x62, 0xe7, 0x62, 0x75, 0xcd, 0x2d, 0x28, 0xa9, 0x34, 0x32, 0xe4, 0x62, 0x0b, 0x06, 0xab, 0x17,
	0x52, 0xe7, 0x62, 0x09, 0x4e, 0xcd, 0x4b, 0x11, 0xe2, 0xd3, 0x83, 0x28, 0xf4, 0x4d, 0x2d, 0x2e,
	0x4e, 0x4c, 0x4f, 0x95, 0xe2, 0xd5, 0x83, 0x1a, 0x0f, 0xd6, 0xa0, 0xc4, 0xe0, 0x14, 0x70, 0xe2,
	0x91, 0x1c, 0xe3, 0x85, 0x47, 0x72, 0x8c, 0x0f, 0x1e, 0xc9, 0x31, 0x4e, 0x78, 0x2c, 0xc7, 0x70,
	0xe1, 0xb1, 0x1c, 0xc3, 0x8d, 0xc7, 0x72, 0x0c, 0x51, 0x66, 0xe9, 0x99, 0x25, 0x19, 0xa5, 0x49,
	0x7a, 0xc9, 0xf9, 0xb9, 0xfa, 0x29, 0xc5, 0x19, 0xa5, 0x39, 0x95, 0x89, 0xd9, 0xfa, 0x45, 0x89,
	0x05, 0x99, 0x29, 0xfa, 0xf8, 0xdc, 0x9f, 0xc4, 0x06, 0x76, 0x94, 0x31, 0x20, 0x00, 0x00, 0xff,
	0xff, 0x2d, 0x93, 0x3c, 0xeb, 0xe6, 0x00, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// ServerClient is the client API for Server service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type ServerClient interface {
	Send(ctx context.Context, in *types.Message, opts ...grpc.CallOption) (*Empty, error)
}

type serverClient struct {
	cc *grpc.ClientConn
}

func NewServerClient(cc *grpc.ClientConn) ServerClient {
	return &serverClient{cc}
}

func (c *serverClient) Send(ctx context.Context, in *types.Message, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/server.Server/Send", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ServerServer is the server API for Server service.
type ServerServer interface {
	Send(context.Context, *types.Message) (*Empty, error)
}

// UnimplementedServerServer can be embedded to have forward compatible implementations.
type UnimplementedServerServer struct {
}

func (*UnimplementedServerServer) Send(ctx context.Context, req *types.Message) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Send not implemented")
}

func RegisterServerServer(s *grpc.Server, srv ServerServer) {
	s.RegisterService(&_Server_serviceDesc, srv)
}

func _Server_Send_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(types.Message)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServerServer).Send(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/server.Server/Send",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServerServer).Send(ctx, req.(*types.Message))
	}
	return interceptor(ctx, in, info, handler)
}

var _Server_serviceDesc = grpc.ServiceDesc{
	ServiceName: "server.Server",
	HandlerType: (*ServerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Send",
			Handler:    _Server_Send_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "consensus/swarms/grpc/server/server.proto",
}

func (m *Empty) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Empty) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Empty) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	return len(dAtA) - i, nil
}

func encodeVarintServer(dAtA []byte, offset int, v uint64) int {
	offset -= sovServer(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *Empty) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	return n
}

func sovServer(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozServer(x uint64) (n int) {
	return sovServer(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Empty) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowServer
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
			return fmt.Errorf("proto: Empty: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Empty: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		default:
			iNdEx = preIndex
			skippy, err := skipServer(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthServer
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthServer
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
func skipServer(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowServer
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
					return 0, ErrIntOverflowServer
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
					return 0, ErrIntOverflowServer
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
				return 0, ErrInvalidLengthServer
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupServer
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthServer
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthServer        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowServer          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupServer = fmt.Errorf("proto: unexpected end of group")
)