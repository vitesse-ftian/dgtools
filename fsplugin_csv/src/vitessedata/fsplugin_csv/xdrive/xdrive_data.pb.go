// Code generated by protoc-gen-go.
// source: xdrive_data.proto
// DO NOT EDIT!

/*
Package xdrive is a generated protocol buffer package.

It is generated from these files:
	xdrive_data.proto

It has these top-level messages:
	ColumnDesc
	Filter
	CSVSpec
	FileSpec
	StringList
	KeyValue
	KeyValueList
	RmgrInfo
	ReadRequest
	SampleRequest
	DataReply
	XCol
	XRowSet
	PluginDataReply
	SizeMetaRequest
	SizeMetaReply
	PluginSizeMetaReply
	PageData
	WriteRequest
	PluginWriteRequest
	WriteReply
	PluginWriteReply
*/
package xdrive

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type SpqType int32

const (
	SpqType_UNKNOWN SpqType = 0
	SpqType_BOOL    SpqType = 1
	SpqType_INT16   SpqType = 2
	SpqType_INT32   SpqType = 3
	SpqType_INT64   SpqType = 4
	SpqType_INT128  SpqType = 5
	SpqType_FLOAT   SpqType = 6
	SpqType_DOUBLE  SpqType = 7
	// BYTEA    = 0x0008;   not supported.
	SpqType_CSTR             SpqType = 9
	SpqType_DEC64            SpqType = 10
	SpqType_DEC128           SpqType = 11
	SpqType_DATE             SpqType = 65539
	SpqType_TIME_MILLIS      SpqType = 131075
	SpqType_TIMESTAMP_MILLIS SpqType = 196612
	SpqType_TIME_MICROS      SpqType = 262148
	SpqType_TIMESTAMP_MICROS SpqType = 327684
	SpqType_JSON             SpqType = 393225
)

var SpqType_name = map[int32]string{
	0:      "UNKNOWN",
	1:      "BOOL",
	2:      "INT16",
	3:      "INT32",
	4:      "INT64",
	5:      "INT128",
	6:      "FLOAT",
	7:      "DOUBLE",
	9:      "CSTR",
	10:     "DEC64",
	11:     "DEC128",
	65539:  "DATE",
	131075: "TIME_MILLIS",
	196612: "TIMESTAMP_MILLIS",
	262148: "TIME_MICROS",
	327684: "TIMESTAMP_MICROS",
	393225: "JSON",
}
var SpqType_value = map[string]int32{
	"UNKNOWN":          0,
	"BOOL":             1,
	"INT16":            2,
	"INT32":            3,
	"INT64":            4,
	"INT128":           5,
	"FLOAT":            6,
	"DOUBLE":           7,
	"CSTR":             9,
	"DEC64":            10,
	"DEC128":           11,
	"DATE":             65539,
	"TIME_MILLIS":      131075,
	"TIMESTAMP_MILLIS": 196612,
	"TIME_MICROS":      262148,
	"TIMESTAMP_MICROS": 327684,
	"JSON":             393225,
}

func (x SpqType) String() string {
	return proto.EnumName(SpqType_name, int32(x))
}
func (SpqType) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

type ColumnDesc struct {
	Name string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Type int32  `protobuf:"varint,2,opt,name=type" json:"type,omitempty"`
}

func (m *ColumnDesc) Reset()                    { *m = ColumnDesc{} }
func (m *ColumnDesc) String() string            { return proto.CompactTextString(m) }
func (*ColumnDesc) ProtoMessage()               {}
func (*ColumnDesc) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *ColumnDesc) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *ColumnDesc) GetType() int32 {
	if m != nil {
		return m.Type
	}
	return 0
}

type Filter struct {
	Op     string   `protobuf:"bytes,1,opt,name=op" json:"op,omitempty"`
	Column string   `protobuf:"bytes,2,opt,name=column" json:"column,omitempty"`
	Args   []string `protobuf:"bytes,3,rep,name=args" json:"args,omitempty"`
}

func (m *Filter) Reset()                    { *m = Filter{} }
func (m *Filter) String() string            { return proto.CompactTextString(m) }
func (*Filter) ProtoMessage()               {}
func (*Filter) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *Filter) GetOp() string {
	if m != nil {
		return m.Op
	}
	return ""
}

func (m *Filter) GetColumn() string {
	if m != nil {
		return m.Column
	}
	return ""
}

func (m *Filter) GetArgs() []string {
	if m != nil {
		return m.Args
	}
	return nil
}

type CSVSpec struct {
	Delimiter string `protobuf:"bytes,1,opt,name=delimiter" json:"delimiter,omitempty"`
	Nullstr   string `protobuf:"bytes,2,opt,name=nullstr" json:"nullstr,omitempty"`
	Header    bool   `protobuf:"varint,3,opt,name=header" json:"header,omitempty"`
	Quote     string `protobuf:"bytes,4,opt,name=quote" json:"quote,omitempty"`
	Escape    string `protobuf:"bytes,5,opt,name=escape" json:"escape,omitempty"`
}

func (m *CSVSpec) Reset()                    { *m = CSVSpec{} }
func (m *CSVSpec) String() string            { return proto.CompactTextString(m) }
func (*CSVSpec) ProtoMessage()               {}
func (*CSVSpec) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

func (m *CSVSpec) GetDelimiter() string {
	if m != nil {
		return m.Delimiter
	}
	return ""
}

func (m *CSVSpec) GetNullstr() string {
	if m != nil {
		return m.Nullstr
	}
	return ""
}

func (m *CSVSpec) GetHeader() bool {
	if m != nil {
		return m.Header
	}
	return false
}

func (m *CSVSpec) GetQuote() string {
	if m != nil {
		return m.Quote
	}
	return ""
}

func (m *CSVSpec) GetEscape() string {
	if m != nil {
		return m.Escape
	}
	return ""
}

type FileSpec struct {
	Path    string   `protobuf:"bytes,1,opt,name=path" json:"path,omitempty"`
	Format  string   `protobuf:"bytes,2,opt,name=format" json:"format,omitempty"`
	Csvspec *CSVSpec `protobuf:"bytes,3,opt,name=csvspec" json:"csvspec,omitempty"`
}

func (m *FileSpec) Reset()                    { *m = FileSpec{} }
func (m *FileSpec) String() string            { return proto.CompactTextString(m) }
func (*FileSpec) ProtoMessage()               {}
func (*FileSpec) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{3} }

func (m *FileSpec) GetPath() string {
	if m != nil {
		return m.Path
	}
	return ""
}

func (m *FileSpec) GetFormat() string {
	if m != nil {
		return m.Format
	}
	return ""
}

func (m *FileSpec) GetCsvspec() *CSVSpec {
	if m != nil {
		return m.Csvspec
	}
	return nil
}

type StringList struct {
	Str []string `protobuf:"bytes,1,rep,name=str" json:"str,omitempty"`
}

func (m *StringList) Reset()                    { *m = StringList{} }
func (m *StringList) String() string            { return proto.CompactTextString(m) }
func (*StringList) ProtoMessage()               {}
func (*StringList) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{4} }

func (m *StringList) GetStr() []string {
	if m != nil {
		return m.Str
	}
	return nil
}

type KeyValue struct {
	Key   string `protobuf:"bytes,1,opt,name=key" json:"key,omitempty"`
	Value string `protobuf:"bytes,2,opt,name=value" json:"value,omitempty"`
}

func (m *KeyValue) Reset()                    { *m = KeyValue{} }
func (m *KeyValue) String() string            { return proto.CompactTextString(m) }
func (*KeyValue) ProtoMessage()               {}
func (*KeyValue) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{5} }

func (m *KeyValue) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *KeyValue) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

type KeyValueList struct {
	Kv []*KeyValue `protobuf:"bytes,1,rep,name=kv" json:"kv,omitempty"`
}

func (m *KeyValueList) Reset()                    { *m = KeyValueList{} }
func (m *KeyValueList) String() string            { return proto.CompactTextString(m) }
func (*KeyValueList) ProtoMessage()               {}
func (*KeyValueList) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{6} }

func (m *KeyValueList) GetKv() []*KeyValue {
	if m != nil {
		return m.Kv
	}
	return nil
}

type RmgrInfo struct {
	Scheme   string        `protobuf:"bytes,1,opt,name=scheme" json:"scheme,omitempty"`
	Format   string        `protobuf:"bytes,2,opt,name=format" json:"format,omitempty"`
	Rpath    string        `protobuf:"bytes,3,opt,name=rpath" json:"rpath,omitempty"`
	Conf     *KeyValueList `protobuf:"bytes,4,opt,name=conf" json:"conf,omitempty"`
	Pluginop string        `protobuf:"bytes,5,opt,name=pluginop" json:"pluginop,omitempty"`
}

func (m *RmgrInfo) Reset()                    { *m = RmgrInfo{} }
func (m *RmgrInfo) String() string            { return proto.CompactTextString(m) }
func (*RmgrInfo) ProtoMessage()               {}
func (*RmgrInfo) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{7} }

func (m *RmgrInfo) GetScheme() string {
	if m != nil {
		return m.Scheme
	}
	return ""
}

func (m *RmgrInfo) GetFormat() string {
	if m != nil {
		return m.Format
	}
	return ""
}

func (m *RmgrInfo) GetRpath() string {
	if m != nil {
		return m.Rpath
	}
	return ""
}

func (m *RmgrInfo) GetConf() *KeyValueList {
	if m != nil {
		return m.Conf
	}
	return nil
}

func (m *RmgrInfo) GetPluginop() string {
	if m != nil {
		return m.Pluginop
	}
	return ""
}

type ReadRequest struct {
	// Which file(s)
	Filespec *FileSpec `protobuf:"bytes,1,opt,name=filespec" json:"filespec,omitempty"`
	// Table Schema
	Columndesc []*ColumnDesc `protobuf:"bytes,2,rep,name=columndesc" json:"columndesc,omitempty"`
	// Names of required columns
	Columnlist []string `protobuf:"bytes,3,rep,name=columnlist" json:"columnlist,omitempty"`
	// Filters
	Filter []*Filter `protobuf:"bytes,4,rep,name=filter" json:"filter,omitempty"`
	// Fragment
	FragId  int32 `protobuf:"varint,5,opt,name=frag_id,json=fragId" json:"frag_id,omitempty"`
	FragCnt int32 `protobuf:"varint,6,opt,name=frag_cnt,json=fragCnt" json:"frag_cnt,omitempty"`
}

func (m *ReadRequest) Reset()                    { *m = ReadRequest{} }
func (m *ReadRequest) String() string            { return proto.CompactTextString(m) }
func (*ReadRequest) ProtoMessage()               {}
func (*ReadRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{8} }

func (m *ReadRequest) GetFilespec() *FileSpec {
	if m != nil {
		return m.Filespec
	}
	return nil
}

func (m *ReadRequest) GetColumndesc() []*ColumnDesc {
	if m != nil {
		return m.Columndesc
	}
	return nil
}

func (m *ReadRequest) GetColumnlist() []string {
	if m != nil {
		return m.Columnlist
	}
	return nil
}

func (m *ReadRequest) GetFilter() []*Filter {
	if m != nil {
		return m.Filter
	}
	return nil
}

func (m *ReadRequest) GetFragId() int32 {
	if m != nil {
		return m.FragId
	}
	return 0
}

func (m *ReadRequest) GetFragCnt() int32 {
	if m != nil {
		return m.FragCnt
	}
	return 0
}

type SampleRequest struct {
	// Which file(s)
	Filespec *FileSpec `protobuf:"bytes,1,opt,name=filespec" json:"filespec,omitempty"`
	// Table Schema
	Columndesc []*ColumnDesc `protobuf:"bytes,2,rep,name=columndesc" json:"columndesc,omitempty"`
	// Fragment
	FragId  int32 `protobuf:"varint,3,opt,name=frag_id,json=fragId" json:"frag_id,omitempty"`
	FragCnt int32 `protobuf:"varint,4,opt,name=frag_cnt,json=fragCnt" json:"frag_cnt,omitempty"`
	// Sample size
	Nrow int32 `protobuf:"varint,5,opt,name=nrow" json:"nrow,omitempty"`
}

func (m *SampleRequest) Reset()                    { *m = SampleRequest{} }
func (m *SampleRequest) String() string            { return proto.CompactTextString(m) }
func (*SampleRequest) ProtoMessage()               {}
func (*SampleRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{9} }

func (m *SampleRequest) GetFilespec() *FileSpec {
	if m != nil {
		return m.Filespec
	}
	return nil
}

func (m *SampleRequest) GetColumndesc() []*ColumnDesc {
	if m != nil {
		return m.Columndesc
	}
	return nil
}

func (m *SampleRequest) GetFragId() int32 {
	if m != nil {
		return m.FragId
	}
	return 0
}

func (m *SampleRequest) GetFragCnt() int32 {
	if m != nil {
		return m.FragCnt
	}
	return 0
}

func (m *SampleRequest) GetNrow() int32 {
	if m != nil {
		return m.Nrow
	}
	return 0
}

type DataReply struct {
	Data []byte `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
}

func (m *DataReply) Reset()                    { *m = DataReply{} }
func (m *DataReply) String() string            { return proto.CompactTextString(m) }
func (*DataReply) ProtoMessage()               {}
func (*DataReply) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{10} }

func (m *DataReply) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

type XCol struct {
	Colname string    `protobuf:"bytes,1,opt,name=colname" json:"colname,omitempty"`
	Nrow    int32     `protobuf:"varint,2,opt,name=nrow" json:"nrow,omitempty"`
	Nullmap []bool    `protobuf:"varint,3,rep,packed,name=nullmap" json:"nullmap,omitempty"`
	Sdata   []string  `protobuf:"bytes,4,rep,name=sdata" json:"sdata,omitempty"`
	I32Data []int32   `protobuf:"varint,5,rep,packed,name=i32data" json:"i32data,omitempty"`
	I64Data []int64   `protobuf:"varint,6,rep,packed,name=i64data" json:"i64data,omitempty"`
	F32Data []float32 `protobuf:"fixed32,7,rep,packed,name=f32data" json:"f32data,omitempty"`
	F64Data []float64 `protobuf:"fixed64,8,rep,packed,name=f64data" json:"f64data,omitempty"`
}

func (m *XCol) Reset()                    { *m = XCol{} }
func (m *XCol) String() string            { return proto.CompactTextString(m) }
func (*XCol) ProtoMessage()               {}
func (*XCol) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{11} }

func (m *XCol) GetColname() string {
	if m != nil {
		return m.Colname
	}
	return ""
}

func (m *XCol) GetNrow() int32 {
	if m != nil {
		return m.Nrow
	}
	return 0
}

func (m *XCol) GetNullmap() []bool {
	if m != nil {
		return m.Nullmap
	}
	return nil
}

func (m *XCol) GetSdata() []string {
	if m != nil {
		return m.Sdata
	}
	return nil
}

func (m *XCol) GetI32Data() []int32 {
	if m != nil {
		return m.I32Data
	}
	return nil
}

func (m *XCol) GetI64Data() []int64 {
	if m != nil {
		return m.I64Data
	}
	return nil
}

func (m *XCol) GetF32Data() []float32 {
	if m != nil {
		return m.F32Data
	}
	return nil
}

func (m *XCol) GetF64Data() []float64 {
	if m != nil {
		return m.F64Data
	}
	return nil
}

type XRowSet struct {
	Tag     int32   `protobuf:"varint,1,opt,name=tag" json:"tag,omitempty"`
	Round   int32   `protobuf:"varint,2,opt,name=round" json:"round,omitempty"`
	Columns []*XCol `protobuf:"bytes,3,rep,name=columns" json:"columns,omitempty"`
}

func (m *XRowSet) Reset()                    { *m = XRowSet{} }
func (m *XRowSet) String() string            { return proto.CompactTextString(m) }
func (*XRowSet) ProtoMessage()               {}
func (*XRowSet) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{12} }

func (m *XRowSet) GetTag() int32 {
	if m != nil {
		return m.Tag
	}
	return 0
}

func (m *XRowSet) GetRound() int32 {
	if m != nil {
		return m.Round
	}
	return 0
}

func (m *XRowSet) GetColumns() []*XCol {
	if m != nil {
		return m.Columns
	}
	return nil
}

type PluginDataReply struct {
	Errcode int32    `protobuf:"varint,1,opt,name=errcode" json:"errcode,omitempty"`
	Errmsg  string   `protobuf:"bytes,2,opt,name=errmsg" json:"errmsg,omitempty"`
	Rowset  *XRowSet `protobuf:"bytes,3,opt,name=rowset" json:"rowset,omitempty"`
}

func (m *PluginDataReply) Reset()                    { *m = PluginDataReply{} }
func (m *PluginDataReply) String() string            { return proto.CompactTextString(m) }
func (*PluginDataReply) ProtoMessage()               {}
func (*PluginDataReply) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{13} }

func (m *PluginDataReply) GetErrcode() int32 {
	if m != nil {
		return m.Errcode
	}
	return 0
}

func (m *PluginDataReply) GetErrmsg() string {
	if m != nil {
		return m.Errmsg
	}
	return ""
}

func (m *PluginDataReply) GetRowset() *XRowSet {
	if m != nil {
		return m.Rowset
	}
	return nil
}

type SizeMetaRequest struct {
	// Which file(s)
	Filespec *FileSpec `protobuf:"bytes,1,opt,name=filespec" json:"filespec,omitempty"`
	// Table Schema
	Columndesc []*ColumnDesc `protobuf:"bytes,2,rep,name=columndesc" json:"columndesc,omitempty"`
	// Fragment
	FragId  int32 `protobuf:"varint,3,opt,name=frag_id,json=fragId" json:"frag_id,omitempty"`
	FragCnt int32 `protobuf:"varint,4,opt,name=frag_cnt,json=fragCnt" json:"frag_cnt,omitempty"`
}

func (m *SizeMetaRequest) Reset()                    { *m = SizeMetaRequest{} }
func (m *SizeMetaRequest) String() string            { return proto.CompactTextString(m) }
func (*SizeMetaRequest) ProtoMessage()               {}
func (*SizeMetaRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{14} }

func (m *SizeMetaRequest) GetFilespec() *FileSpec {
	if m != nil {
		return m.Filespec
	}
	return nil
}

func (m *SizeMetaRequest) GetColumndesc() []*ColumnDesc {
	if m != nil {
		return m.Columndesc
	}
	return nil
}

func (m *SizeMetaRequest) GetFragId() int32 {
	if m != nil {
		return m.FragId
	}
	return 0
}

func (m *SizeMetaRequest) GetFragCnt() int32 {
	if m != nil {
		return m.FragCnt
	}
	return 0
}

type SizeMetaReply struct {
	Nrow  int64 `protobuf:"varint,1,opt,name=nrow" json:"nrow,omitempty"`
	Nbyte int64 `protobuf:"varint,2,opt,name=nbyte" json:"nbyte,omitempty"`
}

func (m *SizeMetaReply) Reset()                    { *m = SizeMetaReply{} }
func (m *SizeMetaReply) String() string            { return proto.CompactTextString(m) }
func (*SizeMetaReply) ProtoMessage()               {}
func (*SizeMetaReply) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{15} }

func (m *SizeMetaReply) GetNrow() int64 {
	if m != nil {
		return m.Nrow
	}
	return 0
}

func (m *SizeMetaReply) GetNbyte() int64 {
	if m != nil {
		return m.Nbyte
	}
	return 0
}

type PluginSizeMetaReply struct {
	Errcode  int32          `protobuf:"varint,1,opt,name=errcode" json:"errcode,omitempty"`
	Errmsg   string         `protobuf:"bytes,2,opt,name=errmsg" json:"errmsg,omitempty"`
	Sizemeta *SizeMetaReply `protobuf:"bytes,3,opt,name=sizemeta" json:"sizemeta,omitempty"`
}

func (m *PluginSizeMetaReply) Reset()                    { *m = PluginSizeMetaReply{} }
func (m *PluginSizeMetaReply) String() string            { return proto.CompactTextString(m) }
func (*PluginSizeMetaReply) ProtoMessage()               {}
func (*PluginSizeMetaReply) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{16} }

func (m *PluginSizeMetaReply) GetErrcode() int32 {
	if m != nil {
		return m.Errcode
	}
	return 0
}

func (m *PluginSizeMetaReply) GetErrmsg() string {
	if m != nil {
		return m.Errmsg
	}
	return ""
}

func (m *PluginSizeMetaReply) GetSizemeta() *SizeMetaReply {
	if m != nil {
		return m.Sizemeta
	}
	return nil
}

type PageData struct {
	Data []byte `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
}

func (m *PageData) Reset()                    { *m = PageData{} }
func (m *PageData) String() string            { return proto.CompactTextString(m) }
func (*PageData) ProtoMessage()               {}
func (*PageData) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{17} }

func (m *PageData) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

type WriteRequest struct {
	Filespec   *FileSpec     `protobuf:"bytes,1,opt,name=filespec" json:"filespec,omitempty"`
	Columndesc []*ColumnDesc `protobuf:"bytes,2,rep,name=columndesc" json:"columndesc,omitempty"`
	Page       []*PageData   `protobuf:"bytes,3,rep,name=page" json:"page,omitempty"`
}

func (m *WriteRequest) Reset()                    { *m = WriteRequest{} }
func (m *WriteRequest) String() string            { return proto.CompactTextString(m) }
func (*WriteRequest) ProtoMessage()               {}
func (*WriteRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{18} }

func (m *WriteRequest) GetFilespec() *FileSpec {
	if m != nil {
		return m.Filespec
	}
	return nil
}

func (m *WriteRequest) GetColumndesc() []*ColumnDesc {
	if m != nil {
		return m.Columndesc
	}
	return nil
}

func (m *WriteRequest) GetPage() []*PageData {
	if m != nil {
		return m.Page
	}
	return nil
}

type PluginWriteRequest struct {
	Filespec   *FileSpec     `protobuf:"bytes,1,opt,name=filespec" json:"filespec,omitempty"`
	Columndesc []*ColumnDesc `protobuf:"bytes,2,rep,name=columndesc" json:"columndesc,omitempty"`
	Rowset     *XRowSet      `protobuf:"bytes,3,opt,name=rowset" json:"rowset,omitempty"`
}

func (m *PluginWriteRequest) Reset()                    { *m = PluginWriteRequest{} }
func (m *PluginWriteRequest) String() string            { return proto.CompactTextString(m) }
func (*PluginWriteRequest) ProtoMessage()               {}
func (*PluginWriteRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{19} }

func (m *PluginWriteRequest) GetFilespec() *FileSpec {
	if m != nil {
		return m.Filespec
	}
	return nil
}

func (m *PluginWriteRequest) GetColumndesc() []*ColumnDesc {
	if m != nil {
		return m.Columndesc
	}
	return nil
}

func (m *PluginWriteRequest) GetRowset() *XRowSet {
	if m != nil {
		return m.Rowset
	}
	return nil
}

type WriteReply struct {
}

func (m *WriteReply) Reset()                    { *m = WriteReply{} }
func (m *WriteReply) String() string            { return proto.CompactTextString(m) }
func (*WriteReply) ProtoMessage()               {}
func (*WriteReply) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{20} }

type PluginWriteReply struct {
	Errcode int32  `protobuf:"varint,1,opt,name=errcode" json:"errcode,omitempty"`
	Errmsg  string `protobuf:"bytes,2,opt,name=errmsg" json:"errmsg,omitempty"`
}

func (m *PluginWriteReply) Reset()                    { *m = PluginWriteReply{} }
func (m *PluginWriteReply) String() string            { return proto.CompactTextString(m) }
func (*PluginWriteReply) ProtoMessage()               {}
func (*PluginWriteReply) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{21} }

func (m *PluginWriteReply) GetErrcode() int32 {
	if m != nil {
		return m.Errcode
	}
	return 0
}

func (m *PluginWriteReply) GetErrmsg() string {
	if m != nil {
		return m.Errmsg
	}
	return ""
}

func init() {
	proto.RegisterType((*ColumnDesc)(nil), "xdrive.ColumnDesc")
	proto.RegisterType((*Filter)(nil), "xdrive.Filter")
	proto.RegisterType((*CSVSpec)(nil), "xdrive.CSVSpec")
	proto.RegisterType((*FileSpec)(nil), "xdrive.FileSpec")
	proto.RegisterType((*StringList)(nil), "xdrive.StringList")
	proto.RegisterType((*KeyValue)(nil), "xdrive.KeyValue")
	proto.RegisterType((*KeyValueList)(nil), "xdrive.KeyValueList")
	proto.RegisterType((*RmgrInfo)(nil), "xdrive.RmgrInfo")
	proto.RegisterType((*ReadRequest)(nil), "xdrive.ReadRequest")
	proto.RegisterType((*SampleRequest)(nil), "xdrive.SampleRequest")
	proto.RegisterType((*DataReply)(nil), "xdrive.DataReply")
	proto.RegisterType((*XCol)(nil), "xdrive.XCol")
	proto.RegisterType((*XRowSet)(nil), "xdrive.XRowSet")
	proto.RegisterType((*PluginDataReply)(nil), "xdrive.PluginDataReply")
	proto.RegisterType((*SizeMetaRequest)(nil), "xdrive.SizeMetaRequest")
	proto.RegisterType((*SizeMetaReply)(nil), "xdrive.SizeMetaReply")
	proto.RegisterType((*PluginSizeMetaReply)(nil), "xdrive.PluginSizeMetaReply")
	proto.RegisterType((*PageData)(nil), "xdrive.PageData")
	proto.RegisterType((*WriteRequest)(nil), "xdrive.WriteRequest")
	proto.RegisterType((*PluginWriteRequest)(nil), "xdrive.PluginWriteRequest")
	proto.RegisterType((*WriteReply)(nil), "xdrive.WriteReply")
	proto.RegisterType((*PluginWriteReply)(nil), "xdrive.PluginWriteReply")
	proto.RegisterEnum("xdrive.SpqType", SpqType_name, SpqType_value)
}

func init() { proto.RegisterFile("xdrive_data.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 1044 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0xcc, 0x56, 0x5f, 0x6f, 0xe3, 0x44,
	0x10, 0xc7, 0xb1, 0xe3, 0x38, 0x93, 0xdc, 0x9d, 0x6f, 0x29, 0x87, 0x41, 0xa8, 0x44, 0x16, 0x3a,
	0x02, 0x42, 0x15, 0x97, 0x56, 0x15, 0x3c, 0xf6, 0x92, 0x56, 0x0a, 0x97, 0x36, 0xd5, 0x3a, 0xf7,
	0x87, 0xa7, 0xca, 0xe7, 0x6c, 0x52, 0xab, 0x8e, 0xed, 0xae, 0x37, 0x29, 0xe9, 0x53, 0xa5, 0x43,
	0x42, 0x7c, 0x81, 0x7b, 0xe5, 0x91, 0xcf, 0xc0, 0x77, 0xe0, 0x9b, 0xf0, 0xc8, 0x17, 0x40, 0x3b,
	0xde, 0x4d, 0xd3, 0x8a, 0x93, 0x80, 0x87, 0x8a, 0xb7, 0xf9, 0xcd, 0xfc, 0x3c, 0xf3, 0x9b, 0xd9,
	0xd9, 0x4d, 0xe0, 0xe1, 0x0f, 0x63, 0x1e, 0x2f, 0xd8, 0xc9, 0x38, 0x14, 0xe1, 0x56, 0xce, 0x33,
	0x91, 0x11, 0xbb, 0x74, 0xf9, 0x3b, 0x00, 0xdd, 0x2c, 0x99, 0xcf, 0xd2, 0x1e, 0x2b, 0x22, 0x42,
	0xc0, 0x4a, 0xc3, 0x19, 0xf3, 0x8c, 0x96, 0xd1, 0xae, 0x53, 0xb4, 0xa5, 0x4f, 0x2c, 0x73, 0xe6,
	0x55, 0x5a, 0x46, 0xbb, 0x4a, 0xd1, 0xf6, 0x7b, 0x60, 0x1f, 0xc4, 0x89, 0x60, 0x9c, 0xdc, 0x87,
	0x4a, 0x96, 0x2b, 0x7e, 0x25, 0xcb, 0xc9, 0x23, 0xb0, 0x23, 0xcc, 0x87, 0xfc, 0x3a, 0x55, 0x48,
	0x66, 0x09, 0xf9, 0xb4, 0xf0, 0xcc, 0x96, 0x29, 0x33, 0x4b, 0xdb, 0xff, 0xc9, 0x80, 0x5a, 0x37,
	0x78, 0x11, 0xe4, 0x2c, 0x22, 0x9f, 0x40, 0x7d, 0xcc, 0x92, 0x78, 0x16, 0x0b, 0xc6, 0x55, 0xba,
	0x6b, 0x07, 0xf1, 0xa0, 0x96, 0xce, 0x93, 0xa4, 0x10, 0x5c, 0xa5, 0xd5, 0x50, 0xd6, 0x3b, 0x65,
	0xe1, 0x98, 0x71, 0xcf, 0x6c, 0x19, 0x6d, 0x87, 0x2a, 0x44, 0x36, 0xa0, 0x7a, 0x3e, 0xcf, 0x04,
	0xf3, 0x2c, 0xe4, 0x97, 0x40, 0xb2, 0x59, 0x11, 0x85, 0x39, 0xf3, 0xaa, 0xa5, 0xba, 0x12, 0xf9,
	0x21, 0x38, 0x07, 0x71, 0xc2, 0x50, 0x09, 0x01, 0x2b, 0x0f, 0xc5, 0xa9, 0x9e, 0x81, 0xb4, 0xe5,
	0x77, 0x93, 0x8c, 0xcf, 0x42, 0xa1, 0xbb, 0x2a, 0x11, 0xf9, 0x02, 0x6a, 0x51, 0xb1, 0x28, 0x72,
	0x16, 0x61, 0xf9, 0x46, 0xe7, 0xc1, 0x56, 0x39, 0xd7, 0x2d, 0xd5, 0x17, 0xd5, 0x71, 0x7f, 0x13,
	0x20, 0x10, 0x3c, 0x4e, 0xa7, 0x83, 0xb8, 0x10, 0xc4, 0x05, 0x53, 0x36, 0x63, 0xe0, 0x34, 0xa4,
	0xe9, 0x77, 0xc0, 0x79, 0xc6, 0x96, 0x2f, 0xc2, 0x64, 0xce, 0x64, 0xf4, 0x8c, 0x2d, 0x95, 0x02,
	0x69, 0xca, 0x76, 0x16, 0x32, 0xa4, 0xea, 0x97, 0xc0, 0xff, 0x1a, 0x9a, 0xfa, 0x1b, 0xcc, 0xda,
	0x82, 0xca, 0xd9, 0x02, 0x93, 0x36, 0x3a, 0xae, 0x56, 0xa2, 0x19, 0xb4, 0x72, 0xb6, 0xf0, 0xdf,
	0x1a, 0xe0, 0xd0, 0xd9, 0x94, 0xf7, 0xd3, 0x49, 0x26, 0xbb, 0x2a, 0xa2, 0x53, 0xb6, 0x3a, 0x6f,
	0x85, 0xde, 0xd9, 0xed, 0x06, 0x54, 0x39, 0x8e, 0xc6, 0x2c, 0x45, 0x20, 0x20, 0x6d, 0xb0, 0xa2,
	0x2c, 0x9d, 0xe0, 0xa0, 0x1b, 0x9d, 0x8d, 0xdb, 0x65, 0xa5, 0x30, 0x8a, 0x0c, 0xf2, 0x31, 0x38,
	0x79, 0x32, 0x9f, 0xc6, 0x69, 0x96, 0xab, 0xf9, 0xaf, 0xb0, 0xff, 0x87, 0x01, 0x0d, 0xca, 0xc2,
	0x31, 0x65, 0xe7, 0x73, 0x56, 0x08, 0xf2, 0x15, 0x38, 0x93, 0x38, 0x61, 0x38, 0x5a, 0x03, 0x33,
	0xaf, 0x1a, 0xd2, 0x27, 0x45, 0x57, 0x0c, 0xd2, 0x01, 0x28, 0xf7, 0x6c, 0xcc, 0x8a, 0xc8, 0xab,
	0xe0, 0x00, 0xc8, 0xea, 0x28, 0x56, 0xfb, 0x4d, 0xd7, 0x58, 0x64, 0x53, 0x7f, 0x93, 0xc4, 0x85,
	0x50, 0x7b, 0xb9, 0xe6, 0x21, 0x8f, 0xc1, 0x9e, 0xe0, 0x8e, 0x7b, 0x16, 0xe6, 0xbb, 0xbf, 0x56,
	0x5f, 0x30, 0x4e, 0x55, 0x94, 0x7c, 0x08, 0xb5, 0x09, 0x0f, 0xa7, 0x27, 0xf1, 0x18, 0x9b, 0xaa,
	0x52, 0x5b, 0xc2, 0xfe, 0x98, 0x7c, 0x04, 0x0e, 0x06, 0xa2, 0x54, 0x78, 0x36, 0x46, 0x90, 0xd8,
	0x4d, 0x85, 0xff, 0x9b, 0x01, 0xf7, 0x82, 0x70, 0x96, 0x27, 0xec, 0xee, 0xfa, 0x5d, 0xd3, 0x69,
	0xbe, 0x53, 0xa7, 0x75, 0x43, 0x27, 0xbe, 0x07, 0x3c, 0xbb, 0x50, 0x8d, 0xa1, 0xed, 0x7f, 0x0a,
	0xf5, 0x5e, 0x28, 0x42, 0xca, 0xf2, 0x64, 0x29, 0x09, 0xf2, 0x51, 0x41, 0xc9, 0x4d, 0x8a, 0xb6,
	0xff, 0xbb, 0x01, 0xd6, 0xab, 0x6e, 0x96, 0xc8, 0x5b, 0x1b, 0x65, 0xc9, 0xda, 0x83, 0xa2, 0xe1,
	0x2a, 0x6f, 0xe5, 0x3a, 0xaf, 0xbe, 0xe3, 0xb3, 0x30, 0xc7, 0xc3, 0x70, 0xa8, 0x86, 0x72, 0xef,
	0x0a, 0xac, 0x62, 0xe1, 0x21, 0x95, 0x40, 0xf2, 0xe3, 0xed, 0x0e, 0xfa, 0xab, 0x2d, 0x53, 0xaa,
	0x56, 0x10, 0x23, 0xbb, 0x3b, 0x18, 0xb1, 0x5b, 0x66, 0xdb, 0xa4, 0x1a, 0xca, 0xc8, 0x44, 0x7d,
	0x53, 0x6b, 0x99, 0xed, 0x0a, 0xd5, 0x10, 0x23, 0xea, 0x1b, 0xa7, 0x65, 0xb6, 0x0d, 0xaa, 0xa1,
	0xff, 0x3d, 0xd4, 0x5e, 0xd1, 0xec, 0x22, 0x60, 0x78, 0x6b, 0x45, 0x38, 0xc5, 0x66, 0xaa, 0x54,
	0x9a, 0x78, 0x25, 0xb2, 0x79, 0x3a, 0x56, 0x9d, 0x94, 0x80, 0x3c, 0xc6, 0xc6, 0xe7, 0xb3, 0xb4,
	0x7c, 0xef, 0x1a, 0x9d, 0xa6, 0x3e, 0x1b, 0x39, 0x17, 0xaa, 0x83, 0x7e, 0x02, 0x0f, 0x8e, 0xf1,
	0x02, 0x5c, 0x0f, 0xd4, 0x83, 0x1a, 0xe3, 0x3c, 0xca, 0xc6, 0x4c, 0x95, 0xd1, 0x10, 0xdf, 0x2e,
	0xce, 0x67, 0xc5, 0x54, 0xdf, 0xca, 0x12, 0x91, 0xcf, 0xc1, 0xe6, 0xd9, 0x45, 0xc1, 0xc4, 0xed,
	0x27, 0x48, 0xa9, 0xa6, 0x2a, 0xec, 0xff, 0x6a, 0xc0, 0x83, 0x20, 0xbe, 0x64, 0x87, 0x4c, 0x16,
	0xfb, 0x1f, 0xaf, 0x9d, 0xff, 0x2d, 0xdc, 0xbb, 0x16, 0xaa, 0xd6, 0x0c, 0xf7, 0x45, 0x4a, 0x34,
	0xd5, 0xbe, 0x6c, 0x40, 0x35, 0x7d, 0xbd, 0x14, 0xe5, 0x93, 0x68, 0xd2, 0x12, 0xf8, 0x97, 0xf0,
	0x7e, 0x39, 0xd2, 0x9b, 0x09, 0xfe, 0xfd, 0x58, 0x9f, 0x80, 0x53, 0xc4, 0x97, 0x6c, 0xc6, 0x44,
	0xa8, 0x06, 0xfb, 0x81, 0xee, 0xf4, 0x46, 0x6a, 0xba, 0xa2, 0xf9, 0x9b, 0xe0, 0x1c, 0x87, 0x53,
	0x26, 0x0f, 0xf3, 0x6f, 0x2f, 0xc6, 0x5b, 0x03, 0x9a, 0x2f, 0x79, 0x2c, 0xee, 0xf0, 0xd2, 0x7f,
	0x26, 0x7f, 0xcc, 0xa6, 0x4c, 0xad, 0xe1, 0x2a, 0xbb, 0x96, 0x49, 0x31, 0xea, 0xff, 0x62, 0x00,
	0x29, 0xa7, 0x76, 0xc7, 0xf2, 0xfe, 0xf1, 0xee, 0x36, 0x01, 0x94, 0xb4, 0x3c, 0x59, 0xfa, 0x3d,
	0x70, 0x6f, 0xc8, 0xfd, 0x4f, 0x27, 0xfc, 0xe5, 0x9f, 0x06, 0xd4, 0x82, 0xfc, 0x7c, 0xb4, 0xcc,
	0x19, 0x69, 0x40, 0xed, 0xf9, 0xd1, 0xb3, 0xa3, 0xe1, 0xcb, 0x23, 0xf7, 0x3d, 0xe2, 0x80, 0xf5,
	0x74, 0x38, 0x1c, 0xb8, 0x06, 0xa9, 0x43, 0xb5, 0x7f, 0x34, 0x7a, 0xb2, 0xeb, 0x56, 0x94, 0xb9,
	0xdd, 0x71, 0x4d, 0x65, 0xee, 0xee, 0xb8, 0x16, 0x01, 0xb0, 0x25, 0xa1, 0xf3, 0x8d, 0x5b, 0x95,
	0xee, 0x83, 0xc1, 0x70, 0x6f, 0xe4, 0xda, 0xd2, 0xdd, 0x1b, 0x3e, 0x7f, 0x3a, 0xd8, 0x77, 0x6b,
	0x32, 0x5b, 0x37, 0x18, 0x51, 0xb7, 0x2e, 0x09, 0xbd, 0xfd, 0xee, 0xee, 0x8e, 0x0b, 0x48, 0xd8,
	0xef, 0xca, 0xef, 0x1a, 0x04, 0xc0, 0xea, 0xed, 0x8d, 0xf6, 0xdd, 0x37, 0x57, 0x16, 0x79, 0x08,
	0x8d, 0x51, 0xff, 0x70, 0xff, 0xe4, 0xb0, 0x3f, 0x18, 0xf4, 0x03, 0xf7, 0xcd, 0x95, 0x43, 0x1e,
	0x81, 0x2b, 0x5d, 0xc1, 0x68, 0xef, 0xf0, 0x58, 0xfb, 0x7f, 0xbc, 0x6a, 0xae, 0x51, 0xbb, 0x74,
	0x28, 0x5d, 0xee, 0x6d, 0xaa, 0xf2, 0x6f, 0xc8, 0x0a, 0xdf, 0x05, 0xc3, 0x23, 0xf7, 0xe7, 0x2b,
	0xef, 0xb5, 0x8d, 0xff, 0xff, 0xb6, 0xff, 0x0a, 0x00, 0x00, 0xff, 0xff, 0x65, 0x0a, 0x30, 0x0a,
	0x14, 0x0a, 0x00, 0x00,
}