package datatype

import (
	_ "fmt"
)

const (
	Name = iota
	SqlName
	GoOptName
	XDrColName
)

type OptionalType interface {
	Names() []string
}

type OptionalBool struct {
	v  bool
	ok bool
}

func (o *OptionalBool) Names() []string {
	return []string{"bool", "bool", "OptionalBool", "I32Data"}
}

func (o *OptionalBool) Get() (bool, bool) {
	return o.v, o.ok
}

func (o *OptionalBool) Set(v bool) {
	o.v = v
	o.ok = true
}

func (o *OptionalBool) SetNull() {
	o.ok = false
}

type OptionalInt32 struct {
	v  int32
	ok bool
}

func (o *OptionalInt32) Names() []string {
	return []string{"int32", "int", "OptionalInt32", "I32Data"}
}

func (o *OptionalInt32) Get() (int32, bool) {
	return o.v, o.ok
}

func (o *OptionalInt32) Set(v int32) {
	o.v = v
	o.ok = true
}

func (o *OptionalInt32) SetNull() {
	o.ok = false
}

type OptionalInt64 struct {
	v  int64
	ok bool
}

func (o *OptionalInt64) Names() []string {
	return []string{"int64", "bigint", "OptionalInt64", "I64Data"}
}

func (o *OptionalInt64) Get() (int64, bool) {
	return o.v, o.ok
}

func (o *OptionalInt64) Set(v int64) {
	o.v = v
	o.ok = true
}

func (o *OptionalInt64) SetNull() {
	o.ok = false
}

type OptionalFloat32 struct {
	v  float32
	ok bool
}

func (o *OptionalFloat32) Names() []string {
	return []string{"float32", "float4", "OptionalFloat32", "F32Data"}
}

func (o *OptionalFloat32) Get() (float32, bool) {
	return o.v, o.ok
}

func (o *OptionalFloat32) Set(v float32) {
	o.v = v
	o.ok = true
}

func (o *OptionalFloat32) SetNull() {
	o.ok = false
}

type OptionalFloat64 struct {
	v  float64
	ok bool
}

func (o *OptionalFloat64) Names() []string {
	return []string{"float64", "float8", "OptionalFloat64", "F64Data"}
}

func (o *OptionalFloat64) Get() (float64, bool) {
	return o.v, o.ok
}

func (o *OptionalFloat64) Set(v float64) {
	o.v = v
	o.ok = true
}

func (o *OptionalFloat64) SetNull() {
	o.ok = false
}

type OptionalString struct {
	v  string
	ok bool
}

func (o *OptionalString) Names() []string {
	return []string{"string", "text", "OptionalString", "Sdata"}
}

func (o *OptionalString) Get() (string, bool) {
	return o.v, o.ok
}

func (o *OptionalString) Set(v string) {
	o.v = v
	o.ok = true
}

func (o *OptionalString) SetNull() {
	o.v = ""
	o.ok = false
}

func MapType(t string) OptionalType {
	switch t {
	case "bool":
		return new(OptionalBool)
	case "int32":
		return new(OptionalInt32)
	case "int64":
		return new(OptionalInt64)
	case "float32":
		return new(OptionalFloat32)
	case "float64":
		return new(OptionalFloat64)
	case "string":
		return new(OptionalString)
	default:
		return nil
	}
}
