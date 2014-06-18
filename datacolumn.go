package datatable

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

var DataTypeError = errors.New("datatype error")

type DataColumn struct {
	index    int
	Name     string
	DataType reflect.Type
}

func (d *DataColumn) Index() int {
	return d.index
}

//alloc empty value,return pointer the value
func (d *DataColumn) PtrZeroValue() interface{} {
	defer func() {
		if f := recover(); f != nil {
			panic(fmt.Sprintf("%s,type:%s", f, d.DataType))
		}
	}()
	return reflect.New(d.DataType).Interface()
}
func (d *DataColumn) ZeroValue() interface{} {
	defer func() {
		if f := recover(); f != nil {
			panic(fmt.Sprintf("%s,type:%s", f, d.DataType))
		}
	}()
	return reflect.New(d.DataType).Elem().Interface()
}
func (d *DataColumn) Clone() *DataColumn {
	result := DataColumn{}
	result = *d
	return &result
}

func NewDataColumn(name string, dataType reflect.Type) *DataColumn {
	return &DataColumn{Name: name, DataType: dataType}
}
func NewStringColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf(string("")))
}
func NewFloat64Column(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf(float64(0)))
}
func NewInt64Column(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf(int64(0)))
}
func NewIntColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf(int(0)))
}
func NewBoolColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf(true))
}
func NewByteaColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf([]byte{}))
}
func NewTimeColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf(time.Now()))
}
