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
	dataType reflect.Type `json:"-"`
	Name     string
	NotNull  bool
	MaxSize  int
}

func (d *DataColumn) Index() int {
	return d.index
}

//alloc empty value,return pointer the value
func (d *DataColumn) PtrZeroValue() interface{} {
	defer func() {
		if f := recover(); f != nil {
			panic(fmt.Sprintf("%s,type:%s", f, d.DataType()))
		}
	}()
	if d.NotNull {
		return reflect.New(d.DataType()).Interface()
	} else {
		return PtrNilValue
	}

}
func (d *DataColumn) Valid(value interface{}) error {
	if d.NotNull || value != nil {
		if !reflect.DeepEqual(reflect.TypeOf(value), d.dataType) {
			return fmt.Errorf("the value %v(%T) not is type %s", value, value, d.dataType.String())
		}
	}
	if value != nil && d.MaxSize > 0 && d.dataType.Kind() == reflect.String && len(value.(string)) > d.MaxSize {
		return fmt.Errorf("the value %q(%T) length %d > maxsize(%d)", value, value, len(value.(string)), d.MaxSize)
	}
	return nil
}
func (d *DataColumn) ZeroValue() interface{} {
	defer func() {
		if f := recover(); f != nil {
			panic(fmt.Sprintf("%s,type:%s", f, d.DataType()))
		}
	}()
	if d.NotNull {
		return reflect.New(d.DataType()).Elem().Interface()
	} else {
		return NilValue
	}
}
func (d *DataColumn) Clone() *DataColumn {
	result := DataColumn{}
	result = *d
	return &result
}
func (d *DataColumn) DataType() reflect.Type {
	if d.NotNull {
		return d.dataType
	} else {
		return InterfaceType
	}
}

func NewDataColumn(name string, dataType reflect.Type) *DataColumn {
	return &DataColumn{Name: name, dataType: dataType, NotNull: true}
}
func NewDataColumnN(name string, dataType reflect.Type) *DataColumn {
	return &DataColumn{Name: name, dataType: dataType, NotNull: false}
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
