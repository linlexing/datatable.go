package datatable

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

const (
	String ColumnType = iota
	Int64
	Float64
	Bool
	Time
	Bytea
)

type ColumnType int

func (c ColumnType) String() string {
	switch c {
	case String:
		return "string"
	case Int64:
		return "int64"
	case Float64:
		return "float64"
	case Bool:
		return "bool"
	case Time:
		return "time"
	case Bytea:
		return "bytea"
	default:
		return "error"
	}
}

type DataColumn struct {
	index    int
	dataType ColumnType
	Name     string
	MaxSize  int
	NotNull  bool
}

var reflectType []reflect.Type = []reflect.Type{
	reflect.TypeOf(""),
	reflect.TypeOf(int64(0)),
	reflect.TypeOf(float64(0)),
	reflect.TypeOf(true),
	reflect.TypeOf(time.Time{}),
	reflect.TypeOf([]byte{})}

func (d *DataColumn) Index() int {
	return d.index
}

//alloc empty value,return pointer the value
func (d *DataColumn) PtrZeroValue() interface{} {
	defer func() {
		if f := recover(); f != nil {
			panic(fmt.Sprintf("%s,type:%s", f, d.StoreType()))
		}
	}()
	if d.NotNull {
		return reflect.New(d.StoreType()).Interface()
	} else {
		return PtrNilValue
	}

}
func (d *DataColumn) Valid(value interface{}) error {
	if d.NotNull || value != nil {
		if !reflect.DeepEqual(reflect.TypeOf(value), d.ReflectType()) {
			return fmt.Errorf("the value %v(%T) not is type %s", value, value, d.ReflectType().String())
		}
	}
	if value != nil && d.MaxSize > 0 && d.dataType == String && len(value.(string)) > d.MaxSize {
		return fmt.Errorf("the value %q(%T) length %d > maxsize(%d)", value, value, len(value.(string)), d.MaxSize)
	}
	return nil
}
func (d *DataColumn) ZeroValue() interface{} {
	defer func() {
		if f := recover(); f != nil {
			panic(fmt.Sprintf("%s,type:%s", f, d.StoreType()))
		}
	}()
	if d.NotNull {
		return reflect.New(d.StoreType()).Elem().Interface()
	} else {
		return NilValue
	}
}
func (d *DataColumn) Clone() *DataColumn {
	result := DataColumn{}
	result = *d
	return &result
}
func (d *DataColumn) StoreType() reflect.Type {
	if d.NotNull {
		return reflectType[d.dataType]
	} else {
		return InterfaceType
	}
}
func (d *DataColumn) ReflectType() reflect.Type {
	return reflectType[d.dataType]
}
func (d *DataColumn) EncodeString(value interface{}) string {
	switch tv := value.(type) {
	case nil:
		return ""
	case string:
		return tv
	case []byte:
		if len(tv) == 0 {
			return ""
		}
		return fmt.Sprintf("\\x%x", tv)
	case int64:
		return fmt.Sprint(tv)
	case float64:
		return fmt.Sprintf("%.17f", tv)
	case time.Time:
		return tv.Format(time.RFC3339Nano)
	case bool:
		if tv {
			return "t"
		} else {
			return "f"
		}
	default:
		panic(fmt.Errorf("can't convert %v(%T) to string", tv, tv))
	}
}
func decodeHex(value string) ([]byte, error) {
	if len(value) >= 2 && bytes.Equal([]byte(value)[:2], []byte("\\x")) {
		// bytea_output = hex
		s := []byte(value)[2:] // trim off leading "\\x"
		rev := make([]byte, hex.DecodedLen(len(s)))
		_, result_err := hex.Decode(rev, s)
		if result_err == nil {
			return rev, nil
		} else {
			return nil, result_err
		}
	} else {
		return nil, fmt.Errorf("%s is invalid hex string", value)
	}
}
func (d *DataColumn) DecodeString(value string) (interface{}, error) {
	if value == "" {
		if d.NotNull {
			return nil, fmt.Errorf("the column %q can't nullable,the empty string invalid", d.Name)
		}
		return nil, nil
	}
	switch tv := d.ZeroValue().(type) {
	case string:
		return value, nil
	case []byte:
		return decodeHex(value)
	case int64:
		return strconv.ParseInt(string(value), 10, 64)
	case float64:
		return strconv.ParseFloat(value, 64)
	case time.Time:
		return time.Parse(time.RFC3339Nano, value)
	case bool:
		return strconv.ParseBool(value)
	default:
		return nil, fmt.Errorf("can't convert %q to type %T", value, tv)
	}
}

func NewDataColumn(name string, dataType ColumnType, maxsize int, notnull bool) *DataColumn {
	return &DataColumn{Name: name, dataType: dataType, NotNull: notnull, MaxSize: maxsize}
}

func StringColumn(name string, maxsize int, notnull bool) *DataColumn {
	return NewDataColumn(name, String, maxsize, notnull)
}
func Float64Column(name string, notnull bool) *DataColumn {
	return NewDataColumn(name, Float64, 0, notnull)
}
func Int64Column(name string, notnull bool) *DataColumn {
	return NewDataColumn(name, Int64, 0, notnull)
}
func BoolColumn(name string, notnull bool) *DataColumn {
	return NewDataColumn(name, Bool, 0, notnull)
}
func ByteaColumn(name string, notnull bool) *DataColumn {
	return NewDataColumn(name, Bytea, 0, notnull)
}
func TimeColumn(name string, notnull bool) *DataColumn {
	return NewDataColumn(name, Time, 0, notnull)
}

func NewStringColumn(name string) *DataColumn {
	return NewDataColumn(name, String, 0, true)
}
func NewFloat64Column(name string) *DataColumn {
	return NewDataColumn(name, Float64, 0, true)
}
func NewInt64Column(name string) *DataColumn {
	return NewDataColumn(name, Int64, 0, true)
}
func NewBoolColumn(name string) *DataColumn {
	return NewDataColumn(name, Bool, 0, true)
}
func NewByteaColumn(name string) *DataColumn {
	return NewDataColumn(name, Bytea, 0, true)
}
func NewTimeColumn(name string) *DataColumn {
	return NewDataColumn(name, Time, 0, true)
}
