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
	String  ColumnType = "string"
	Int64   ColumnType = "int64"
	Float64 ColumnType = "float64"
	Bool    ColumnType = "bool"
	Time    ColumnType = "time"
	Bytea   ColumnType = "bytea"
)

type ColumnType string
type DataColumn struct {
	index    int
	DataType ColumnType
	Name     string
	MaxSize  int
	NotNull  bool
}

var reflectType map[ColumnType]reflect.Type = map[ColumnType]reflect.Type{
	String:  reflect.TypeOf(""),
	Int64:   reflect.TypeOf(int64(0)),
	Float64: reflect.TypeOf(float64(0)),
	Bool:    reflect.TypeOf(true),
	Time:    reflect.TypeOf(time.Time{}),
	Bytea:   reflect.TypeOf([]byte{})}
var typeNil map[ColumnType]interface{} = map[ColumnType]interface{}{
	String:  (*string)(nil),
	Int64:   (*int64)(nil),
	Float64: (*float64)(nil),
	Bool:    (*bool)(nil),
	Time:    (*time.Time)(nil),
	Bytea:   (*[]byte)(nil)}

func (d *DataColumn) Index() int {
	return d.index
}

func (d *DataColumn) Valid(value interface{}) error {
	if d.NotNull || value != nil {
		if d.DataType == String {
			switch tv := value.(type) {
			case string:
				if d.MaxSize > 0 && len(tv) > d.MaxSize {
					return fmt.Errorf("the value %q(%T) length %d > maxsize(%d)", value, value, len(tv), d.MaxSize)
				} else {
					return nil
				}
			case []byte:
				str := string(tv)
				if d.MaxSize > 0 && len(str) > d.MaxSize {
					return fmt.Errorf("the value %q(%T) length %d > maxsize(%d)", value, value, len(str), d.MaxSize)
				} else {
					return nil
				}
			default:
				return fmt.Errorf("the column %q value %v(%T) not is type %s", d.Name, value, value, d.ReflectType().String())
			}
		} else if !reflect.DeepEqual(reflect.TypeOf(value), d.ReflectType()) {
			return fmt.Errorf("the column %q value %v(%T) not is type %s", d.Name, value, value, d.ReflectType().String())
		}
	}
	if value != nil && d.MaxSize > 0 && d.DataType == String && len(value.(string)) > d.MaxSize {
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
		return reflect.New(d.ReflectType()).Elem().Interface()
	} else {
		return typeNil[d.DataType]
	}
}
func (d *DataColumn) Clone() *DataColumn {
	result := DataColumn{}
	result = *d
	return &result
}
func (d *DataColumn) StoreType() reflect.Type {
	if d.NotNull {
		return reflectType[d.DataType]
	} else {
		return reflect.PtrTo(reflectType[d.DataType])
	}
}
func (d *DataColumn) ReflectType() reflect.Type {
	return reflectType[d.DataType]
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
func (d *DataColumn) Decode(v interface{}) interface{} {
	if d.NotNull {
		return v
	} else {
		switch d.DataType {
		case String:
			if v == (*string)(nil) {
				return nil
			} else {
				return *(v.(*string))
			}
		case Bool:
			if v == (*bool)(nil) {
				return nil
			} else {
				return *(v.(*bool))
			}
		case Int64:
			if v == (*int64)(nil) {
				return nil
			} else {
				return *(v.(*int64))
			}
		case Float64:
			if v == (*float64)(nil) {
				return nil
			} else {
				return *(v.(*float64))
			}
		case Time:
			if v == (*time.Time)(nil) {
				return nil
			} else {
				return *(v.(*time.Time))
			}
		case Bytea:
			if v == (*[]byte)(nil) {
				return nil
			} else {
				return *(v.(*[]byte))
			}
		default:
			panic(fmt.Errorf("column type %q invalid", d.DataType))
		}

	}
}
func (d *DataColumn) Encode(v interface{}) interface{} {
	if d.NotNull {
		return v
	} else {
		if v == nil {
			return typeNil[d.DataType]
		} else {
			switch d.DataType {
			case String:
				switch tv := v.(type) {
				case string:
					return &tv
				case []byte:
					str := string(tv)
					return &str
				default:
					panic(fmt.Errorf("can't convert src --> dest:%T -- > string", v))
				}
			case Bool:
				tv := v.(bool)
				return &tv
			case Int64:
				tv := v.(int64)
				return &tv
			case Float64:
				tv := v.(float64)
				return &tv
			case Time:
				tv := v.(time.Time)
				return &tv
			case Bytea:
				tv := v.([]byte)
				return &tv
			default:
				panic(fmt.Errorf("column type %q invalid", d.DataType))
			}

		}
	}
}
func (d *DataColumn) DecodeString(value string) (interface{}, error) {
	if value == "" {
		if d.NotNull {
			return nil, fmt.Errorf("the column %q can't nullable,the empty string invalid", d.Name)
		}
		return nil, nil
	}
	switch d.DataType {
	case String:
		return value, nil
	case Bytea:
		return decodeHex(value)
	case Int64:
		return strconv.ParseInt(string(value), 10, 64)
	case Float64:
		return strconv.ParseFloat(value, 64)
	case Time:
		return time.Parse(time.RFC3339Nano, value)
	case Bool:
		return strconv.ParseBool(value)
	default:
		return nil, fmt.Errorf("can't convert %q to type %T", value, d.DataType)
	}
}

func NewDataColumn(name string, dataType ColumnType, maxsize int, notnull bool) *DataColumn {
	return &DataColumn{Name: name, DataType: dataType, NotNull: notnull, MaxSize: maxsize}
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
