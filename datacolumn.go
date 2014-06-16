package datatable

import (
	"errors"
	"math"
	"reflect"
	"time"
)

var DataTypeError = errors.New("datatype error")

type DataColumn struct {
	index     int
	Name      string
	DataType  reflect.Type
	NullValue interface{}
	MaxSize   int
}

func (d *DataColumn) Clone() *DataColumn {
	result := DataColumn{}
	result = *d
	return &result
}
func (d *DataColumn) Object() map[string]interface{} {
	return map[string]interface{}{
		"index":    d.index,
		"Name":     d.Name,
		"DataType": d.DataType.String(),
		"MaxSize":  d.MaxSize,
	}
}

func NewDataColumn(name string, dataType reflect.Type, maxSize int) *DataColumn {
	c := &DataColumn{Name: name, DataType: dataType, MaxSize: maxSize}
	switch dataType.Kind() {
	case reflect.String:
		c.NullValue = ""
	case reflect.Float64:
		c.NullValue = math.SmallestNonzeroFloat64
	case reflect.Int64:
		c.NullValue = int64(math.MinInt64)
	case reflect.Int:
		c.NullValue = math.MinInt32
	default:
		if reflect.DeepEqual(dataType, reflect.TypeOf(time.Now())) {
			c.NullValue = time.Unix(0, 0)
		}
	}
	return c

}
func NewStringColumn(name string, maxSize int) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf(string("")), maxSize)
}
func NewFloat64Column(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf(float64(0)), 0)
}
func NewInt64Column(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf(int64(0)), 0)
}
func NewIntColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf(int(0)), 0)
}
func NewBoolColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf(true), 0)
}
func NewByteaColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf([]byte{}), 0)
}
func NewTimeColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf(time.Now()), 0)
}
func NewJSONColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf(map[string]interface{}{}), 0)
}

func NewStringArrayColumn(name string, maxSize int) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf([]string{}), maxSize)
}
func NewFloat64ArrayColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf([]float64{}), 0)
}
func NewInt64ArrayColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf([]int64{}), 0)
}

func NewBoolArrayColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf([]bool{}), 0)
}
func NewByteaArrayColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf([][]byte{}), 0)
}
func NewTimeArrayColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf([]time.Time{}), 0)
}
func NewJSONArrayColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf([]map[string]interface{}{}), 0)
}
