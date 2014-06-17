package datatable

import (
	"errors"
	"math"
	"reflect"
	"time"
)

var DataTypeError = errors.New("datatype error")

type DataColumn struct {
	index        int
	Name         string
	DataType     reflect.Type
	ReversalNull bool //when retrieving data that auto convert the NullValue to nil,default is true
	NullValue    interface{}
}

func (d *DataColumn) Index() int {
	return d.index
}
func (d *DataColumn) Clone() *DataColumn {
	result := DataColumn{}
	result = *d
	return &result
}

func NewDataColumn(name string, dataType reflect.Type) *DataColumn {
	c := &DataColumn{Name: name, DataType: dataType, ReversalNull: true}
	switch dataType.Kind() {
	case reflect.String:
		c.NullValue = ""
	case reflect.Float32:
		c.NullValue = math.SmallestNonzeroFloat32
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
func NewJSONColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf(map[string]interface{}{}))
}

func NewStringArrayColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf([]string{}))
}
func NewFloat64ArrayColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf([]float64{}))
}
func NewInt64ArrayColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf([]int64{}))
}

func NewBoolArrayColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf([]bool{}))
}
func NewByteaArrayColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf([][]byte{}))
}
func NewTimeArrayColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf([]time.Time{}))
}
func NewJSONArrayColumn(name string) *DataColumn {
	return NewDataColumn(name, reflect.TypeOf([]map[string]interface{}{}))
}
