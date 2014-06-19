package datatable

import (
	"fmt"
	"reflect"
	"time"
)

func safeToStrings(s ...interface{}) []string {
	r := make([]string, len(s), len(s))
	for i, v := range s {
		r[i] = safeToString(v)
	}
	return r
}
func safeToString(s interface{}) string {

	if s == nil {
		return ""
	}
	switch r := s.(type) {
	case string:
		return r
	case []byte:
		return string(r)
	default:
		return fmt.Sprintf("%v", s)
	}

}

//0-equ -1 less 1 large
func cmpValue(v1, v2 interface{}) int {
	switch v1.(type) {
	case []interface{}:
		for i, e1 := range v1.([]interface{}) {
			if i >= len(v2.([]interface{})) {
				return 1
			}
			oneCmp := cmpValue(e1, v2.([]interface{})[i])
			if oneCmp == 0 {
				continue
			}
			return oneCmp
		}
		if len(v1.([]interface{})) == len(v2.([]interface{})) {
			return 0
		}
		return -1
	case string:
		if v1.(string) == v2.(string) {
			return 0
		}
		if v1.(string) < v2.(string) {
			return -1
		}
		return 1
	case bool:
		if v1.(bool) && v2.(bool) || !v1.(bool) && !v2.(bool) {
			return 0
		}
		if !v1.(bool) && v2.(bool) {
			return -1
		}
		return 1
	case float64:
		if v1.(float64) == v2.(float64) {
			return 0
		}
		if v1.(float64) < v2.(float64) {
			return -1
		}
		return 1
	case byte:
		if v1.(byte) == v2.(byte) {
			return 0
		}
		if v1.(byte) < v2.(byte) {
			return -1
		}
		return 1
	case int64:
		if v1.(int64) == v2.(int64) {
			return 0
		}
		if v1.(int64) < v2.(int64) {
			return -1
		}
		return 1
	case int:
		if v1.(int) == v2.(int) {
			return 0
		}
		if v1.(int) < v2.(int) {
			return -1
		}
		return 1
	case time.Time:
		if v1.(time.Time).Equal(v2.(time.Time)) {
			return 0
		}
		if v1.(time.Time).Before(v2.(time.Time)) {
			return -1
		}
		return 1
	case []byte:
		return cmpBytea(v1.([]byte), v2.([]byte))
	case []string:
		return cmpStringSlice(v1.([]string), v2.([]string))
	case []bool:
		return cmpBoolSlice(v1.([]bool), v2.([]bool))
	case []float64:
		return cmpFloat64Slice(v1.([]float64), v2.([]float64))
	case []float32:
		return cmpFloat32Slice(v1.([]float32), v2.([]float32))
	case []int64:
		return cmpInt64Slice(v1.([]int64), v2.([]int64))
	case []int:
		return cmpIntSlice(v1.([]int), v2.([]int))
	case []time.Time:
		return cmpTimeSlice(v1.([]time.Time), v2.([]time.Time))
	case [][]byte:
		return cmpByteaSlice(v1.([][]byte), v2.([][]byte))
	default:
		panic(PrimaryKeyTypeError(reflect.TypeOf(v1).String()))
	}
}

func cmpStringSlice(v1, v2 []string) int {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return 1
		}
		oneCmp := cmpValue(e1, v2[i])
		if oneCmp == 0 {
			continue
		}
		return oneCmp
	}
	if len(v1) == len(v2) {
		return 0
	}
	return -1
}
func cmpBoolSlice(v1, v2 []bool) int {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return 1
		}
		oneCmp := cmpValue(e1, v2[i])
		if oneCmp == 0 {
			continue
		}
		return oneCmp
	}
	if len(v1) == len(v2) {
		return 0
	}
	return -1
}
func cmpIntSlice(v1, v2 []int) int {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return 1
		}
		oneCmp := cmpValue(e1, v2[i])
		if oneCmp == 0 {
			continue
		}
		return oneCmp
	}
	if len(v1) == len(v2) {
		return 0
	}
	return -1
}
func cmpInt64Slice(v1, v2 []int64) int {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return 1
		}
		oneCmp := cmpValue(e1, v2[i])
		if oneCmp == 0 {
			continue
		}
		return oneCmp
	}
	if len(v1) == len(v2) {
		return 0
	}
	return -1
}
func cmpFloat64Slice(v1, v2 []float64) int {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return 1
		}
		oneCmp := cmpValue(e1, v2[i])
		if oneCmp == 0 {
			continue
		}
		return oneCmp
	}
	if len(v1) == len(v2) {
		return 0
	}
	return -1
}
func cmpFloat32Slice(v1, v2 []float32) int {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return 1
		}
		oneCmp := cmpValue(e1, v2[i])
		if oneCmp == 0 {
			continue
		}
		return oneCmp
	}
	if len(v1) == len(v2) {
		return 0
	}
	return -1
}
func cmpTimeSlice(v1, v2 []time.Time) int {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return 1
		}
		oneCmp := cmpValue(e1, v2[i])
		if oneCmp == 0 {
			continue
		}
		return oneCmp
	}
	if len(v1) == len(v2) {
		return 0
	}
	return -1
}
func cmpBytea(v1, v2 []byte) int {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return 1
		}
		oneCmp := cmpValue(e1, v2[i])
		if oneCmp == 0 {
			continue
		}
		return oneCmp
	}
	if len(v1) == len(v2) {
		return 0
	}
	return -1
}
func cmpByteaSlice(v1, v2 [][]byte) int {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return 1
		}
		oneCmp := cmpValue(e1, v2[i])
		if oneCmp == 0 {
			continue
		}
		return oneCmp
	}
	if len(v1) == len(v2) {
		return 0
	}
	return -1
}
