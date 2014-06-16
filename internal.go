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
func lessSlices(v1, v2 []interface{}) bool {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return false
		}
		e2 := v2[i]
		//if reflect.DeepEqual(e1, e2) {
		if e1 == e2 {
			continue
		} else {
			return less(e1, e2)
		}
	}
	return len(v1) < len(v2)
}
func lessStringSlice(v1, v2 []string) bool {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return false
		}
		e2 := v2[i]
		if e1 == e2 {
			continue
		} else {
			return e1 < e2
		}
	}
	return len(v1) < len(v2)
}
func lessBoolSlice(v1, v2 []bool) bool {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return false
		}
		e2 := v2[i]
		if e1 == e2 {
			continue
		} else {
			return !e1 && e2
		}
	}
	return len(v1) < len(v2)
}
func lessIntSlice(v1, v2 []int) bool {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return false
		}
		e2 := v2[i]
		if e1 == e2 {
			continue
		} else {
			return e1 < e2
		}
	}
	return len(v1) < len(v2)
}
func lessInt64Slice(v1, v2 []int64) bool {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return false
		}
		e2 := v2[i]
		if e1 == e2 {
			continue
		} else {
			return e1 < e2
		}
	}
	return len(v1) < len(v2)
}
func lessFloat64Slice(v1, v2 []float64) bool {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return false
		}
		e2 := v2[i]
		if e1 == e2 {
			continue
		} else {
			return e1 < e2
		}
	}
	return len(v1) < len(v2)
}
func lessFloat32Slice(v1, v2 []float32) bool {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return false
		}
		e2 := v2[i]
		if e1 == e2 {
			continue
		} else {
			return e1 < e2
		}
	}
	return len(v1) < len(v2)
}
func lessTimeSlice(v1, v2 []time.Time) bool {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return false
		}
		e2 := v2[i]
		if e1 == e2 {
			continue
		} else {
			return e1.Before(e2)
		}
	}
	return len(v1) < len(v2)
}
func lessBytea(v1, v2 []byte) bool {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return false
		}
		e2 := v2[i]
		if e1 == e2 {
			continue
		} else {
			return e1 < e2
		}
	}
	return len(v1) < len(v2)
}
func equBytea(v1, v2 []byte) bool {
	if len(v1) != len(v2) {
		return false
	}
	for i := 0; i < len(v1); i++ {
		if v1[i] != v2[i] {
			return false
		}
	}
	return true
}
func lessByteaSlice(v1, v2 [][]byte) bool {
	for i, e1 := range v1 {
		if i >= len(v2) {
			return false
		}
		e2 := v2[i]
		if equBytea(e1, e2) {
			continue
		} else {
			return lessBytea(e1, e2)
		}
	}
	return len(v1) < len(v2)
}
func less(v1, v2 interface{}) bool {
	switch v1.(type) {
	case string:
		return v1.(string) < v2.(string)
	case bool:
		return !v1.(bool) && v2.(bool)
	case float64:
		return v1.(float64) < v2.(float64)
	case int64:
		return v1.(int64) < v2.(int64)
	case int:
		return v1.(int) < v2.(int)
	case time.Time:
		return v1.(time.Time).Before(v2.(time.Time))
	case []byte:
		return lessBytea(v1.([]byte), v2.([]byte))
	case []string:
		return lessStringSlice(v1.([]string), v2.([]string))
	case []bool:
		return lessBoolSlice(v1.([]bool), v2.([]bool))
	case []float64:
		return lessFloat64Slice(v1.([]float64), v2.([]float64))
	case []float32:
		return lessFloat32Slice(v1.([]float32), v2.([]float32))
	case []int64:
		return lessInt64Slice(v1.([]int64), v2.([]int64))
	case []int:
		return lessIntSlice(v1.([]int), v2.([]int))
	case []time.Time:
		return lessTimeSlice(v1.([]time.Time), v2.([]time.Time))
	case [][]byte:
		return lessByteaSlice(v1.([][]byte), v2.([][]byte))
	default:
		panic(PrimaryKeyTypeError(reflect.TypeOf(v1).String()))
	}
}
