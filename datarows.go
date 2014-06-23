package datatable

import (
	"reflect"
)

type dataRows struct {
	data []interface{}
}

func (r *dataRows) Merge(src *dataRows) {
	for i, _ := range r.data {
		r.data[i] = reflect.AppendSlice(reflect.ValueOf(r.data[i]), reflect.ValueOf(src.data[i])).Interface()
	}
}
func (r *dataRows) Remove(rowIndex int) {
	for i, _ := range r.data {
		lastIdx := reflect.ValueOf(r.data[i]).Len() - 1
		reflect.ValueOf(r.data[i]).Index(rowIndex).Set(reflect.ValueOf(r.data[i]).Index(lastIdx))
		r.data[i] = reflect.ValueOf(r.data[i]).Slice(0, lastIdx).Interface()
	}
	return
}
func (r *dataRows) Count() int {
	if len(r.data) == 0 {
		return 0
	}
	return reflect.ValueOf(r.data[0]).Len()
}
func (r *dataRows) Get(col, row int) interface{} {
	return reflect.ValueOf(r.data[col]).Index(row).Interface()
}
func (r *dataRows) AddColumn(t reflect.Type) {
	r.data = append(r.data, reflect.MakeSlice(reflect.SliceOf(t), r.Count(), r.Count()).Interface())
	return
}
func (r *dataRows) AddRow(values []interface{}) {
	for i, _ := range r.data {
		r.data[i] = reflect.Append(reflect.ValueOf(r.data[i]), reflect.ValueOf(values[i])).Interface()
	}
	return
}
func (r *dataRows) GetRow(row int) []interface{} {
	var result []interface{}
	for i, _ := range r.data {
		result = append(result, r.Get(i, row))
	}
	return result
}
func (r *dataRows) Set(col, row int, value interface{}) {
	reflect.ValueOf(r.data[col]).Index(row).Set(reflect.ValueOf(value))
}
func (r *dataRows) SetRow(row int, values []interface{}) {
	for i, _ := range r.data {
		r.Set(i, row, values[i])
	}
	return
}
