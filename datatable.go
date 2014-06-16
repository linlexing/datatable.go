// datatable project datatable.go
package datatable

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

const (
	UNCHANGE byte = iota
	UPDATE
	INSERT
	DELETE
)

var (
	ColumnExistsError   = errors.New("the column exists")
	ColumnNotFoundError = errors.New("the column not found")
	RowNotFoundError    = errors.New("the row not found")
	KeyValueExists      = errors.New("the key value aleary exists")
	NotThisTableRow     = errors.New("the row not is this table's row")
)

type ChangeRow struct {
	Data       []interface{}
	OriginData []interface{}
}

type TableChange struct {
	InsertRows []*ChangeRow
	UpdateRows []*ChangeRow
	DeleteRows []*ChangeRow
	RowCount   int
}

type pkIndex struct {
	dataTable *DataTable
	index     []int
}

type DataTable struct {
	TableName        string
	PKConstraintName string
	changed          bool
	columns          []*DataColumn
	primaryKeys      []*DataColumn
	currentRows      *dataRows
	primaryIndexes   pkIndex
	rowStatus        []byte
	originData       [][]interface{}
	deleteRows       *dataRows
}

func NewDataTable(name string) *DataTable {
	d := &DataTable{
		TableName:   name,
		currentRows: &dataRows{},
		deleteRows:  &dataRows{},
	}
	d.primaryIndexes = pkIndex{dataTable: d}
	return d
}
func (d *DataTable) ColumnByName(name string) (int, *DataColumn) {

	for i, c := range d.columns {
		if c.Name == name {
			return i, c
		}
	}
	return -1, nil
}
func (d *DataTable) AddColumn(c *DataColumn) *DataColumn {
	if d.HasChange() {
		return nil
	}
	if i, _ := d.ColumnByName(c.Name); i == -1 {
		d.currentRows.AddColumn(c.DataType)
		d.deleteRows.AddColumn(c.DataType)
		c.index = len(d.columns)
		d.columns = append(d.columns, c)
		return c
	} else {
		return nil
	}
}
func (d *DataTable) DeleteAll() {
	for d.RowCount() > 0 {
		d.DeleteRow(0)
	}

}
func (d *DataTable) Clone() *DataTable {
	result := NewDataTable(d.TableName)
	for i := 0; i < d.ColumnCount(); i++ {
		c := d.Columns()[i]
		nc := *c
		result.AddColumn(&nc)
	}
	pks := []string{}
	for _, c := range d.PrimaryKeys() {

		pks = append(pks, c.Name)
	}
	result.SetPK(pks...)
	return result
}
func (d *DataTable) PrimaryKeys() []*DataColumn {
	return d.primaryKeys
}
func (d *DataTable) GetPK() []string {
	r := []string{}
	for _, v := range d.primaryKeys {
		r = append(r, v.Name)
	}
	return r
}
func (d *DataTable) HasPrimaryKey() bool {
	return len(d.primaryKeys) > 0
}
func (d *DataTable) ColumnNames() []string {
	r := []string{}
	for _, v := range d.columns {
		r = append(r, v.Name)
	}
	return r
}
func (d *DataTable) Columns() []*DataColumn {
	return d.columns
}
func (d *DataTable) KeyValues(rowIndex int) []interface{} {
	if len(d.PrimaryKeys()) == 0 {
		return nil
	}
	var result []interface{}
	for _, c := range d.primaryKeys {
		result = append(result, reflect.ValueOf(d.currentRows.data[c.index]).Index(d.primaryIndexes.trueIndex(rowIndex)).Interface())
	}
	return result
}
func (d *DataTable) AcceptChange() {

	d.rowStatus = make([]byte, d.currentRows.Count())
	d.originData = make([][]interface{}, d.currentRows.Count())
	d.deleteRows = &dataRows{}
	for _, c := range d.columns {
		d.deleteRows.AddColumn(c.DataType)
	}
	d.changed = false
}

/*func (d *DataTable) NewRow() *dataRow {
	var data []interface{}
	for _, c := range d.columns {
		data = append(data, getNullValue(c.DataType))
	}
	return &dataRow{dataTable: d, data: data, index: -1}
}

func (d *DataTable) ApplyRow(row *dataRow) (err error) {
	if row.index == -1 {
		err = d.AddRow(row)
	} else {
		err = d.UpdateRow(row)
	}
	return
}*/
func (d *DataTable) RowCount() int {
	return d.currentRows.Count()
}
func (d *DataTable) GetValues(rowIndex int) []interface{} {
	return d.decodeRowNull(d.currentRows.GetRow(d.primaryIndexes.trueIndex(rowIndex)))
}
func (d *DataTable) getSequenceValues(r map[string]interface{}) []interface{} {
	vals := make([]interface{}, d.ColumnCount())
	for i, col := range d.columns {
		vals[i] = r[col.Name]
	}
	return vals

}
func (d *DataTable) getPkValues(values []interface{}) []interface{} {
	var result []interface{}
	for _, c := range d.primaryKeys {
		result = append(result, values[c.index])
	}
	return result

}
func (d *DataTable) ColumnCount() int {
	return len(d.columns)
}
func (d *DataTable) SetValues(rowIndex int, values ...interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
			// find out exactly what the error was and set err
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("Unknown panic")
			}
			// invalidate rep
		}
	}()
	if rowIndex < 0 || rowIndex >= d.currentRows.Count() {
		return RowNotFoundError
	}
	newValues := d.encodeRowNull(values)
	if len(newValues) != len(d.columns) {
		panic(ColumnNotFoundError)
	}

	trueIndex := d.primaryIndexes.trueIndex(rowIndex)
	oldValues := d.currentRows.GetRow(trueIndex)
	if reflect.DeepEqual(oldValues, newValues) {
		return nil
	}
	oldPkValue := d.getPkValues(oldValues)
	newPkValue := d.getPkValues(newValues)
	pkChanged := false
	var newKeyIndex int
	if !reflect.DeepEqual(oldPkValue, newPkValue) {
		pkChanged = true
		newKeyIndex = d.primaryIndexes.Search(newPkValue)
		if len(d.primaryKeys) > 0 && newKeyIndex < d.primaryIndexes.Len() &&
			reflect.DeepEqual(newPkValue, d.KeyValues(newKeyIndex)) {
			return KeyValueExists
		}
	}
	d.changed = true
	d.currentRows.SetRow(trueIndex, newValues)
	if d.rowStatus[trueIndex] == UNCHANGE {
		d.rowStatus[trueIndex] = UPDATE
		d.originData[trueIndex] = oldValues
	}
	if pkChanged {
		d.primaryIndexes.changeIndex(rowIndex, newKeyIndex)
	}
	return
}

func (d *DataTable) search(keyValues ...interface{}) int {
	return d.primaryIndexes.Search(keyValues)
}

func (d *DataTable) Search(data ...interface{}) []map[string]interface{} {
	//keyValues := convertToNullableSlices(data)
	keyValues := data
	startIndex := d.primaryIndexes.Search(keyValues)
	i := startIndex
	var result []map[string]interface{}
	for ; i < d.RowCount(); i++ {
		r := d.GetRow(i)
		if r != nil && reflect.DeepEqual(keyValues, d.getPkValues(d.getSequenceValues(r))[:len(keyValues)]) {
			result = append(result, r)
		} else {
			break
		}
	}
	return result
}
func (d *DataTable) Find(data ...interface{}) int {
	if len(d.PrimaryKeys()) == 0 {
		return -1
	}
	//keyValues := convertToNullableSlices(data)
	keyValues := data
	i := d.search(keyValues...)
	if i < d.RowCount() &&
		reflect.DeepEqual(keyValues, d.getPkValues(d.currentRows.GetRow(d.primaryIndexes.trueIndex(i)))) {
		return i
	} else {
		return -1
	}

}
func (d *DataTable) encodeRowNull(vs []interface{}) []interface{} {
	newValues := make([]interface{}, len(vs), len(vs))
	copy(newValues, vs)
	for i, c := range d.columns {
		if newValues[i] == nil {
			newValues[i] = c.NullValue
		}
	}

	return newValues
}
func (d *DataTable) decodeRowNull(vs []interface{}) []interface{} {
	for i, c := range d.columns {
		if c.NullValue != nil {
			if vs[i] == c.NullValue {
				vs[i] = nil
			}
		}
	}
	return vs
}

func (d *DataTable) GetOriginRow(rowIndex int) map[string]interface{} {

	trueIndex := d.primaryIndexes.trueIndex(rowIndex)
	switch d.rowStatus[trueIndex] {
	case UNCHANGE:
		return d.GetRow(rowIndex)
	case UPDATE:
		vals := d.decodeRowNull(d.originData[trueIndex])
		result := map[string]interface{}{}
		for i, col := range d.columns {
			result[col.Name] = vals[i]
		}
		return result
	default:
		return nil
	}
}
func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
func (d *DataTable) AsTabText(columns ...string) string {
	result := []string{}
	if len(columns) > 0 {
		result = append(result, strings.Join(columns, "\t"))
	} else {
		result = append(result, strings.Join(d.ColumnNames(), "\t"))
	}
	for i := 0; i < d.RowCount(); i++ {
		r := d.GetRow(i)
		line := []string{}
		for j := 0; j < d.ColumnCount(); j++ {
			c := d.Columns()[j]
			if len(columns) > 0 && !stringInSlice(c.Name, columns) {
				continue
			}
			if r[c.Name] == nil {
				line = append(line, "")
			} else {
				line = append(line, fmt.Sprintf("%v", r[c.Name]))
			}
		}
		result = append(result, strings.Join(line, "\t"))
	}
	return strings.Join(result, "\n")
}
func (d *DataTable) GetColumnValues(columnIndex int) []interface{} {
	if columnIndex < 0 || columnIndex >= d.ColumnCount() {
		return nil
	}
	icount := reflect.ValueOf(d.currentRows.data[columnIndex]).Len()
	newValues := make([]interface{}, icount, icount)
	for i := 0; i < icount; i++ {
		v := reflect.ValueOf(d.currentRows.data[columnIndex]).Index(i)
		if d.columns[columnIndex].NullValue != nil &&
			reflect.DeepEqual(v, d.columns[columnIndex].NullValue) {
			newValues[i] = nil
		} else {
			newValues[i] = v.Interface()
		}
	}
	return newValues
}
func (d *DataTable) GetColumnStrings(columnIndex int) []string {
	if columnIndex < 0 || columnIndex >= d.ColumnCount() {
		return []string{}
	}
	if d.Columns()[columnIndex].DataType.Kind() == reflect.String {
		return d.currentRows.data[columnIndex].([]string)
	} else {
		return safeToStrings(d.GetColumnValues(columnIndex)...)
	}
}
func (d *DataTable) GetValue(rowIndex, colIndex int) interface{} {
	v := d.currentRows.Get(d.primaryIndexes.trueIndex(rowIndex), colIndex)
	if d.Columns()[colIndex].NullValue != nil && v == d.Columns()[colIndex].NullValue {
		v = nil
	}
	return v
}
func (d *DataTable) GetRow(rowIndex int) map[string]interface{} {
	vals := d.GetValues(rowIndex)
	result := map[string]interface{}{}
	for i, col := range d.columns {
		result[col.Name] = vals[i]
	}
	return result
}
func (d *DataTable) UpdateRow(rowIndex int, r map[string]interface{}) error {
	return d.SetValues(rowIndex, d.getSequenceValues(r)...)
}
func (d *DataTable) getChangeInsert() []*ChangeRow {
	result := []*ChangeRow{}
	for i, status := range d.rowStatus {
		if status == INSERT {
			result = append(result, &ChangeRow{
				Data: d.decodeRowNull(d.currentRows.GetRow(i)),
			})
		}
	}
	return result
}
func (d *DataTable) getChangeUpdate() []*ChangeRow {
	result := []*ChangeRow{}
	for i, status := range d.rowStatus {
		if status == UPDATE {
			result = append(result, &ChangeRow{
				Data:       d.decodeRowNull(d.currentRows.GetRow(i)),
				OriginData: d.decodeRowNull(d.originData[i]),
			})
		}
	}
	return result
}
func (d *DataTable) getChangeDelete() []*ChangeRow {
	result := []*ChangeRow{}
	for i := 0; i < d.deleteRows.Count(); i++ {

		result = append(result, &ChangeRow{
			Data: d.decodeRowNull(d.deleteRows.GetRow(i)),
		})
	}
	return result
}
func (d *DataTable) GetChange() *TableChange {
	result := &TableChange{}
	result.DeleteRows = d.getChangeDelete()
	result.UpdateRows = d.getChangeUpdate()
	result.InsertRows = d.getChangeInsert()
	result.RowCount = len(result.DeleteRows) + len(result.UpdateRows) + len(result.InsertRows)
	return result
}

func (d *DataTable) DeleteRow(rowIndex int) error {
	if rowIndex < 0 || rowIndex >= d.currentRows.Count() {
		return RowNotFoundError
	}
	trueIndex := d.primaryIndexes.trueIndex(rowIndex)
	var oldValues []interface{}

	switch d.rowStatus[trueIndex] {
	case UNCHANGE, INSERT:
		oldValues = d.currentRows.GetRow(trueIndex)
	case UPDATE:
		oldValues = d.originData[trueIndex]
	default:
		oldValues = nil
	}
	lastIdx := d.RowCount() - 1
	d.changed = true
	d.currentRows.Remove(trueIndex)
	if oldValues != nil {

		d.deleteRows.AddRow(oldValues)
	}
	d.rowStatus[rowIndex], d.rowStatus = d.rowStatus[lastIdx], d.rowStatus[:lastIdx]
	d.originData[rowIndex], d.originData = d.originData[lastIdx], d.originData[:lastIdx]
	d.primaryIndexes.removeIndex(rowIndex, lastIdx)

	return nil
}
func (d *DataTable) AddValues(vs ...interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
			// find out exactly what the error was and set err
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("Unknown panic")
			}
			// invalidate rep
		}
	}()
	if len(vs) != len(d.columns) {
		panic(ColumnNotFoundError)
	}
	data := d.encodeRowNull(vs)
	keyvalues := d.getPkValues(data)
	newKeyIndex := d.primaryIndexes.Search(keyvalues)
	if len(d.primaryKeys) > 0 && newKeyIndex < d.primaryIndexes.Len() &&
		reflect.DeepEqual(d.KeyValues(newKeyIndex), keyvalues) {
		return KeyValueExists
	}
	d.changed = true
	d.currentRows.AddRow(data)
	newIndex := d.currentRows.Count() - 1
	d.rowStatus = append(d.rowStatus, INSERT)
	d.originData = append(d.originData, nil)
	d.primaryIndexes.appendIndex(newKeyIndex, newIndex)
	return

}
func (d *DataTable) AddRow(r map[string]interface{}) error {
	return d.AddValues(d.getSequenceValues(r)...)
}
func (p *pkIndex) removeIndex(rowIndex, lastTrueIdx int) {
	//Delete preserving order
	newLstIdx := p.index[rowIndex]
	p.index = append(p.index[:rowIndex], p.index[rowIndex+1:]...)
	//search the lastTrueIdx
	for i, v := range p.index {
		if v == lastTrueIdx {
			p.index[i] = newLstIdx
			break
		}
	}
}

//索引位置调整，旧位置调整到新位置
func (p *pkIndex) changeIndex(oldIndex, newIndex int) {
	if oldIndex == newIndex {
		return
	}
	//取出旧索引对应的索引值
	oldTrueIndex := p.index[oldIndex]
	//在新位置插入索引值
	p.index = append(p.index[:newIndex], append([]int{oldTrueIndex}, p.index[newIndex:]...)...)
	//如果新索引在旧索引的前面，则旧索引实际位置是后移一个的位置
	if oldIndex > newIndex {
		oldIndex++ //由于插入了，所以要加1
	}
	//删除旧索引
	p.index = append(p.index[:oldIndex], p.index[oldIndex+1:]...)
}
func (p *pkIndex) appendIndex(newIndex, newTrueIndex int) {
	p.index = append(p.index[:newIndex], append([]int{newTrueIndex}, p.index[newIndex:]...)...)
}
func (p *pkIndex) rebuildPKIndex() {
	if len(p.dataTable.PrimaryKeys()) == 0 {
		return
	}
	p.index = make([]int, p.dataTable.currentRows.Count())
	for i := 0; i < len(p.index); i++ {
		p.index[i] = i
	}
	sort.Sort(p)
}
func (p *pkIndex) Len() int {
	return len(p.index)
}
func (p *pkIndex) trueIndex(i int) int {
	if len(p.dataTable.PrimaryKeys()) == 0 {
		return i
	}

	return p.index[i]
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
	if len(v1) < len(v2) {
		return true
	} else {
		return false
	}
}

//func nullableOff(v interface{}) interface{} {

//	switch v.(type) {
//	case sql.NullBool:
//		return v.(sql.NullBool).Bool
//	case sql.NullFloat64:
//		return v.(sql.NullFloat64).Float64
//	case sql.NullInt64:
//		return v.(sql.NullInt64).Int64
//	case sql.NullString:
//		return v.(sql.NullString).String
//	default:
//		panic(fmt.Sprintf("type %#v invalid", v))
//	}
//}
//func convertToNullableSlices(data []interface{}) []interface{} {
//	result := []interface{}{}
//	for _, v := range data {
//		result = append(result, convertToNullable(v))
//	}
//	return result
//}
//func convertToNullable(v interface{}) interface{} {
//	switch v.(type) {
//	case bool:
//		return sql.NullBool{Bool: v.(bool), Valid: true}
//	case float64:
//		return sql.NullFloat64{Float64: v.(float64), Valid: true}
//	case int:
//		return sql.NullInt64{Int64: int64(v.(int)), Valid: true}
//	case int64:
//		return sql.NullInt64{Int64: v.(int64), Valid: true}
//	case string:
//		return sql.NullString{String: v.(string), Valid: true}
//	case sql.NullBool, sql.NullFloat64, sql.NullInt64, sql.NullString:
//		return v
//	default:
//		panic(fmt.Sprintf("type %#v invalid", v))
//	}

//}
func less(v1, v2 interface{}) bool {
	//v1 = convertToNullable(v1)
	//v2 = convertToNullable(v2)
	if reflect.TypeOf(v1) != reflect.TypeOf(v2) {
		return false
	}
	switch v1.(type) {
	//case sql.NullBool:
	//	vi := v1.(sql.NullBool)
	//	vj := v2.(sql.NullBool)
	//	return !vi.Valid || (vj.Valid && !vi.Bool)
	//case sql.NullFloat64:
	//	vi := v1.(sql.NullFloat64)
	//	vj := v2.(sql.NullFloat64)
	//	return !vi.Valid || (vj.Valid && vi.Float64 < vj.Float64)
	//case sql.NullInt64:
	//	vi := v1.(sql.NullInt64)
	//	vj := v2.(sql.NullInt64)
	//	return !vi.Valid || (vj.Valid && vi.Int64 < vj.Int64)
	//case sql.NullString:
	//	vi := v1.(sql.NullString)
	//	vj := v2.(sql.NullString)
	//	return !vi.Valid || (vj.Valid && vi.String < vj.String)
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
	default:
		panic(fmt.Sprintf("type %#v invalid", v1))
	}
}
func (d *DataTable) ColumnIndex(col string) int {
	for i, c := range d.columns {
		if c.Name == col {
			return i
		}
	}
	return -1
}
func (d *DataTable) IsPrimaryKey(cname string) bool {
	return d.columnIndexByPrimaryKey(cname) > -1
}

func (d *DataTable) columnIndexByPrimaryKey(cname string) int {
	for i, p := range d.primaryKeys {
		if cname == p.Name {
			return i
		}
	}
	return -1
}
func (p *pkIndex) Less(i, j int) bool {
	ss := lessSlices(p.dataTable.KeyValues(i), p.dataTable.KeyValues(j))

	return ss
}
func (p *pkIndex) Swap(i, j int) {
	p.index[i], p.index[j] = p.index[j], p.index[i]
}

func (p *pkIndex) Search(keys []interface{}) int {
	return sort.Search(len(p.index), func(i int) bool {
		ss := !lessSlices(p.dataTable.KeyValues(i), keys)
		return ss
	})
}

func (d *DataTable) SetPK(names ...string) {
	//需要验证每个column存在
	pks := []*DataColumn{}
	for _, c := range names {
		if i, cc := d.ColumnByName(c); i == -1 {
			panic(fmt.Errorf("column %s not found,at %v", c, d.Columns()))
		} else {
			pks = append(pks, cc)
		}
	}
	d.primaryKeys = pks
	d.primaryIndexes.rebuildPKIndex()
}
func (d *DataTable) HasChange() bool {
	return d.changed
}
