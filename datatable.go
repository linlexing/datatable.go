// datatable project datatable.go
package datatable

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
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
	//NotThisTableRow     = errors.New("the row not is this table's row")

)

func PrimaryKeyTypeError(t string) error {
	return fmt.Errorf("primary key type [%s] invalid", t)
}

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

func (d *DataTable) AddColumn(c *DataColumn) *DataColumn {

	if i := d.ColumnIndex(c.Name); i == -1 {
		d.currentRows.AddColumn(c.DataType)
		d.deleteRows.AddColumn(c.DataType)
		for i := 0; i < len(d.originData); i++ {
			d.originData[i] = append(d.originData[i], c.ZeroValue()) //use nil ,maybe error
		}
		c.index = len(d.columns)
		d.columns = append(d.columns, c)
		return c
	} else {
		panic(ColumnExistsError)
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

func (d *DataTable) RowCount() int {
	return d.currentRows.Count()
}
func (d *DataTable) GetValues(rowIndex int) []interface{} {
	return d.currentRows.GetRow(d.primaryIndexes.trueIndex(rowIndex))
}
func (d *DataTable) getSequenceValues(r map[string]interface{}) []interface{} {
	vals := make([]interface{}, d.ColumnCount())
	for i, col := range d.columns {
		var ok bool
		if vals[i], ok = r[col.Name]; !ok {
			panic(fmt.Errorf("can't find column:[%s] at %v", col.Name, r))
		}

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
	newValues := values
	if len(newValues) != len(d.columns) {
		return ColumnNotFoundError
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

func (d *DataTable) GetOriginRow(rowIndex int) map[string]interface{} {

	trueIndex := d.primaryIndexes.trueIndex(rowIndex)
	switch d.rowStatus[trueIndex] {
	case UNCHANGE:
		return d.GetRow(rowIndex)
	case UPDATE:
		vals := d.originData[trueIndex]
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
		newValues[i] = v.Interface()
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
	return d.currentRows.Get(d.primaryIndexes.trueIndex(rowIndex), colIndex)
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
				Data: d.currentRows.GetRow(i),
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
				Data:       d.currentRows.GetRow(i),
				OriginData: d.originData[i],
			})
		}
	}
	return result
}
func (d *DataTable) getChangeDelete() []*ChangeRow {
	result := []*ChangeRow{}
	for i := 0; i < d.deleteRows.Count(); i++ {

		result = append(result, &ChangeRow{
			Data: d.deleteRows.GetRow(i),
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
	if len(vs) != len(d.columns) {
		return ColumnNotFoundError
	}
	data := vs
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

//the primary key data type must in
//int int64 float32 float64 string []byte time.Time
//and/or above type's slice
func (d *DataTable) SetPK(names ...string) {
	//需要验证每个column存在
	pks := []*DataColumn{}
	for _, c := range names {
		if i := d.ColumnIndex(c); i == -1 {
			panic(fmt.Errorf("column %s not found,at %v", c, d.Columns()))
		} else {
			pks = append(pks, d.Columns()[i])
		}
	}
	d.primaryKeys = pks
	d.primaryIndexes.rebuildPKIndex()
}
func (d *DataTable) HasChange() bool {
	return d.changed
}
