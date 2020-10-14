// datatable project datatable.go
package datatable

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
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
	ColumnExistsError = errors.New("the column exists")
	RowNotFoundError  = errors.New("the row not found")
	KeyValueExists    = errors.New("the key value aleary exists")
	InterfaceType     = reflect.TypeOf((*interface{})(nil)).Elem()
	NilValue          = reflect.Zero(InterfaceType)
	//NotThisTableRow     = errors.New("the row not is this table's row")

)

func NumberOfValueError(valNum, colNum int) error {

	return fmt.Errorf("number of value :%d != column count: %d", valNum, colNum)
}
func ColumnNotFoundError(cname string) error {
	return fmt.Errorf("the column [%s] not found", cname)
}
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
	TableName      string
	changed        bool
	Columns        []*DataColumn
	PK             []string
	currentRows    *dataRows
	primaryIndexes pkIndex
	rowStatus      []byte
	originData     [][]interface{}
	deleteRows     *dataRows
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
		d.currentRows.AddColumn(c.StoreType())
		d.deleteRows.AddColumn(c.StoreType())
		for i := 0; i < len(d.originData); i++ {
			d.originData[i] = append(d.originData[i], c.ZeroValue()) //use nil ,maybe error
		}
		c.index = len(d.Columns)
		d.Columns = append(d.Columns, c)
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
		c := d.Columns[i]
		nc := *c
		result.AddColumn(&nc)
	}
	result.SetPK(d.PK...)
	return result
}

func (d *DataTable) HasPrimaryKey() bool {
	return len(d.PK) > 0
}
func (d *DataTable) ColumnNames() []string {
	r := make([]string, d.ColumnCount())
	for i, v := range d.Columns {
		r[i] = v.Name
	}
	return r
}

func (d *DataTable) KeyValues(rowIndex int) []interface{} {
	if len(d.PK) == 0 {
		return nil
	}
	var result []interface{}
	for _, c := range d.PK {
		result = append(result, d.currentRows.Get(d.ColumnIndex(c), d.primaryIndexes.trueIndex(rowIndex)))
	}
	return result
}
func (d *DataTable) AcceptChange() {

	d.rowStatus = make([]byte, d.currentRows.Count())
	d.originData = make([][]interface{}, d.currentRows.Count())
	d.deleteRows = &dataRows{}
	for _, c := range d.Columns {
		d.deleteRows.AddColumn(c.StoreType())
	}
	d.changed = false
}

func (d *DataTable) RowCount() int {
	return d.currentRows.Count()
}
func (d *DataTable) GetValues(rowIndex int) []interface{} {
	rev := make([]interface{}, d.ColumnCount())
	for i, v := range d.currentRows.GetRow(d.primaryIndexes.trueIndex(rowIndex)) {
		rev[i] = d.Columns[i].Decode(v)
	}
	return rev
}
func (d *DataTable) getSequenceValues(r map[string]interface{}) []interface{} {
	vals := make([]interface{}, d.ColumnCount())
	for i, col := range d.Columns {
		var ok bool
		if vals[i], ok = r[col.Name]; !ok {
			//panic(fmt.Errorf("can't find column:[%s] at %v", col.Name, r))
		}

	}
	return vals

}
func (d *DataTable) getPkValues(values []interface{}) []interface{} {
	var result []interface{}
	for _, c := range d.PK {
		result = append(result, values[d.ColumnIndex(c)])
	}
	return result

}
func (d *DataTable) ColumnCount() int {
	return len(d.Columns)
}
func (d *DataTable) SetValues(rowIndex int, values ...interface{}) error {
	newValues, err := d.validValues(values)
	if err != nil {
		return err
	}
	if len(newValues) != d.ColumnCount() {
		return NumberOfValueError(len(newValues), d.ColumnCount())
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
		if len(d.PK) > 0 && newKeyIndex < d.primaryIndexes.Len() &&
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
	return nil
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
		r := d.Row(i)
		if r != nil && cmpValue(keyValues, d.getPkValues(d.getSequenceValues(r))[:len(keyValues)]) == 0 {
			result = append(result, r)
		} else {
			break
		}
	}
	return result
}
func (d *DataTable) Find(data ...interface{}) int {
	if len(d.PK) == 0 {
		return -1
	}
	//keyValues := convertToNullableSlices(data)
	keyValues := data
	i := d.search(keyValues...)
	if i < d.RowCount() &&
		cmpValue(keyValues, d.getPkValues(d.currentRows.GetRow(d.primaryIndexes.trueIndex(i)))) == 0 {
		return i
	} else {
		return -1
	}

}

func (d *DataTable) GetOriginRow(rowIndex int) map[string]interface{} {

	trueIndex := d.primaryIndexes.trueIndex(rowIndex)
	switch d.rowStatus[trueIndex] {
	case UNCHANGE:
		return d.Row(rowIndex)
	case UPDATE:
		vals := d.originData[trueIndex]
		result := map[string]interface{}{}
		for i, col := range d.Columns {
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

//Exports as CSV, if no columns provided it will use all columns
func (d *DataTable) AsCsv(columns ...string) string {
	bys := &bytes.Buffer{}
	csvWriter := csv.NewWriter(bys)
	var line []string
	if len(columns) > 0 {
		line = make([]string, len(columns))
	} else {
		line = make([]string, len(d.ColumnNames()))
		columns = d.ColumnNames()
	}

	for i, v := range columns {
		line[i] = v
	}
	if err := csvWriter.Write(line); err != nil {
		panic(err)
	}
	for rowIdx := 0; rowIdx < d.RowCount(); rowIdx++ {
		for i, v := range columns {
			colIdx := d.ColumnIndex(v)
			line[i] = d.Columns[colIdx].EncodeString(d.GetValue(rowIdx, colIdx))
		}
		if err := csvWriter.Write(line); err != nil {
			panic(err)
		}
	}
	csvWriter.Flush()
	return bys.String()
}
func (d *DataTable) AsJSONP(callback string, columns ...string) string {
	data := make([]interface{}, d.RowCount())
	for rowIdx := 0; rowIdx < d.RowCount(); rowIdx++ {
		line := make([]interface{}, len(columns))
		for i, v := range columns {
			colIdx := d.ColumnIndex(v)
			line[i] = d.GetValue(rowIdx, colIdx)
		}
		data[rowIdx] = line
	}
	bys, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	cols, err := json.Marshal(columns)
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf(`
function %s(){
	data = %s;
	cols = %s;
	rev = [];
	for(rowIndex in data){
		rowObject = {};
		for(colIndex in cols){
			rowObject[cols[colIndex]] = data[rowIndex][colIndex];
		}
		rev.push(rowObject);
	}
	return rev;
}`, callback, string(bys), string(cols))
}

func (d *DataTable) AsTabText(columns ...string) string {
	result := []string{}
	if len(columns) > 0 {
		result = append(result, strings.Join(columns, "\t"))
	} else {
		result = append(result, strings.Join(d.ColumnNames(), "\t"))
	}
	for i := 0; i < d.RowCount(); i++ {
		r := d.Row(i)
		line := []string{}
		for j := 0; j < d.ColumnCount(); j++ {
			c := d.Columns[j]
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
		newValues[i] = d.Columns[columnIndex].Decode(v.Interface())
	}
	return newValues
}
func (d *DataTable) GetValue(rowIndex, colIndex int) interface{} {
	return d.Columns[colIndex].Decode(d.currentRows.Get(colIndex, d.primaryIndexes.trueIndex(rowIndex)))
}
func (d *DataTable) GetString(rowIndex, colIndex int) string {
	return d.Columns[colIndex].EncodeString(d.GetValue(rowIndex, colIndex))
}
func (d *DataTable) Row(rowIndex int) map[string]interface{} {
	vals := d.GetValues(rowIndex)
	result := map[string]interface{}{}
	for i, col := range d.Columns {
		result[col.Name] = vals[i]
	}
	return result
}
func (d *DataTable) Rows() []map[string]interface{} {
	rev := make([]map[string]interface{}, d.RowCount())
	for i := 0; i < d.RowCount(); i++ {
		rev[i] = d.Row(i)
	}
	return rev
}
func (d *DataTable) UpdateRow(rowIndex int, r map[string]interface{}) error {
	return d.SetValues(rowIndex, d.getSequenceValues(r)...)
}
func (d *DataTable) NewRow() map[string]interface{} {
	result := map[string]interface{}{}
	for _, col := range d.Columns {
		result[col.Name] = col.ZeroValue()
	}
	return result
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
			OriginData: d.deleteRows.GetRow(i),
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
func (d *DataTable) validValues(vs []interface{}) ([]interface{}, error) {
	rev := make([]interface{}, len(vs))
	for i, v := range vs {
		if err := d.Columns[i].Valid(v); err != nil {
			return nil, err
		}
		rev[i] = d.Columns[i].Encode(v)
	}
	return rev, nil
}
func (d *DataTable) AddValues(vs ...interface{}) error {
	if len(vs) != d.ColumnCount() {
		return NumberOfValueError(len(vs), d.ColumnCount())
	}
	data, err := d.validValues(vs)
	if err != nil {
		return err
	}
	keyvalues := d.getPkValues(data)
	newKeyIndex := d.primaryIndexes.Search(keyvalues)
	if len(d.PK) > 0 && newKeyIndex < d.primaryIndexes.Len() &&
		reflect.DeepEqual(d.KeyValues(newKeyIndex), keyvalues) {
		return KeyValueExists
	}
	d.changed = true
	d.currentRows.AddRow(data)
	newIndex := d.currentRows.Count() - 1
	d.rowStatus = append(d.rowStatus, INSERT)
	d.originData = append(d.originData, nil)
	d.primaryIndexes.appendIndex(newKeyIndex, newIndex)
	return nil

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
	if len(p.dataTable.PK) == 0 {
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
	if len(p.dataTable.PK) == 0 {
		return i
	}

	return p.index[i]
}
func (d *DataTable) ColumnIndex(col string) int {
	for i, c := range d.Columns {
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
	for i, p := range d.PK {
		if cname == p {
			return i
		}
	}
	return -1
}
func (d *DataTable) Clear() {
	d.currentRows = &dataRows{}
	d.deleteRows = &dataRows{}
	d.primaryIndexes = pkIndex{dataTable: d}
	d.rowStatus = nil
	d.originData = nil
	for _, c := range d.Columns {
		d.currentRows.AddColumn(c.StoreType())
		d.deleteRows.AddColumn(c.StoreType())
	}
	d.changed = false
}
func (p *pkIndex) Less(i, j int) bool {
	return cmpValue(p.dataTable.KeyValues(i), p.dataTable.KeyValues(j)) < 0
}
func (p *pkIndex) Swap(i, j int) {
	p.index[i], p.index[j] = p.index[j], p.index[i]
}

func (p *pkIndex) Search(keys []interface{}) int {
	return sort.Search(len(p.index), func(i int) bool {
		return cmpValue(p.dataTable.KeyValues(i), keys) >= 0
	})
}

//the primary key data type must in
//int int64 float32 float64 string []byte time.Time
//and/or above type's slice
func (d *DataTable) SetPK(names ...string) {
	//需要验证每个column存在
	for _, c := range names {
		if i := d.ColumnIndex(c); i == -1 {
			panic(fmt.Errorf("column %s not found,at %v", c, d.Columns))
		}
	}
	d.PK = names
	d.primaryIndexes.rebuildPKIndex()
}
func (d *DataTable) HasChange() bool {
	return d.changed
}
func (d *DataTable) Merge(srcTable *DataTable) error {
	if d.ColumnCount() != srcTable.ColumnCount() {
		return fmt.Errorf("the src table columncount:%d not is %d", srcTable.ColumnCount(), d.ColumnCount())
	}
	for i, col := range d.Columns {
		if srcTable.Columns[i].DataType != col.DataType {
			return fmt.Errorf("the column:%s data type %s not equal %s", col.Name, col.DataType, srcTable.Columns[i].DataType)
		}
	}
	pks := make([]int, len(srcTable.primaryIndexes.index))
	copy(pks, srcTable.primaryIndexes.index)
	for i, _ := range pks {
		pks[i] += d.RowCount()
	}
	d.primaryIndexes.index = append(d.primaryIndexes.index, pks...)
	d.currentRows.Merge(srcTable.currentRows)
	d.deleteRows.Merge(srcTable.deleteRows)
	d.rowStatus = append(d.rowStatus, srcTable.rowStatus...)
	d.originData = append(d.originData, srcTable.originData...)
	return nil
}
