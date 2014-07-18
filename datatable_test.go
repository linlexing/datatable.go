package datatable

import (
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestBase(t *testing.T) {
	table := NewDataTable("Table1")
	if table == nil {
		t.Error("can't create")
	}
}
func TestDataTable_AddColumns(t *testing.T) {
	table := NewDataTable("Table1")
	c := table.AddColumn(NewStringColumn("column1"))
	if c == nil {
		t.Error("error")
	}
	if c.Name != "column1" {
		t.Error("error")
	}
}
func TestDataTable_ColumnIndex(t *testing.T) {
	table := NewDataTable("Table1")
	c := table.AddColumn(NewStringColumn("column1"))
	if c.index != 0 {
		t.Error("error")
	}
	if v := table.Columns[0]; v == nil {
		t.Error("null")
	}
	c = table.AddColumn(NewInt64Column("column2"))
	if c.index != 1 {
		t.Error("error")
	}
}
func TestDataTable_PrimaryKey(t *testing.T) {
	table := NewDataTable("Table1")
	c1 := table.AddColumn(NewStringColumn("column1"))
	c2 := table.AddColumn(NewInt64Column("column2"))
	table.SetPK("column1", "column2")

	if !table.IsPrimaryKey(c1.Name) || !table.IsPrimaryKey(c2.Name) {
		t.Error("error")
	}
}
func TestDataTable_NoPrimaryKey(t *testing.T) {
	table := NewDataTable("Table1")
	table.AddColumn(NewStringColumn("column1"))
	table.AddColumn(NewInt64Column("column2"))
	err := table.AddValues("11", int64(11))
	if err != nil {
		t.Error(err)
	}
	err = table.AddValues("22", int64(22))
	if err != nil {
		t.Error(err)
	}
}
func TestMemory(t *testing.T) {
	m := runtime.MemStats{}
	runtime.ReadMemStats(&m)
	fmt.Println("mem:", m.Alloc/1024/1024)
	tab := CreateBenchmarkData(10000, 20)
	runtime.ReadMemStats(&m)
	fmt.Println("count:", tab.RowCount(), ",mem:", m.Alloc/1024/1024)
}
func TestDataTable_AlterColumn(t *testing.T) {
	table := NewDataTable("Table1")
	table.AddColumn(NewStringColumn("column1"))
	table.AddColumn(NewInt64Column("column2"))
	table.SetPK("column1", "column2")
	table.AddRow(map[string]interface{}{
		"column1": "test",
		"column2": int64(12),
	})
	table.AddRow(map[string]interface{}{
		"column1": "test1",
		"column2": int64(0),
	})

	c := table.AddColumn(NewStringColumn("column3"))
	if c == nil {
		t.Error("error")
	}
}
func CreateTestData() *DataTable {
	table := NewDataTable("Table1")
	table.AddColumn(NewStringColumn("column1"))
	table.AddColumn(NewIntColumn("column2"))
	table.AddColumn(NewStringColumn("column3"))
	table.SetPK("column1", "column2")
	table.AddValues("first", 10, "test1")
	table.AddValues("second", 1, "test")
	table.AddValues("aaa", 10, "test1")
	table.AddValues("bbb", 10, "test1")
	table.AddValues("first", 11, "test1")
	return table
}
func TestDataTable_Data(t *testing.T) {
	table := NewDataTable("Table1")
	table.AddColumn(NewStringColumn("column1"))
	table.AddColumn(NewInt64Column("column2"))
	table.SetPK("column1", "column2")

	table.AddRow(map[string]interface{}{
		"column1": "test",
		"column2": int64(3),
	})
	if table.RowCount() != 1 {
		t.Error("error")
	}
	r := table.Row(0)
	if r == nil {
		t.Error("error")
	}
	if r["column1"] != "test" {
		t.Error("error")
	}
	if r["column2"] != int64(3) {
		t.Error("error")
	}
	r["column2"] = int64(12)
	err := table.UpdateRow(0, r)
	if err != nil {
		t.Error(err)
	}
	r = table.Row(0)
	if r["column2"] != int64(12) {
		t.Error("error")
	}
	i := table.Find("test", int64(12))
	if i != 0 {
		t.Error(fmt.Printf("error,i:%v\n", i))
	}
	err = table.AddRow(map[string]interface{}{
		"column1": "test",
		"column2": int64(12),
	})
	if err != KeyValueExists {
		t.Error("error")
	}
	err = table.AddRow(map[string]interface{}{
		"column1": "tes",
		"column2": int64(12),
	})
	if err != nil {
		t.Error(err)
	}
	r = table.Row(0)
	if r == nil {
		t.Error("error")
	}
	if r["column1"] != "tes" {
		t.Error("error")
	}
	err = table.DeleteRow(1)
	if err != nil {
		t.Error(err)
	}
	if table.RowCount() != 1 {
		t.Error(fmt.Printf("error,count:%v", table.RowCount()))
	}

}
func TestDataTable_Search(t *testing.T) {
	table := CreateTestData()
	if table.Find("first", 10) == -1 {
		t.Error("error")
	}
	rows := table.Search("first")
	if len(rows) != 2 {
		t.Error(fmt.Printf("error,count:%v\n", len(rows)))
	}
}
func TestDataTable_Delete(t *testing.T) {
	table := CreateTestData()
	if table.RowCount() != 5 {
		t.Error("error")
	}
	if err := table.DeleteRow(4); err != nil || table.RowCount() != 4 {
		t.Error(err)
	}

}
func TestDataTable_GetChange(t *testing.T) {
	table := CreateTestData()
	table.AcceptChange()
	r := table.Row(0)
	r["column1"] = "first1"
	table.UpdateRow(0, r)
	table.AddValues("zzzdfdfdf", 2323, "dfadfa")
	err := table.DeleteRow(1)
	if err != nil {
		t.Error(err)
	}

	chgsRows := table.GetChange()
	if len(chgsRows.DeleteRows) != 1 ||
		len(chgsRows.UpdateRows) != 1 ||
		len(chgsRows.InsertRows) != 1 {
		t.Error(fmt.Sprintf("error,count:%#v", chgsRows))
	}
}
func TestDataTable_GetOriginRow(t *testing.T) {
	table := CreateTestData()
	table.AcceptChange()
	r := table.Row(0)
	r["column3"] = "afdafad"
	table.UpdateRow(0, r)
	r1 := table.GetOriginRow(0)
	if r1 == nil {
		t.Error("error")
	}
	if r["column3"] == r1["column3"] {
		t.Error("error")
	}
}
func Benchmark_AddRowWithPrimaryKeys(b *testing.B) {
	table := CreateBenchmarkData(0, 30)
	table.SetPK("column1")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		one := []interface{}{fmt.Sprintf("rows%v", i), i}
		for i := 2; i < 30; i++ {
			one = append(one, fmt.Sprintf("column%v", i+1))
		}
		if err := table.AddValues(one...); err != nil {
			b.Error(err)
		}
	}
	b.SetBytes(400)

}
func Benchmark_SetPKValues(b *testing.B) {
	table := CreateBenchmarkData(10000, 30)
	table.SetPK("column1")
	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rowindex := rand.Intn(10000)
		r := table.Row(rowindex)
		if r == nil {
			fmt.Print("error", ",ri:", rowindex)
		}
		r[table.Columns[0].Name] = fmt.Sprint(rand.Int63n(1000000))
		if err := table.UpdateRow(i, r); err != nil && err != KeyValueExists {
			b.Error(err)
		}
	}
	b.SetBytes(400)
}
func CreateBenchmarkData(row, column int) *DataTable {
	table := NewDataTable("Table1")
	table.AddColumn(NewStringColumn("column1"))
	table.AddColumn(NewIntColumn("column2"))
	for i := 2; i < column; i++ {
		table.AddColumn(NewStringColumn(fmt.Sprintf("column%v", i+1)))
	}
	for i := 0; i < row; i++ {

		one := []interface{}{fmt.Sprintf("rows%v", i), i}
		for i := 2; i < column; i++ {
			one = append(one, fmt.Sprintf("column%v", i+1))
		}
		err := table.AddValues(one...)
		if err != nil {
			panic(err)
		}
	}
	return table
}
func Benchmark_AddRowNoPrimaryKeys(b *testing.B) {
	table := NewDataTable("Table1")
	table.AddColumn(NewStringColumn("column1"))
	table.AddColumn(NewInt64Column("column2"))
	for i := 0; i < 30; i++ {
		table.AddColumn(NewStringColumn(fmt.Sprintf("column%v", i+3)))

	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		one := []interface{}{fmt.Sprintf("rows%v", i), int64(i)}
		for i := 0; i < 30; i++ {
			one = append(one, fmt.Sprintf("column%v", i+3))
		}
		if err := table.AddValues(one...); err != nil {
			b.Error(err)
		}
	}

}
func Benchmark_BuildIndex300(b *testing.B) {
	table := CreateBenchmarkData(300, 3)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table.SetPK()
		table.SetPK("column1")
	}

}
func Benchmark_Find(b *testing.B) {
	table := CreateBenchmarkData(10000, 10)
	table.SetPK("column1", "column2")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table.Find(fmt.Sprintf("column%v", i+1), i)
	}
	b.SetBytes(140)
}
func BenchmarkRow(b *testing.B) {
	table := CreateBenchmarkData(100, 10)
	table.SetPK("column1", "column2")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table.Row(rand.Intn(100))
	}
	b.SetBytes(140)
}
func ExampleDataTable_AddValues() {
	table := NewDataTable("table1")
	table.AddColumn(NewStringColumn("column1"))
	table.AddColumn(NewIntColumn("column2"))
	table.AddColumn(NewStringColumn("column3"))
	table.AddColumn(NewStringColumn("column4"))
	table.AddColumn(NewStringColumn("column5"))
	table.AddColumn(NewStringColumn("column6"))
	table.AddColumn(NewStringColumn("column7"))
	table.AddColumn(NewStringColumn("column8"))
	table.SetPK("column2", "column1")
	for i := 10; i >= 1; i-- {
		table.AddValues(fmt.Sprint("row", i), i, "field3", "field4", "field5", "field6", "field7", "field8")
	}
	table.AddValues(fmt.Sprint("row", 200), 2, "field3", "field4", "field5", "field6", "field7", "field8")
	err := table.SetValues(0, fmt.Sprint("row", 201), 6, "field3", "field4", "field5", "field6", "field7", "field8")
	if err != nil {
		fmt.Print(err)
	}
	r := table.Row(3)
	r["column1"] = "row101"
	r["column2"] = 2
	if err := table.UpdateRow(3, r); err != nil {
		fmt.Print(err)
	}
	fmt.Print(table.AsTabText("column1", "column2"))
	// Output:
	//column1	column2
	//row101	2
	//row2	2
	//row200	2
	//row3	3
	//row5	5
	//row201	6
	//row6	6
	//row7	7
	//row8	8
	//row9	9
	//row10	10

}
func TestGetColumnValues(t *testing.T) {
	table := NewDataTable("table1")
	table.AddColumn(NewStringColumn("column1"))
	table.AddValues("1")
	table.AddValues("2")
	arr := table.GetColumnValues(0)
	if !reflect.DeepEqual(arr, []interface{}{"1", "2"}) {
		t.Error("error")
	}
}
func TestHasChange(t *testing.T) {
	table := NewDataTable("table1")
	table.AddColumn(NewStringColumn("column1"))
	table.AddColumn(NewInt64Column("column2"))
	if table.HasChange() == true {
		t.Error("error")
	}
	table.AddValues("2322", int64(2223))
	if table.HasChange() == false {
		t.Error("error")
	}
	table.AcceptChange()
	if table.HasChange() == true {
		t.Error("error")
	}

}

func TestByteaColumn(t *testing.T) {
	table := NewDataTable("table1")
	if table.AddColumn(NewByteaColumn("column1")) == nil {
		t.Error("is nil")
	}
	if table.AddColumn(NewInt64Column("column2")) == nil {
		t.Error("is nil")
	}
	row := []interface{}{[]byte{1, 2, 3}, int64(44)}
	if err := table.AddValues(row...); err != nil {
		t.Error(err, ",lenght:", len(row), ",columnLen:", table.ColumnCount())
	}
	if !reflect.DeepEqual([]byte{1, 2, 3}, table.GetValue(0, 0)) {
		t.Error("error")
	}
	row = []interface{}{[]byte(nil), int64(44)}
	if err := table.AddValues(row...); err != nil {
		t.Error(err, ",lenght:", len(row), ",columnLen:", table.ColumnCount())
	}
	if len(table.GetValue(1, 0).([]byte)) != 0 {
		t.Error(fmt.Sprintf("error:%T", table.GetValue(1, 0)))
	}

}
func TestArrayColumn(t *testing.T) {
	table := NewDataTable("table1")
	if table.AddColumn(NewDataColumn("column1", reflect.TypeOf([][]byte{}))) == nil {
		t.Error("is nil")
	}
	if table.AddColumn(NewDataColumn("column2", reflect.TypeOf([]string{}))) == nil {
		t.Error("is nil")
	}
	row := []interface{}{[][]byte{[]byte{1, 2, 3}, []byte{2, 2, 3}}, []string{"22", "33"}}
	if err := table.AddValues(row...); err != nil {
		t.Error(err, ",lenght:", len(row), ",columnLen:", table.ColumnCount())
	}
	if !reflect.DeepEqual([][]byte{[]byte{1, 2, 3}, []byte{2, 2, 3}}, table.GetValue(0, 0)) {
		t.Error("error")
	}
	table.SetPK("column1", "column2")
	if table.Find([][]byte{[]byte{1, 2, 3}, []byte{2, 2, 3}}, []string{"22", "33"}) < 0 {
		t.Error("error")
	}
}
func TestMerge(t *testing.T) {
	table := NewDataTable("table1")
	table.AddColumn(NewDataColumn("column1", reflect.TypeOf("")))
	table.AddColumn(NewDataColumn("column2", reflect.TypeOf("")))
	table.SetPK("column1")
	table.AddValues("row1", "row1_1")
	table.AddValues("row2", "row2_1")
	table.AcceptChange()
	table.SetValues(0, "row1_1", "test")
	table1 := NewDataTable("table1")
	table1.AddColumn(NewDataColumn("column1", reflect.TypeOf("")))
	table1.AddColumn(NewDataColumn("column2", reflect.TypeOf("")))
	table1.SetPK("column1")
	table1.AddValues("row3", "row3_1")
	table1.AddValues("row4", "row4_1")
	table1.AcceptChange()
	table1.DeleteRow(0)

	if err := table.Merge(table1); err != nil {
		t.Error(err)
	}
	if table.RowCount() != 3 || table.GetChange().RowCount != 2 {
		t.Error("error", table.GetChange().RowCount)
	}
}

func TestClear(t *testing.T) {
	table := NewDataTable("table1")
	table.AddColumn(NewDataColumn("column1", reflect.TypeOf("")))
	table.AddColumn(NewDataColumn("column2", reflect.TypeOf("")))
	table.SetPK("column1")
	table.AddValues("row1", "row1_1")
	table.AddValues("row2", "row2_1")
	table.Clear()
	if table.RowCount() != 0 {
		t.Error("error")
	}
	table.AddValues("row3", "row3_1")
	table.AddValues("row4", "row4_1")
	if table.RowCount() != 2 {
		t.Error("error")
	}
}
func TestInterfaceColumn(t *testing.T) {
	table := NewDataTable("table1")
	table.AddColumn(NewDataColumn("column1", reflect.TypeOf("")))
	table.AddColumn(NewDataColumnN("column2", reflect.TypeOf("")))
	table.SetPK("column1")
	if err := table.AddValues("row1", "row1_1"); err != nil {
		t.Error(err)
	}
	if err := table.AddValues("row2", nil); err != nil {
		t.Error(err)
	}
	if table.Row(1)["column2"] != nil {
		t.Error("error")
	}
	if err := table.AddValues("row3", 1); err != nil {
		t.Error(err)
	}
}
