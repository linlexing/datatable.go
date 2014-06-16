datatable.go
============

A memory table, similar to. Net DataTable

http://godoc.org/github.com/linlexing/datatable.go

#### exemple for base:

```go
table := NewDataTable("Table1")
table.AddColumn(NewStringColumn("column1"))
table.AddColumn(NewInt64Column("column2"))
table.SetPK("column1")
table.AddRow(map[string]interface{}{
	"column1":"row1",
	"column2":int64(1),
})
table.AddValues("row2",int64(2))
row1 := table.GetValues(0)
row2 := table.GetRow(1)
```
#### exemple for find:

```go
table := NewDataTable("Table1")
table.AddColumn(NewStringColumn("column1"))
table.AddColumn(NewInt64Column("column2"))
table.SetPK("column1")
table.AddValues("row1",int64(1))
table.AddValues("row2",int64(2))
//only find the primary key's column
if foundIndex := table.Find("row1");foundIndex == -1{
	//not found
}else{
	foundRow := table.GetValue(foundIndex)
}
```
#### exemple for change:

```go
table := NewDataTable("Table1")
table.AddColumn(NewStringColumn("column1"))
table.AddColumn(NewInt64Column("column2"))
table.SetPK("column1")
table.AddValues("row1",int64(1))
table.AddValues("row2",int64(2))
table.SetValues(0,"row3",int64(3))
table.DeleteRow(1)
if table.HasChange() {
	data := table.GetChange()
	/*apply to database,the result is TableChange struct:
	type ChangeRow struct {
		Data       []interface{}
		OriginData []interface{}
	}

	type TableChange struct {
		InsertRows []*ChangeRow
		UpdateRows []*ChangeRow
		DeleteRows []*ChangeRow
		RowCount   int
	}*/
}
```
