package driver

import "github.com/enustah/db-canal/util"

type (
	Event      string
	ColumnType int
)

const (
	EventInsert  Event = "insert"
	EventUpdate  Event = "update"
	EventDelete  Event = "delete"
	EventUnknown Event = "unknown"
)

const (
	ColumnTypeStruct  ColumnType = iota // RawMap value should return type of interface{}
	ColumnTypeUnknown                   // RawMap value should return type of interface{}
	ColumnTypeNumber                    // RawMap value should return type of int64
	ColumnTypeEnum                      // RawMap value should return type of string
	ColumnTypeFloat                     // RawMap value should return type of float64
	ColumnTypeString                    // RawMap value should return type of string
	ColumnTypeBytes                     // RawMap value should return type of []byte
	ColumnDatetime                      // RawMap value should return type of time.Time
)

// the type which must implement deepCopy
type deepCopyType interface {
	*Database | *Table | *Column | *Data
}

type DeepCopy[T deepCopyType] interface {
	DeepCopy() T
}

type Database struct {
	Name string
}

func (d *Database) DeepCopy() *Database {
	return &Database{
		Name: d.Name,
	}
}

type Table struct {
	Name   string
	Column []*Column
}

type Column struct {
	Name     string
	Type     ColumnType
	Metadata map[string]interface{}
}

func (c *Column) DeepCopy() *Column {
	return &Column{
		Name:     c.Name,
		Type:     c.Type,
		Metadata: util.DeepCopyMap(c.Metadata),
	}
}

func (t *Table) DeepCopy() *Table {
	column := make([]*Column, 0, len(t.Column))
	for _, v := range t.Column {
		column = append(column, v.DeepCopy())
	}
	return &Table{
		Name:   t.Name,
		Column: column,
	}
}

type Data struct {
	Event Event
	// raw map to the data. key should be raw column string
	RawMap   map[string]interface{}
	Table    *Table
	Database *Database
	// Metadata preserve for other use
	Metadata map[string]interface{}
}

func (d *Data) DeepCopy() *Data {
	return &Data{
		Event:    d.Event,
		RawMap:   util.DeepCopyMap(d.RawMap),
		Table:    d.Table.DeepCopy(),
		Database: d.Database.DeepCopy(),
		Metadata: util.DeepCopyMap(d.Metadata),
	}
}
