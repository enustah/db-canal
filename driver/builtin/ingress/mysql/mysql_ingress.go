package mysql

import (
	"bufio"
	"context"
	"fmt"
	"github.com/enustah/db-canal/config"
	"github.com/enustah/db-canal/driver"
	"github.com/enustah/db-canal/register"
	"github.com/enustah/db-canal/util"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/kr/pretty"
	"github.com/mitchellh/mapstructure"
	"github.com/shopspring/decimal"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

func init() {
	util.Must(register.RegisterIngressDriver("mysql_ingress", &MysqlIngress{}))
}

const (
	metadataTimeFmtStrKey = "time_fmt_str"
	metadataEnumKey       = "enum"
)

type mysqlEventHandler struct {
	canal.DummyEventHandler
	ctx        context.Context
	dataChan   chan<- *driver.Data
	curLogName string
}

func newMysqlEventHandler(dataChan chan<- *driver.Data, ctx context.Context) *mysqlEventHandler {
	return &mysqlEventHandler{
		dataChan: dataChan,
		ctx:      ctx,
	}
}

func (h *mysqlEventHandler) SetLogName(logName string) {
	h.curLogName = logName
}

func (h *mysqlEventHandler) OnRow(e *canal.RowsEvent) error {
	data := convertRowEventToData(e, h.curLogName)
	for _, v := range data {
		select {
		case <-h.ctx.Done():
			return nil
		default:
			h.dataChan <- v
		}
	}
	return nil
}

func (h *mysqlEventHandler) OnRotate(rotateEvent *replication.RotateEvent) error {
	h.SetLogName(string(rotateEvent.NextLogName))
	return nil
}

func (h *mysqlEventHandler) String() string {
	return "mysqlEventHandler"
}

type MysqlIngress struct {
	savePointPath string
	savePoint     mysql.Position
	cfg           *canal.Config
	canal         *canal.Canal
	dataChan      chan *driver.Data

	lock        *sync.Mutex
	ctx         context.Context
	mainLoopCtx context.Context
	cancelFunc  func()
}

type mysqlIngressOption struct {
	Username          string   `mapstructure:"username"`
	Password          string   `mapstructure:"password"`
	Tables            []string `mapstructure:"tables"`
	SavePointFilePath string   `mapstructure:"savePointFilePath"`
}

func (m *MysqlIngress) getFirstPosition() (mysql.Position, error) {
	p := mysql.Position{}
	r, err := m.canal.Execute("SHOW BINARY LOGS")
	if err != nil {
		return p, err
	}
	p.Name, err = r.GetString(0, 0)
	if err == nil {
		var pos uint64
		pos, err = r.GetUint(0, 1)
		p.Pos = uint32(pos)
	}
	return p, err
}

func (m *MysqlIngress) Init(config config.IngressConfig) error {
	option := &mysqlIngressOption{}
	if err := mapstructure.Decode(config.Options, option); err != nil {
		return err
	}
	cfg := canal.NewDefaultConfig()
	cfg.Addr = config.Dsn
	cfg.User = option.Username
	cfg.Password = option.Password
	cfg.UseDecimal = true
	cfg.Dump.ExecutionPath = ""
	if len(option.Tables) != 0 {
		cfg.IncludeTableRegex = option.Tables
	}
	m.savePointPath = option.SavePointFilePath
	m.cfg = cfg
	m.lock = &sync.Mutex{}
	return nil
}

func (m *MysqlIngress) Start() (<-chan *driver.Data, error) {
	f, err := os.OpenFile(m.savePointPath, os.O_RDWR|os.O_CREATE, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	m.savePoint, err = m.getSavePoint(f)
	if err != nil {
		return nil, err
	}

	m.dataChan = make(chan *driver.Data)
	m.ctx, m.cancelFunc = context.WithCancel(context.TODO())
	var (
		mainLoopCancelFunc func()
		eventHandler       = newMysqlEventHandler(m.dataChan, m.ctx)
	)
	m.mainLoopCtx, mainLoopCancelFunc = context.WithCancel(context.TODO())

	go func() {
		defer mainLoopCancelFunc()
		for {
			select {
			case <-m.ctx.Done():
				return
			default:
				c, err := canal.NewCanal(m.cfg)
				if err != nil {
					util.GetLog().Errorf("mysql canal run fail: %v", err)
					time.Sleep(1 * time.Second)
					continue
				}
				m.canal = c
				if m.savePoint.Name == "" {
					p, err := m.getFirstPosition()
					if err != nil {
						util.GetLog().WithField("error", err).Errorf("canal get first binlog position fail")
						time.Sleep(1 * time.Second)
						continue
					}
					eventHandler.SetLogName(p.Name)
				} else {
					eventHandler.SetLogName(m.savePoint.Name)
				}
				m.canal.SetEventHandler(eventHandler)
				err = m.canal.RunFrom(m.savePoint)
				if err != nil {
					util.GetLog().Errorf("mysql canal run fail: %v", err)
				}
				m.closeCanal()
				time.Sleep(300 * time.Millisecond)
			}
		}

	}()
	return m.dataChan, nil
}

func (m *MysqlIngress) SavePoint(data *driver.Data) error {
	if data.Metadata != nil {
		if _pos, ok := data.Metadata["nextPos"]; ok {
			pos := _pos.(mysql.Position)
			return ioutil.WriteFile(m.savePointPath, []byte(fmt.Sprintf("%s:%d", pos.Name, pos.Pos)), 0)
		}
	}
	return nil
}

func (m *MysqlIngress) Stop() {
	m.cancelFunc()
closeLoop:
	for {
		select {
		case <-m.mainLoopCtx.Done():
			break closeLoop
		default:
			m.closeCanal()
		}
		time.Sleep(1 * time.Second)
	}
	close(m.dataChan)
}

func (m *MysqlIngress) getSavePoint(r io.Reader) (mysql.Position, error) {
	p := mysql.Position{}
	l, _, err := bufio.NewReader(r).ReadLine()
	if err != nil || len(l) == 0 {
		return p, util.IgnoreEOFErr(err)
	}
	s := strings.Split(string(l), ":")
	if len(s) != 2 {
		return p, fmt.Errorf("read mysql position fail. can not parse `%s`", string(l))
	}
	name := s[0]
	position := s[1]
	i, err := strconv.ParseInt(position, 10, 64)
	if err != nil {
		return p, err
	}
	p.Pos = uint32(i)
	p.Name = name
	return p, nil
}
func (m *MysqlIngress) closeCanal() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.canal != nil {
		m.canal.Close()
		m.canal = nil
	}
}
func convertRowEventToData(event *canal.RowsEvent, curLogName string) []*driver.Data {
	table := &driver.Table{
		Name:   event.Table.Name,
		Column: make([]*driver.Column, 0, len(event.Table.Columns)),
	}
	database := &driver.Database{
		Name: event.Table.Schema,
	}
	// type map reference go-mysql-org/go-mysql@v1.4.0/replication/row_event.go
	// all unsigned int will map to int64, it may be out of int64 range if unsigned int too big.
	convertColumn(table, event.Table.Columns)
	var dataEvent driver.Event
	switch event.Action {
	case canal.InsertAction:
		dataEvent = driver.EventInsert
	case canal.UpdateAction:
		dataEvent = driver.EventUpdate
	case canal.DeleteAction:
		dataEvent = driver.EventDelete
	default:
		util.GetLog().WithField("event", event.Action).Warnf("get unknown event")
		dataEvent = driver.EventUnknown
	}
	data := make([]*driver.Data, 0, len(event.Rows))
	for _, r := range event.Rows {
		row := make(map[string]interface{})
		for i, v := range r {
			column := table.Column[i]
			row[column.Name] = convertColumnValue(column, v)
		}
		d := &driver.Data{
			Event:    dataEvent,
			RawMap:   row,
			Table:    table,
			Database: database,
			Metadata: map[string]interface{}{},
		}
		data = append(data, d)
	}
	/*
		due to row event can pick more tha one row, metadata only record the position in last row.
		SavePoint only save position when metadata["nextPos"] exist.
	*/
	lastRow := data[len(data)-1]
	lastRow.Metadata["nextPos"] = mysql.Position{
		Name: curLogName,
		Pos:  event.Header.LogPos,
	}
	return data
}

// convert mysql column type to driver column type
func convertColumn(table *driver.Table, column []schema.TableColumn) {
	for _, v := range column {
		var timeFmtStr string
		setTimeFmtStr := func(fmt string) {
			if timeFmtStr == "" {
				timeFmtStr = fmt
			}
		}

		switch v.Type {
		case schema.TYPE_NUMBER,
			schema.TYPE_SET,
			schema.TYPE_BIT,
			schema.TYPE_MEDIUM_INT:
			table.Column = append(table.Column, &driver.Column{
				Name: v.Name,
				Type: driver.ColumnTypeNumber,
			})
		case schema.TYPE_ENUM:
			table.Column = append(table.Column, &driver.Column{
				Name: v.Name,
				Type: driver.ColumnTypeEnum,
				Metadata: map[string]interface{}{
					metadataEnumKey: v.EnumValues,
				},
			})
		case schema.TYPE_FLOAT,
			schema.TYPE_DECIMAL:
			table.Column = append(table.Column, &driver.Column{
				Name: v.Name,
				Type: driver.ColumnTypeFloat,
			})
		case schema.TYPE_STRING,
			schema.TYPE_JSON:
			table.Column = append(table.Column, &driver.Column{
				Name: v.Name,
				Type: driver.ColumnTypeString,
			})
		case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
			setTimeFmtStr("2006-01-02 15:04:05")
			fallthrough
		case schema.TYPE_DATE:
			setTimeFmtStr("2006-01-02")
			fallthrough
		case schema.TYPE_TIME:
			setTimeFmtStr("15:04:05")
			table.Column = append(table.Column, &driver.Column{
				Name: v.Name,
				Type: driver.ColumnDatetime,
				Metadata: map[string]interface{}{
					metadataTimeFmtStrKey: timeFmtStr,
				},
			})
		case schema.TYPE_BINARY, schema.TYPE_POINT:
			table.Column = append(table.Column, &driver.Column{
				Name: v.Name,
				Type: driver.ColumnTypeBytes,
			})
		default:
			util.GetLog().WithField("column", pretty.Sprint(v)).Warnf("unknown row type")
			table.Column = append(table.Column, &driver.Column{
				Name: v.Name,
				Type: driver.ColumnTypeUnknown,
			})
		}
	}
}

func convertColumnValue(column *driver.Column, value interface{}) interface{} {
	handleNil := func(tZeroVal interface{}) {
		if value == nil {
			value = reflect.Zero(reflect.TypeOf(tZeroVal)).Interface()
		}
	}

	switch column.Type {
	case driver.ColumnTypeNumber:
		handleNil(0)
		return util.ConvertIntegerTo64(value)
	case driver.ColumnTypeEnum:
		if value == nil {
			return ""
		}
		enumIdx := reflect.ValueOf(value).Int()
		if column.Metadata[metadataEnumKey] == nil || len(column.Metadata[metadataEnumKey].([]string)) < int(enumIdx) {
			util.GetLog().WithField("value", pretty.Sprint(value)).Warnf("can not cast enum value")
			return ""
		}
		return column.Metadata[metadataEnumKey].([]string)[enumIdx]
	case driver.ColumnTypeFloat:
		handleNil(float64(0))
		switch v := value.(type) {
		case float32:
			f, _ := decimal.NewFromFloat32(v).Float64()
			return f
		case float64:
			return v
		case decimal.Decimal:
			f, _ := v.Float64()
			return f
		default:
			util.GetLog().WithField("value", pretty.Sprint(value)).Warnf("can not cast float value")
			return float64(0)
		}
	case driver.ColumnTypeString:
		handleNil("")
		switch v := value.(type) {
		case string:
			return reflect.ValueOf(value).String()
		case []byte:
			return string(v)
		default:
			util.GetLog().WithField("value", pretty.Sprint(value)).Warnf("can not cast string value")
			return ""
		}

	case driver.ColumnTypeBytes:
		handleNil([]byte{})
		switch v := value.(type) {
		case []byte:
			return v
		case string:
			return []byte(v)
		default:
			util.GetLog().WithField("value", pretty.Sprint(value)).Warnf("can not cast bytes value")
			return []byte{}
		}
	case driver.ColumnDatetime:
		handleNil(time.Time{})
		switch v := value.(type) {
		case time.Time:
			return v
		case string:
			t, err := time.ParseInLocation(column.Metadata[metadataTimeFmtStrKey].(string), v, time.Local)
			if err != nil {
				util.GetLog().WithField("value", pretty.Sprint(value)).Warnf("can not cast datetime value")
			}
			return t
		default:
			util.GetLog().WithField("value", pretty.Sprint(value)).Warnf("can not cast datetime value")
			return time.Time{}
		}
	default:
		return value
	}
}
