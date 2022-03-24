package clickhouse

import (
	"github.com/enustah/db-canal/config"
	"github.com/enustah/db-canal/driver"
	"github.com/enustah/db-canal/register"
	"github.com/enustah/db-canal/util"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

func init() {
	util.Must(register.RegisterEgressDriver("clickhouse_egress", &ClickhouseEgress{}))
}

type ClickhouseEgress struct {
	db *gorm.DB
}

func (c *ClickhouseEgress) Init(config config.EgressConfig) error {
	var err error
	c.db, err = gorm.Open(clickhouse.Open(config.Url), &gorm.Config{})
	return err
}

func (c *ClickhouseEgress) Start() error {
	_, err := c.db.Raw("select 1").Rows()
	return err
}

func (c *ClickhouseEgress) WriteData(dataBatch []*driver.Data) error {
	// map<tableName,rawMap[]>
	m := make(map[string][]map[string]interface{})
	for _, v := range dataBatch {
		// ignore delete
		if v.Event == driver.EventDelete {
			continue
		}
		_, ok := m[v.Table.Name]
		if !ok {
			m[v.Table.Name] = make([]map[string]interface{}, 0, len(dataBatch))
		}
		m[v.Table.Name] = append(m[v.Table.Name], v.RawMap)
	}
	return c.db.Transaction(func(tx *gorm.DB) error {
		var err error
		for k, v := range m {
			if err == nil {
				err = tx.Table(k).Create(v).Error
			}
		}
		return err
	})
}

func (c *ClickhouseEgress) Stop() {

}
