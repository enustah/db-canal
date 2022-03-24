package test

import (
	"github.com/enustah/db-canal/canal/multi_canal"
	"github.com/enustah/db-canal/config"
	"github.com/enustah/db-canal/driver"
	"github.com/enustah/db-canal/hook"
	"github.com/enustah/db-canal/register"
	"github.com/enustah/db-canal/util"
	"github.com/kr/pretty"
	"testing"
)

const (
	mysqlClickhouseConf = `
config:
  - ingress:
      driver: mysql_ingress
      dsn: "172.17.0.2:3306"
      options:
        username: "root"
        password: "root"
        #database:
        #  - "test"
        tables:
          - "test\\.test"
          - "test\\.test2"
        savePointFilePath: "/tmp/a"

    canalConfig:
      name: test_mysql
      maxWaitTime: 1500
      maxDataBatch: 500

      retryOption:
        maxInterval: 3000
        multiplier: 1.5
        initialInterval: 1000

    egress:
      - driver: clickhouse_egress
        url: "tcp://172.17.0.3:9000?database=test&username=root&password=root"
        hookChain: 
          - "hk1()"
          - "dataFilter(test,test,id,=,123)"
          - "dataFilter(test,test2,name,=,fff)"
          - "delay(1s)"
          - "hk2()"


logLevel: "info"

`
)

func TestMysqlIClickhouseO(t *testing.T) {
	util.Must(register.RegisterHook(
		func(ctx *hook.Ctx, args []interface{}) error {
			ctx.ForEach(func(data *driver.Data) (drop bool, stop bool) {
				data.Table.Name = "test2"
				if data.RawMap["name"] == "abort" {
					ctx.Abort()
					return false, true
				}
				return
			})
			return nil
		}, "hk1", nil))

	util.Must(register.RegisterHook(
		func(ctx *hook.Ctx, args []interface{}) error {
			ctx.ForEach(func(data *driver.Data) (drop bool, stop bool) {
				pretty.Println(data)
				return
			})
			return nil
		}, "hk2", nil))

	c, err := config.FromYaml(mysqlClickhouseConf)
	util.Must(err)
	cc, err := multi_canal.NewMultiCanal(c[0])
	util.Must(err)
	util.Must(cc.Run())

	<-(chan interface{})(nil)
}
