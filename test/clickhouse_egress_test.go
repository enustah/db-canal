package test

import (
	"github.com/enustah/db-canal/canal/multi_canal"
	"github.com/enustah/db-canal/config"
	"github.com/enustah/db-canal/driver"
	"github.com/kr/pretty"

	"github.com/enustah/db-canal/hook"
	"github.com/enustah/db-canal/register"
	"github.com/enustah/db-canal/util"
	"testing"
)

const clickhouseConf = `
config:
  - ingress:
      driver: fake_ingress
      dsn: ""

    canalConfig:
      name: test_ch
      maxWaitTime: 3000
      maxDataBatch: 10

      retryOption:
        maxInterval: 3000
        multiplier: 1.5
        initialInterval: 1000

    egress:
      - driver: clickhouse_egress
        url: "tcp://172.17.0.2:9000?database=test&username=root&password=root"
        hookChain:
          - "hk()"


logLevel: "info"
`

/*
CREATE TABLE test
(
    `id` UInt64,
    `name` String
)
ENGINE = MergeTree
PRIMARY KEY id
ORDER BY id
*/

var i int64 = 0

func TestClickhouseEgress(t *testing.T) {
	util.Must(register.RegisterHook(func(ctx *hook.Ctx, args []interface{}) error {
		ctx.ForEach(func(data *driver.Data) (drop bool, stop bool) {
			// overwrite data
			data.Table.Name = "test"
			data.RawMap["id"] = i
			data.RawMap["name"] = "tttaaa"
			i += 1

			pretty.Println(data)
			return false, false
		})
		return nil
	}, "hk", nil))

	c, err := config.FromYaml(clickhouseConf)
	util.Must(err)
	cc, err := multi_canal.NewMultiCanal(c[0])
	util.Must(err)
	util.Must(cc.Run())

	<-(chan interface{})(nil)
}
