package test

import (
	"errors"
	"fmt"
	"github.com/enustah/db-canal/canal/multi_canal"
	"github.com/enustah/db-canal/config"
	"github.com/enustah/db-canal/driver"
	"github.com/enustah/db-canal/hook"
	"github.com/enustah/db-canal/register"
	"github.com/enustah/db-canal/util"
	"testing"
)

const mysqlConf = `
config:
  - ingress:
      driver: mysql_ingress
      dsn: "172.17.0.2:3306"
      options:
        username: "root"
        password: "root"
        database:
          - "test"
        tables:
          - "test"
        savePointFilePath: "/tmp/a"



    canalConfig:
      name: test_mysql
      maxWaitTime: 0
      maxDataBatch: 5

      retryOption:
        maxInterval: 3000
        multiplier: 1.5
        initialInterval: 1000

    egress:
      - driver: fake_egress
        hookChain:
          - "hk()"


logLevel: "info"

`

func TestMysqlIngressDriver(t *testing.T) {
	// util.Must(register.RegisterHook(func(ctx *hook.Ctx, args []interface{}) error {
	// 	ctx.ForEach(func(data *driver.Data) (drop bool, next bool) {
	// 		fmt.Println("ppp")
	// 		pretty.Println(data)
	// 		return false, true
	// 	})
	// 	return nil
	// }, "hk", nil))
	i := 0
	util.Must(register.RegisterHook(func(ctx *hook.Ctx, args []interface{}) error {
		ctx.ForEach(func(data *driver.Data) (drop bool, stop bool) {
			fmt.Println("=")
			return
		})
		if i == 3 {
			return nil
		}
		i += 1
		return errors.New("")
	}, "hk", nil))

	c, err := config.FromYaml(mysqlConf)
	util.Must(err)
	cc, err := multi_canal.NewMultiCanal(c[0])
	util.Must(err)
	util.Must(cc.Run())

	<-(chan interface{})(nil)
}
