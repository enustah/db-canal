package test

import (
	"crypto/rand"
	"github.com/enustah/db-canal/canal/multi_canal"
	"github.com/enustah/db-canal/config"
	"github.com/enustah/db-canal/driver"
	"github.com/enustah/db-canal/hook"
	"github.com/enustah/db-canal/register"
	"github.com/enustah/db-canal/util"
	"testing"
	"time"
)

const fakeConf = `
config:
  - ingress:
      driver: fake_ingress
      options:
        o1: "asd"
        o2: "zxc"

    canalConfig:
      name: fake->fake
      maxWaitTime: 2000
      maxDataBatch: 30

      retryOption:
        maxInterval: 3000
        multiplier: 1.5
        initialInterval: 1000

    egress:
      - driver: fake_egress1
        options:
          o1: "11"
          o2: "22"
        hookChain:
          - "randomDrop()"
#          - "delay(10s)"
      - driver: fake_egress2
        options:
          o1: "11"
          o2: "22"
        hookChain:
          - "randomDrop()"


logLevel: "debug"
`

func TestCanal(t *testing.T) {
	util.Must(register.RegisterHook(func(ctx *hook.Ctx, args []interface{}) error {
		dropCount := 0
		ctx.ForEach(func(data *driver.Data) (drop bool, stop bool) {
			b := make([]byte, 1)
			rand.Read(b)
			if b[0]%2 == 0 {
				return false, false
			}
			dropCount++
			return true, false
		})
		util.GetLog().Infof("drop data count: %d", dropCount)
		return nil
	}, "randomDrop", []hook.Arg{}))

	c, err := config.FromYaml(fakeConf)
	util.Must(err)
	canal, err := multi_canal.NewMultiCanal(c[0])

	util.Must(err)
	util.Must(canal.Run())
	time.Sleep(10 * time.Second)
	canal.Stop()
	time.Sleep(2 * time.Second)
}
