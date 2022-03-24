package test

import (
	"github.com/enustah/db-canal/config"
	"github.com/enustah/db-canal/util"
	"github.com/kr/pretty"
	"testing"
)

const configStr = `
config:
  - ingress:
      driver: fake_ingress
      dsn: fake_dsn
      options:
        o1: "asd"
        o2: 
        - "asd"
        - "qwe"

    canalConfig:
      name: fake->fake
      maxWaitTime: 3000
      maxDataBatch: 10

      retryOption:
        maxInterval: 3000
        multiplier: 1.5
        initialInterval: 1000

    egress:
      - driver: fake_egress1
        url: fake_url
        options:
          o1: "11"
          o2: "22"
        hookChain:
          - "hk1(arg1)"
          - "hk2(arg2)"

logLevel: "debug"
`

func TestConfig(t *testing.T) {
	c, err := config.FromYaml(configStr)
	util.Must(err)
	pretty.Print(c)
}
