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

const esConf = `
config:
  - ingress:
      driver: fake_ingress
      dsn: ""

    canalConfig:
      name: test_es
      maxWaitTime: 3000
      maxDataBatch: 10

      retryOption:
        maxInterval: 3000
        multiplier: 1.5
        initialInterval: 1000

    egress:
      - driver: elasticsearch_egress
        url: "http://172.17.0.3:9200"
        hookChain:
          - "hk()"
        options:
          idColumn:
            test: id
            fake_tb: fake_id

          extraAddr: 
           - http://172.17.0.3:9200

          username: fake
          password: fake
          numWorkers: 1
          flushBytes: 10485760 # in bytes
          flushInterval: 3s
          #proxy: http://127.0.0.1:8080
          


logLevel: "info"
`

/*

curl -X DELETE http://172.17.0.3:9200/test

curl -X PUT -H "Content-type: application/json" http://172.17.0.3:9200/test --data '
{
 "settings": {
    "index": {
      "number_of_shards": 1,
      "number_of_replicas": 0
    }
  },
  "mappings": {
    "properties": {
      "name": {
        "type": "keyword"
      }
    }
  }
}
'

*/

func TestEsEgress(t *testing.T) {
	util.Must(register.RegisterHook(func(ctx *hook.Ctx, args []interface{}) error {
		ctx.ForEach(func(data *driver.Data) (drop bool, stop bool) {
			// overwrite data
			data.Table.Name = "test"
			data.RawMap["id"] = i
			data.RawMap["name"] = "cc"
			i += 1

			pretty.Println(data)
			return false, false
		})
		return nil
	}, "hk", nil))

	c, err := config.FromYaml(esConf)
	util.Must(err)
	cc, err := multi_canal.NewMultiCanal(c[0])
	util.Must(err)
	util.Must(cc.Run())

	<-(chan interface{})(nil)
}
