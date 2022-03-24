package test

import (
	"github.com/enustah/db-canal/canal/multi_canal"
	"github.com/enustah/db-canal/config"
	"github.com/enustah/db-canal/driver"
	"github.com/enustah/db-canal/hook"
	"github.com/enustah/db-canal/register"
	"github.com/enustah/db-canal/util"
	"testing"
)

/*
mysql:

create table test(
	id bigint,
	uid bigint unsigned ,
	test_int int,
	test_uint int unsigned ,
	test_str varchar(255),
	test_chr char(5),
	test_txt text,
	test_float float,
	test_double double,
	test_dec DECIMAL,
    test_enum enum('e1','e2','e3'),
	test_bit bit(2),
	test_date DATE,
	test_time time,
	test_datetime datetime,
	test_timstp timestamp,
	test_var_bin VARBINARY(255),
	test_bin BINARY(5),
	test_blob BLOB
);

insert into  test() values(
	-1152921504606846976,1152921504606846976,-1073741824,1073741824,
	'iamvarcahr','char','iamtext',
	1.234,2.345,1024.4096,'e1',1,
	'1989-06-04','23:59:59','1989-06-04 00:00:1','1989-06-04 00:00:1',
	0x123456789,
	0x01,
	0x111111111111
)
---------------------------------------------------------------------
ch:

create table test(
    id  Int64,
	uid UInt64 ,
	test_int Int32,
	test_uint UInt32,
	test_str String,
	test_chr String,
	test_txt String,
	test_float FLOAT,
	test_double DOUBLE,
	test_dec DOUBLE,
    test_enum String,
	test_bit Int16,
	test_date DATE,
	test_time DateTime,
	test_datetime DateTime,
	test_timstp DateTime,
	test_var_bin String(255),
	test_bin String,
	test_blob String,
	is_delete Bool
) engine=ReplacingMergeTree primary key (id,uid) order by (id,uid)

---------------------------------------------------------------------
es:

curl -X PUT -H "Content-type: application/json" http://172.17.0.2:9200/test --data '
{
    "settings":{
        "index":{
            "number_of_shards":1,
            "number_of_replicas":0
        }
    },
    "mappings":{
        "properties":{
            "id":{
                "type":"long"
            },
            "uid":{
                "type":"unsigned_long"
            },
            "test_int":{
                "type":"integer"
            },
            "test_uint":{
                "type":"unsigned_long"
            },
            "test_str":{
                "type":"keyword"
            },
            "test_chr":{
                "type":"keyword"
            },
            "test_txt":{
                "type":"keyword"
            },
            "test_float":{
                "type":"float"
            },
            "test_double":{
                "type":"double"
            },
            "test_dec":{
                "type":"double"
            },
            "test_enum":{
                "type":"keyword"
            },
            "test_bit":{
                "type":"byte"
            },
            "test_date":{
                "type":"date"
            },
            "test_time":{
                "type":"date"
            },
            "test_datetime":{
                "type":"date"
            },
            "test_timstp":{
                "type":"date"
            },
            "test_var_bin":{
                "type":"binary"
            },
            "test_bin":{
                "type":"binary"
            },
            "test_blob":{
                "type":"binary"
            }
        }
    }
}
'
*/

const mysqlChEs = `
config:
  - ingress:
      driver: mysql_ingress
      dsn: "172.21.0.2:3306"
      options:
        username: "root"
        password: "root"
        tables:
          - "test\\.test"
        savePointFilePath: "/tmp/a"

    canalConfig:
      name: test_mysql
      maxWaitTime: 1500
      maxDataBatch: 100
      retryOption:
        maxInterval: 3000 # in ms
        multiplier: 1.5
        initialInterval: 1000 # in ms
    
    
    egress:
      - driver: clickhouse_egress
        url: "tcp://172.17.0.3:9000?database=test&username=root&password=root"
        hookChain:
          - "clickhouseDelete(is_delete)"
          - "dataFilter(test,test,id,>,50)"
      - driver: elasticsearch_egress
        url: "https://127.0.0.1:1234"
        hookChain:
          - "dataFilter(test,test,id,>,30)"
        options:
          idColumn:
            test: id
          numWorkers: 1
          flushBytes: 10485760 # in bytes
          flushInterval: 3s
          tlsSni: a.ydx.com
          caCert: |
            -----BEGIN CERTIFICATE-----
            MIIDETCCAfkCFCqvQvVj+0QeOkESQQqRIa6QHrl3MA0GCSqGSIb3DQEBCwUAMEUx
            CzAJBgNVBAYTAkhLMQswCQYDVQQIDAJISzELMAkGA1UEBwwCSEsxDTALBgNVBAoM
            BHJvb3QxDTALBgNVBAsMBHJvb3QwHhcNMjIwMzIzMDg1NzM4WhcNMzIwMzIwMDg1
            NzM4WjBFMQswCQYDVQQGEwJISzELMAkGA1UECAwCSEsxCzAJBgNVBAcMAkhLMQ0w
            CwYDVQQKDARyb290MQ0wCwYDVQQLDARyb290MIIBIjANBgkqhkiG9w0BAQEFAAOC
            AQ8AMIIBCgKCAQEAx6Utqkix8MOF/pT3L4iT5nTtGU12QHbQpEaqBdE4JH0EhC17
            r38wrXN1GcRVRJdHXVU4XduAmfjizVWbYBX4d3gkYCCjK6pwQBzE8mv2BgbQTFeX
            ckks5CuejzM4wwbDifqcaj8BrURPJQY5sv2dB9vajzYi66Hf1py9bOfffMWwpc5G
            SJkYJppyWt/6FMih7M2VcUeR1jFHeSxrHvnIX/fKobdEO4z2W0PVnaQBvHYRLkjf
            KkRnOTiyXW3yo+sO/rWzzfsWVjAPf2qKlEQXJmyzSmHTcjO2FyFqQJ4+toiPTK9n
            TzsUHnzD5rdwB+/wAvSIQF6GMheBoRFXvmjFxQIDAQABMA0GCSqGSIb3DQEBCwUA
            A4IBAQA1IH7apGtxmKIHhlv2PuQnMDvOoPR6/DQfZWy7wHOYrTVTxc2FdmSb2mF8
            PJrfGXDOYObnpXyuTc77AHsD965DedoEaRUJ+/c7U9deqXmPgUqK/ogWE/M0qb4l
            kbd0S4cieYL+G8LSeBnt0hggxsEOK/PxahECMlc/gBf+VJzKDNz3sWOp1QxP3SVO
            3IzEyrR1zjt5HXPVUqUrZeTrjs3Ctx/ZyCeO3Kwm33AMpmCYY9+nLjrtOjHT9E/c
            tFYMXLVX+XixtK8mJ3C8uZF+dl8LsBfOONxlPo0WNSTR1iS26EgzrXFX1+hpm0Hl
            pK2YeoFHpRIlsMWZazZ+s6zNrIw6
            -----END CERTIFICATE-----

          
    
logLevel: "debug"
`

func TestMysqlChEs(t *testing.T) {
	util.Must(
		register.RegisterHook(
			func(ctx *hook.Ctx, args []interface{}) error {
				deleteStatusColumn := args[0].(string)
				ctx.ForEach(func(data *driver.Data) (drop bool, stop bool) {
					if data.Event == driver.EventInsert {
						data.RawMap[deleteStatusColumn] = 0
					} else if data.Event == driver.EventDelete {
						data.Event = driver.EventUpdate
						data.RawMap[deleteStatusColumn] = 1
					}
					return
				})
				return nil
			},
			"clickhouseDelete",
			[]hook.Arg{hook.ArgTypeStr},
		),
	)

	c, err := config.FromYaml(mysqlChEs)
	util.Must(err)
	cc, err := multi_canal.NewMultiCanal(c[0])
	util.Must(err)
	util.Must(cc.Run())
	<-(chan interface{})(nil)
}
