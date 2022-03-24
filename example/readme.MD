## 配置示例

mysql_ingress clickhouse_egress elasticsearch_egress 是内置实现的驱动

```yaml
#config 是一个数组 表示每个canal示例
config:
  - ingress:
      #驱动名称 以内置mysql为例
      driver: mysql_ingress
      # mysql地址
      dsn: "172.21.0.2:3306"
      #驱动option 由驱动实现解释并处理
      options:
        username: "root"
        password: "root"
        # filter databases and tables
        #只有配置的表会被同步 填正则表达式  
        #具体可以参考 https://github.com/go-mysql-org/go-mysql/blob/master/canal/config.go  IncludeTableRegex 字段的注释说明
        tables:
          - "db\\.table"      #db库table表
          - "db2\\.table2"

        # the file to save the mysql log position
        #记录保存点的文件,第一次运行没有会自动创建,如果是容器应该使用挂载目录
        savePointFilePath: "/tmp/a"

    #canal配置 
    #canal接收到数据会等待一段时间,或者等到一定数据量才会写到输出源
    canalConfig:
      # only debug purpose
      name: test_mysql
      # time to wait data batch, inspire by elasticsearch refresh. in ms. if <=0, will wait to reach maxDataBatch.
      # canal 每批数据等待最长时间 单位毫秒, 类似elasticsearch refresh. <=0 会等待数据量直到maxDataBatch
      maxWaitTime: 1500
      #max data batch. 0 or 1 will write to output immediately.
      maxDataBatch: 100

      #exponential backoff config. detail can refer https://github.com/cenkalti/backoff
      #指数时间重试option 每次失败后重试时间约等于 max(initialInterval*multiplier*n,maxInterval) n是重试次数 
      #具体可以参考 https://github.com/cenkalti/backoff
      retryOption:
        #最大重试间隔
        maxInterval: 3000 # in ms
        #重试时间倍乘因子
        multiplier: 1.5
        #初始时间间隔
        initialInterval: 1000 # in ms

    #输出源, 数组, 可以配置多个输出源. 所有输出源写入成功才视为写入成功
    egress:
      - driver: clickhouse_egress
        url: "tcp://172.17.0.2:9000?database=test&username=root&password=root"

timeLocation: "Asia/Shanghai"
logLevel: "info"
```

## mysql同步到clickhouse

如果字段名称和字段类型对应 只需要配置和少量代码. mysql的删除操作在clickhouse会忽略. clickhouse对于数据的编辑操作, 一般是用replacingMergeTree引擎, 更新数据时插入新数据,
查询时使用argMax对主键去从. clickhouse实现逻辑删除参考后面的hook例子.

yaml配置参考上面配置示例

```go
package main

import (
	"fmt"
	"github.com/enustah/db-canal/canal/multi_canal"
	"github.com/enustah/db-canal/config"
	// 注册驱动
	_ "github.com/enustah/db-canal/driver/builtin/egress/clickhouse"
	_ "github.com/enustah/db-canal/driver/builtin/ingress/mysql"
	"os"
)

func main() {
	f, err := os.ReadFile("mysql_clickhouse.yaml")
	if err != nil {
		panic(fmt.Errorf("read config file fail: %v", err))
	}
	conf, err := config.FromYaml(string(f))
	if err != nil {
		panic(fmt.Errorf("parse config fail: %v", err))
	}
	canal, err := multi_canal.NewMultiCanal(conf[0])
	if err != nil {
		panic(fmt.Errorf("create canal fail: %v", err))
	}
	if err := canal.Run(); err != nil {
		panic(fmt.Errorf("canal start fail: %v", err))
	}
	<-(chan interface{})(nil)
}
```

以上代码和相应配置可以在 [mysql_clickhouse](../example) 找到

## hook chain

有时候需要对数据进行一定的处理, 典型情况就是类型转换, 一对多同步等情况. 可以通过hook实现. hook会在写入输出源之前调用. 有两个内置注册的hook 函数 分别是delay(str) 和 dataFilter(
dbName,tableName,columnName,operator,date)

delay 只是简单sleep一段时间 例如 delay(10s) 会sleep 10秒

dataFilter 用于过滤一些数据 例如 dataFilter(db,user,id,>,10) 则会过滤掉db库user表id>10的数据 相当于只有id<=10的数据会被同步. 
data 要和列的类型对应,只能是字符串,整数,浮点, 时间类型的列会转成时间戳比较.
dbName和tableName 填 - 表示匹配所有, 例如 dataFilter(-,-,id,>,10)

这个内置hook注册代码在 [预注册hook](../register/init.go)

### 注册自定义hook函数

```go
package main

import (
	"github.com/enustah/db-canal/driver"
	"github.com/enustah/db-canal/hook"
	"github.com/enustah/db-canal/register"
	"github.com/enustah/db-canal/util"
)

func init() {
	util.Must(
		// 注册hook函数 可以注册多个
		register.RegisterHook(
			// args 表示从配置传递过来的参数
			// 参数数量和类型和下面配置的hook.Arg一致
			func(ctx *hook.Ctx, args []interface{}) error {
				// 获取配置的参数
				// arg1:=args[0].(string)
				// arg2:=args[1].(float64)
				// arg3:=args[2].(int64)

				// 遍历所有数据 drop 返回true 表示丢弃数据, stop返回true表示停止遍历
				ctx.ForEach(func(data *driver.Data) (drop bool, stop bool) {
					// do sth with data
					return
				})

				// 和gin类似 Next() 可以执行下一个hook
				// ctx.Next()

				// 和gin类似 Abort()会停止hook执行
				// ctx.Abort()

				// 返回错误会整个hook chain重试, 当前对数据的处理会被保留,
				// 尽量不要返回错误, 并且不能返回永久性错误
				return nil
			},

			// hook 名称
			"customHook",

			// hook参数类型,用于验证参数正确性
			[]hook.Arg{hook.ArgTypeStr, hook.ArgTypeFloat, hook.ArgTypeInt},

			// option args validate
			// func(args []interface{}) error {
			//
			// },
		))
}
```

这部分代码在 [customHook](../example/hook/hook.go)

一个实用的hook例子 clickhouse逻辑删除, deleteStatusColumn是删除状态列, 1 表示已删除.

```go
package main

import (
	"github.com/enustah/db-canal/driver"
	"github.com/enustah/db-canal/hook"
	"github.com/enustah/db-canal/register"
	"github.com/enustah/db-canal/util"
)

func init() {
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
}
```
这部分代码在 [mysql_ch_es_test.go](../test/mysql_ch_es_test.go)

egress需要添加hook chain

```yaml
egress:
  - driver: clickhouse_egress
    url: "tcp://172.17.0.2:9000?database=test&username=root&password=root"
    hookChain:
      - "delay(1s)"                     # 内置hook
      - "dateFilter(name,=,ttt)"        # 内置hook
      - "clickhouseDelete(is_delete)"   # 逻辑删除
      - "customHook(arg1,89.64,8964)"   # 自定义hook 参数类型和数量要和注册的hook.Arg对应
```

## 多库同步 mysql 同步到 clickhouse 和 elasticsearch

一个输入源可以同步到多个输出源,只有所有输出源都写成功才会继续,否则会一直重试. 以mysql同步到es和clickhouse为例, yaml只需要修改egress添加输出源


```yaml
egress:
  #ch 输出源
  - driver: clickhouse_egress
    url: "tcp://172.17.0.2:9000?database=test&username=root&password=root"
    #    hookChain:
    #      - "delay(1s)"                     # 内置hook
    #      - "dateFilter(name,=,ttt)"        # 内置hook

  #es 输出源
  - driver: elasticsearch_egress
    #多个url可以用逗号(,)隔开
    url: "http://172.17.0.3:9200,http://172.17.0.4:9200"
    #option 配置参考 driver/builtin/egress/elasticsearch/elasticsearch_egress.go esEgressOption 结构
    options:
      #映射到elasticsearch的id字段
      #由于各种原因 canal有可能会对数据重复写,es在这里用于id去重
      #ps: 对于clickhouse这种数据库 可以使用replacingMergeTree表引擎 但实际需要根据情况选择.
      idColumn:
        test: id          #表示 test表的id字段名id
        fake_tb: fake_id  #表示 fake_tb表的id字段名fake_id
      #是否忽略删除不存在文档的错误
      ignoreDelete404: true 
      
      username: fake
      password: fake
      numWorkers: 1
      flushBytes: 10485760 # in bytes
      flushInterval: 3s
      
      #以下是tls相关配置
      # tls sni
      #tlsSni: a.ydx.com  
      # 是否跳过证书校验
      #skipCertVerify: false
      # ca 证书
      #caCert: |
      #    -----BEGIN CERTIFICATE-----
      #    MIIDETCCAfkCFCqvQvVj+0QeOkESQQqRIa6QHrl3MA0GCSqGSIb3DQEBCwUAMEUx
      #    CzAJBgNVBAYTAkhLMQswCQYDVQQIDAJISzELMAkGA1UEBwwCSEsxDTALBgNVBAoM
      #    BHJvb3QxDTALBgNVBAsMBHJvb3QwHhcNMjIwMzIzMDg1NzM4WhcNMzIwMzIwMDg1
      #    NzM4WjBFMQswCQYDVQQGEwJISzELMAkGA1UECAwCSEsxCzAJBgNVBAcMAkhLMQ0w
      #    CwYDVQQKDARyb290MQ0wCwYDVQQLDARyb290MIIBIjANBgkqhkiG9w0BAQEFAAOC
      #    AQ8AMIIBCgKCAQEAx6Utqkix8MOF/pT3L4iT5nTtGU12QHbQpEaqBdE4JH0EhC17
      #    r38wrXN1GcRVRJdHXVU4XduAmfjizVWbYBX4d3gkYCCjK6pwQBzE8mv2BgbQTFeX
      #    ckks5CuejzM4wwbDifqcaj8BrURPJQY5sv2dB9vajzYi66Hf1py9bOfffMWwpc5G
      #    SJkYJppyWt/6FMih7M2VcUeR1jFHeSxrHvnIX/fKobdEO4z2W0PVnaQBvHYRLkjf
      #    KkRnOTiyXW3yo+sO/rWzzfsWVjAPf2qKlEQXJmyzSmHTcjO2FyFqQJ4+toiPTK9n
      #    TzsUHnzD5rdwB+/wAvSIQF6GMheBoRFXvmjFxQIDAQABMA0GCSqGSIb3DQEBCwUA
      #    A4IBAQA1IH7apGtxmKIHhlv2PuQnMDvOoPR6/DQfZWy7wHOYrTVTxc2FdmSb2mF8
      #    PJrfGXDOYObnpXyuTc77AHsD965DedoEaRUJ+/c7U9deqXmPgUqK/ogWE/M0qb4l
      #    kbd0S4cieYL+G8LSeBnt0hggxsEOK/PxahECMlc/gBf+VJzKDNz3sWOp1QxP3SVO
      #    3IzEyrR1zjt5HXPVUqUrZeTrjs3Ctx/ZyCeO3Kwm33AMpmCYY9+nLjrtOjHT9E/c
      #    tFYMXLVX+XixtK8mJ3C8uZF+dl8LsBfOONxlPo0WNSTR1iS26EgzrXFX1+hpm0Hl
      #    pK2YeoFHpRIlsMWZazZ+s6zNrIw6
      #    -----END CERTIFICATE-----
#    hookChain:
#      - "delay(1s)"                     # 内置hook
#      - "dateFilter(name,=,ttt)"        # 内置hook
```

更具体的例子 可以参考代码[mysql_to_ch_es](../test/mysql_ch_es_test.go)

## 实现EgressDriver

同步到其他数据库需要实现egressDriver接口 实现很简单 参考 [clickhouse egress](../driver/builtin/egress/clickhouse/clickhouse_egress.go) 和 [elasticsearch egress](../driver/builtin/egress/elasticsearch/elasticsearch_egress.go) 和 

## 实现IngressDriver

TODO