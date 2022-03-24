package main

import (
	"fmt"
	"github.com/enustah/db-canal/canal/multi_canal"
	"github.com/enustah/db-canal/config"
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
