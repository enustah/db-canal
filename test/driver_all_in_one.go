package test

import (
	_ "github.com/enustah/db-canal/driver/builtin/egress/clickhouse"
	_ "github.com/enustah/db-canal/driver/builtin/egress/elasticsearch"
	_ "github.com/enustah/db-canal/driver/builtin/ingress/mysql"
	"github.com/enustah/db-canal/register"
	"github.com/enustah/db-canal/util"
)

func init() {
	util.Must(register.RegisterIngressDriver("fake_ingress", &fakeIngressDriver{}))
	util.Must(register.RegisterEgressDriver("fake_egress", &fakeEgressDriver{}))
	util.Must(register.RegisterEgressDriver("fake_egress1", &fakeEgressDriver{}))
	util.Must(register.RegisterEgressDriver("fake_egress2", &fakeEgressDriver{}))
}
