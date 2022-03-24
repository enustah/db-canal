package test

import (
	"github.com/enustah/db-canal/config"
	"github.com/enustah/db-canal/driver"
	"github.com/enustah/db-canal/util"
	"github.com/kr/pretty"
	"time"
)

type fakeIngressDriver struct {
	ch  chan *driver.Data
	ctx chan interface{}
}

func (f *fakeIngressDriver) Init(config config.IngressConfig) error {
	util.GetLog().WithField("config", pretty.Sprint(config)).Infof("ingress drvier fake_ingress init")
	return nil
}

func (f *fakeIngressDriver) Start() (<-chan *driver.Data, error) {
	f.ch = make(chan *driver.Data)
	f.ctx = make(chan interface{})
	util.GetLog().Infof("ingress drvier fake_ingress start")
	go func() {
		t := time.Tick(500 * time.Millisecond)
		i := 0
		for {
			select {
			case <-t:
				f.ch <- &driver.Data{
					Event: driver.EventInsert,
					RawMap: map[string]interface{}{
						"id":   int64(1),
						"name": "xxx",
					},
					Table: &driver.Table{
						Name: "ttt",
						Column: []*driver.Column{
							{
								Name: "id",
								Type: driver.ColumnTypeNumber,
							},
							{
								Name: "name",
								Type: driver.ColumnTypeString,
							},
						},
					},
					Database: &driver.Database{},
					Metadata: map[string]interface{}{
						"i": i,
					},
				}
			case <-f.ctx:
				return
			}
			i++
		}

	}()
	return f.ch, nil
}

func (f *fakeIngressDriver) SavePoint(data *driver.Data) error {
	util.GetLog().Infof("ingress drvier fake_ingress save: %d", data.Metadata["i"])
	return nil
}

func (f *fakeIngressDriver) Stop() {
	util.GetLog().Infof("ingress drvier fake_ingress stop")
	close(f.ctx)
}

type fakeEgressDriver struct {
}

func (f *fakeEgressDriver) Init(config config.EgressConfig) error {
	util.GetLog().Infof("egress drvier fake_egress init %s", pretty.Sprint(config))
	return nil
}

func (f *fakeEgressDriver) Start() error {
	util.GetLog().Infof("egress drvier fake_egress start")
	return nil
}

func (f *fakeEgressDriver) WriteData(dataBatch []*driver.Data) error {
	util.GetLog().Debugf("egress get data %s", pretty.Sprint(dataBatch))
	// if time.Now().UnixNano()%2 == 0 {
	// 	util.GetLog().Infof("test egress drvier fake_egress write fail")
	// 	return errors.New("fake write fail")
	// }
	util.GetLog().Infof("egress drvier fake_egress write data")
	return nil
}

func (f *fakeEgressDriver) Stop() {
	util.GetLog().Infof("egress drvier fake_egress stop")
}
