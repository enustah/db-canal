package multi_canal

import (
	"github.com/cenkalti/backoff/v4"
	"github.com/enustah/db-canal/canal"
	"github.com/enustah/db-canal/config"
	"github.com/enustah/db-canal/driver"
	"github.com/enustah/db-canal/hook"
	"github.com/enustah/db-canal/register"
	"github.com/enustah/db-canal/util"
	"github.com/kr/pretty"
	"sync"
	"time"
)

type MultiCanalBuilder struct {
	err    error
	canal  *MultiCanal
	config *config.Config
}

func newMultiCanalBuilder(canal *MultiCanal, config *config.Config) canal.CanalBuilder {
	return &MultiCanalBuilder{
		canal:  canal,
		config: config,
	}
}
func (m *MultiCanalBuilder) initCanalConfig() {
	util.GetLog().Debugf("multi canal %s config init", m.config.CanalConfig.Name)
	defaultConf := config.GetDefaultCanalConfig()
	conf := m.config.CanalConfig
	if conf.MaxDataBatch == 0 {
		conf.MaxDataBatch = defaultConf.MaxDataBatch
	}
	if conf.MaxWaitTime == 0 {
		conf.MaxWaitTime = defaultConf.MaxWaitTime
	}
	if conf.RetryOption.InitialInterval == 0 {
		conf.RetryOption.InitialInterval = defaultConf.RetryOption.InitialInterval
	}

	if conf.RetryOption.Multiplier == 0 {
		conf.RetryOption.Multiplier = defaultConf.RetryOption.Multiplier
	}
	if conf.RetryOption.MaxInterval == 0 {
		conf.RetryOption.MaxInterval = defaultConf.RetryOption.MaxInterval
	}
	util.GetLog().WithField("canal", conf.Name).
		WithField("result", pretty.Sprint(m.config.CanalConfig)).
		Debugf("multi canal config init")
}
func (m *MultiCanalBuilder) BuildInput() {
	if m.err == nil {
		util.GetLog().WithField("canal", m.config.CanalConfig.Name).
			WithField("driver", m.config.Ingress.Driver).
			Debugf("multi canal init input with ingress driver")
		var iDriver interface{}
		iDriver, m.err = register.RegisterGetDriver(m.config.Ingress.Driver, driver.TypeIngress)
		if m.err != nil {
			return
		}
		ingressDriver := iDriver.(driver.IngressDriver)

		if m.err = ingressDriver.Init(m.config.Ingress); m.err != nil {
			return
		}
		m.canal.input = &input{
			ingressDriver: ingressDriver,
			driverName:    m.config.Ingress.Driver,
		}
	}
}

func (m *MultiCanalBuilder) BuildOutput() {
	if m.err == nil {
		util.GetLog().WithField("canal", m.config.CanalConfig.Name).Debugf("multi canal outpu init")
		outputs := make([]*output, 0, len(m.config.Egress))
		for _, v := range m.config.Egress {
			util.GetLog().WithField("canal", m.config.CanalConfig.Name).
				WithField("driver", v.Driver).
				Debugf("multi canal init egress driver")

			var eDriver interface{}
			eDriver, m.err = register.RegisterGetDriver(v.Driver, driver.TypeEgress)
			if m.err != nil {
				return
			}
			egressDriver := eDriver.(driver.EgressDriver)
			if m.err = egressDriver.Init(v); m.err != nil {
				return
			}
			var hookChain = hook.HookChain{}

			util.GetLog().WithField("canal", m.config.CanalConfig.Name).
				WithField("driver", v.Driver).
				WithField("hookChain", pretty.Sprint(v.HookChain)).
				Debugf("multi canal init egress parse HookChain")

			if hookChain, m.err = canal.ParseHookChain(v.HookChain); m.err != nil {
				return
			}
			outputs = append(outputs, &output{
				egressDriver: egressDriver,
				hook:         hookChain,
				driverName:   v.Driver,
			})
		}
		util.GetLog().WithField("canal", m.config.CanalConfig.Name).
			Debugf("multi canal outpu init done")

		m.canal.output = outputs
	}
}

func (m *MultiCanalBuilder) GetCanal() (canal.Canal, error) {
	if m.err == nil {
		m.initCanalConfig()
		conf := m.config.CanalConfig
		m.canal.name = conf.Name

		m.canal.maxWaitDataTime = time.Millisecond * time.Duration(conf.MaxWaitTime)
		m.canal.maxDataBatch = conf.MaxDataBatch

		backOff := backoff.NewExponentialBackOff()
		backOff.InitialInterval = time.Millisecond * time.Duration(conf.RetryOption.InitialInterval)
		backOff.Multiplier = conf.RetryOption.Multiplier
		backOff.MaxInterval = time.Millisecond * time.Duration(conf.RetryOption.MaxInterval)
		m.canal.defaultBackoff = backOff

		m.canal.lock = &sync.RWMutex{}

		util.GetLog().WithField("config", pretty.Sprint(m.config)).
			Debugf("multi canal build finish")
		return m.canal, nil
	}
	return nil, m.err
}
