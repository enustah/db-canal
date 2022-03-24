package multi_canal

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/enustah/db-canal/canal"
	"github.com/enustah/db-canal/config"
	"github.com/enustah/db-canal/driver"
	"github.com/enustah/db-canal/hook"
	"github.com/enustah/db-canal/util"
	"github.com/kr/pretty"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type input struct {
	driverName    string
	ingressDriver driver.IngressDriver
}

func (i input) String() string {
	return i.driverName
}

type output struct {
	driverName   string
	egressDriver driver.EgressDriver
	hook         hook.HookChain
}

func (o output) String() string {
	return o.driverName
}

type MultiCanal struct {
	name string
	// create on start, cancel on stop
	ctx context.Context
	// create on main loop start, stop on main loop return, use to Stop() wait main loop exit.
	mainLoopCtx context.Context
	cancelFunc  func()
	// default fail backoff on this canal
	// TODO backoff on different output
	defaultBackoff  *backoff.ExponentialBackOff
	maxWaitDataTime time.Duration
	maxDataBatch    uint
	Started         bool
	lock            *sync.RWMutex

	input  *input
	output []*output
}

func NewMultiCanal(config *config.Config) (canal.Canal, error) {
	util.GetLog().WithField("name", config.CanalConfig.Name).Infof("building multi canal")
	builder := newMultiCanalBuilder(&MultiCanal{}, config)
	builder.BuildInput()
	builder.BuildOutput()
	return builder.GetCanal()
}

func (m *MultiCanal) log() *log.Entry {
	return util.GetLog().WithField("canal", m.name)

}

func (m *MultiCanal) Run() error {
	var err error
	m.onLockDo(func() {
		if m.Started {
			m.log().Warnf("run after start")
			return
		}

		var (
			dataChan <-chan *driver.Data
			outputI  = 0 // increase when output start.
		)

		defer func() {
			// stop all started driver when encounter error
			if err != nil {
				if outputI != 0 {
					m.input.ingressDriver.Stop()
				}
				for i := 0; i < outputI; i++ {
					m.output[i].egressDriver.Stop()
				}
			}
		}()

		m.log().Infof("starting, waiting driver start")

		dataChan, err = m.input.ingressDriver.Start()
		if err != nil {
			m.log().WithField("driver", m.input.driverName).
				WithField("error", err).
				Errorf("ingress start fail")
			return
		}

		for _, v := range m.output {
			if err = v.egressDriver.Start(); err != nil {
				m.log().WithField("driver", v.driverName).
					WithField("error", err).
					Errorf("egress start fail")
				return
			}
			outputI++
		}

		m.ctx, m.cancelFunc = context.WithCancel(context.TODO())
		go m.mainLoop(dataChan)
		m.log().Infof("multi canal started")
		m.Started = true
	})
	return err
}

func (m *MultiCanal) Stop() {
	m.onLockDo(func() {
		if !m.Started {
			m.log().Warnf("stop before start")
			return
		}
		m.cancelFunc()
		m.log().Infof("stopping")
		m.log().Infof("waiting main loop stop")
		<-m.mainLoopCtx.Done()
		m.log().Infof("waiting ingress stop")
		m.input.ingressDriver.Stop()
		m.log().Infof("waiting egress stop")
		for _, v := range m.output {
			v.egressDriver.Stop()
		}
		m.Started = false
		m.log().Infof("stopped")
	})

}

func (m *MultiCanal) onLockDo(f func()) {
	m.lock.Lock()
	defer m.lock.Unlock()
	f()
}

/*
Execute f until success or ctx is done. when f return err, exponential increase backoff time.
Backoff param  can config by multi canal config.
*/
func (m *MultiCanal) backoffDo(f func() error) error {
	var err error
	return backoff.Retry(func() error {
		select {
		case <-m.ctx.Done():
			if err == nil {
				return &backoff.PermanentError{
					Err: canal.CtxDoneErr,
				}
			} else {
				return &backoff.PermanentError{
					Err: err,
				}
			}
		default:
			err = f()
			return err
		}
	}, m.defaultBackoff)
}

func (m *MultiCanal) mainLoop(ch <-chan *driver.Data) {
	var (
		loopCancelFunc func()
	)
	m.mainLoopCtx, loopCancelFunc = context.WithCancel(context.TODO())
	defer loopCancelFunc()
	m.log().Debugf("running main loop")

	for {
		var (
			// dataBatch for each output
			dataBatch      = make([][]*driver.Data, len(m.output)) // [output][dataBatch]
			count     uint = 0
			lastData  *driver.Data
			// append data will copy the data to all output
			appendData = func(data *driver.Data) {
				for i, _ := range dataBatch {
					dataBatch[i] = append(dataBatch[i], data.DeepCopy())
				}
			}
		)
		// init dataBatch for all output
		for i, _ := range dataBatch {
			dataBatch[i] = make([]*driver.Data, 0, m.maxDataBatch)
		}

		// init timer
		var timer <-chan time.Time = nil // nil channel will always block
		if m.maxWaitDataTime <= 0 {
			m.log().WithField("maxDataBatch", m.maxDataBatch).
				Debugf("maxWaitDataTime less than 0, waiting dataBatch reach maxDataBatch")

		} else {
			timer = time.Tick(m.maxWaitDataTime)
		}
		// wait to time tick or dataBatch reach maxDataBatch
	dataLoop:
		for {
			select {
			case <-m.ctx.Done():
				return
			default:
				select {
				case <-m.ctx.Done():
					return
				case <-timer:
					if count == 0 {
						continue
					} else {
						break dataLoop
					}
				case data, ok := <-ch:
					if !ok {
						panic(fmt.Sprintf("multi canal %s data channel close unexpectly", m.name))
					}
					count++
					lastData = data
					appendData(data)
					if count >= m.maxDataBatch { // reach max data batch
						break dataLoop
					}
				}
			}
		}

		m.log().WithField("dataBatch len", len(dataBatch[0])).
			WithField("data", pretty.Sprint(dataBatch[0])).
			Debugf("canal get data batch")
		// write to all output
		// err is not nil only when ctx cancel, dataLoop will certainly return
		waitGroup := &sync.WaitGroup{}
		waitGroup.Add(len(m.output))
		var err error
		for i, v := range m.output {
			go func(output *output, data []*driver.Data) {
				defer waitGroup.Done()
				if herr := m.writeToOutput(output, data); herr != nil {
					err = herr
				}
			}(v, dataBatch[i])
		}
		waitGroup.Wait()

		// all output write data success
		if err == nil {
			m.log().WithField("latestData", pretty.Sprint(lastData)).Debugf("save point")
			m.backoffDo(func() error {
				return m.input.ingressDriver.SavePoint(lastData)
			})
		}
	}
}

func (m *MultiCanal) writeToOutput(output *output, dataBatch []*driver.Data) error {
	m.log().WithField("output", output.driverName).
		WithField("dataBatchLen", len(dataBatch)).
		Debugf("passing hook chain")

	// pass hook chain
	if err := m.backoffDo(func() error {
		var err error
		dataBatch, err = output.hook.PassThrough(dataBatch)
		if err != nil {
			m.log().WithField("output", output.driverName).
				WithField("error", err).
				Errorf("output hook run fail")
		}
		return err
	}); err != nil {
		return err
	}
	m.log().WithField("output", output.driverName).
		WithField("dataBatchLen", len(dataBatch)).
		Debugf("output hook run finish")
	if len(dataBatch) == 0 {
		return nil
	}

	// write data
	return m.backoffDo(func() error {
		var err error
		if err = output.egressDriver.WriteData(dataBatch); err != nil {
			m.log().WithField("output", output.driverName).
				WithField("error", err).
				Errorf("output write data fail")
		}
		return err
	})
}
