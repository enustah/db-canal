package hook

import (
	"fmt"
	"github.com/bits-and-blooms/bitset"
	"github.com/enustah/db-canal/driver"
	"github.com/enustah/db-canal/util"
	"github.com/kr/pretty"
	"strconv"
	"strings"
)

type Arg int

// Arg use to validate hook arg type, will cast to one of int64 string float64.
const (
	ArgTypeInt Arg = iota
	ArgTypeStr
	ArgTypeFloat
)

type ctxDataBatchWarp struct {
	dataBatch []*driver.Data
	dropMap   bitset.BitSet
}

type Ctx struct {
	err           error
	idx           int
	abort         bool
	hook          *HookChain
	dataBatchWarp *ctxDataBatchWarp
}

func (c *Ctx) ForEach(f func(data *driver.Data) (drop bool, stop bool)) {
	for i, v := range c.dataBatchWarp.dataBatch {
		if !c.dataBatchWarp.dropMap.Test(uint(i)) {
			drop, stop := f(v)
			if drop {
				util.GetLog().WithField("data", pretty.Sprint(v)).
					Debugf("data drop. ")
				c.dataBatchWarp.dropMap.SetTo(uint(i), true)
			}
			if stop {
				util.GetLog().WithField("on data", pretty.Sprintf("%v", v)).
					Debugf("data iterate stop")
				break
			}
		}
	}
}

func (c *Ctx) Next() {
	log := util.GetLog().WithField("current index", c.idx)
	if c.idx < len(c.hook.hooks) {
		log = log.WithField("ctx hook[idx]", c.hook.hooks[c.idx].Name)
	} else {
		log.Warnf("run Next() more than one time in hook chain")
	}
	log.Debugf("hook invoke Next(). ")

	c.idx++
	c.runHook()
}

func (c *Ctx) Abort() {
	log := util.GetLog().WithField("current index", c.idx)
	if c.idx < len(c.hook.hooks) {
		log = log.WithField("ctx hook[idx]", c.hook.hooks[c.idx].Name)
	} else {
		log.Warnf("Abort after Next() in hook chain")
	}
	log.Debugf("hook invoke Abort(). ")
	c.abort = true
}

func (c *Ctx) runHook() {
	for c.idx < len(c.hook.hooks) {
		if c.err != nil || c.abort {
			return
		}
		hook := c.hook.hooks[c.idx]
		util.GetLog().WithField("name", hook.Name).
			WithField("args", hook.Args).
			Debugf("run hook")
		if err := hook.HookFunc(c, hook.Args); err != nil {
			util.GetLog().WithField("hook", hook.Name).
				WithField("args", hook.Args).
				WithField("error", err).
				Errorf("hook encounter error")
			c.err = err
		}
		c.idx++
	}
}

// HookFunc args type is one of int64 float64 string
type HookFunc func(ctx *Ctx, args []interface{}) error

type Hook struct {
	fn              HookFunc
	expectArgsType  []Arg
	name            string
	argValidateFunc func(args []interface{}) error
}

func NewHook(fn HookFunc, name string, expectArgsType []Arg, f ...func(args []interface{}) error) *Hook {
	var argValidateFunc func(args []interface{}) error
	if len(f) != 0 {
		argValidateFunc = f[0]
	}
	return &Hook{
		fn:              fn,
		expectArgsType:  expectArgsType,
		name:            name,
		argValidateFunc: argValidateFunc,
	}
}

/*
HookChain inspire by gin middleware. HookChain principal purpose is filter or modify data.
Ctx create new instance everytime when new data batch through hook chain. When error is nil,
the data will pass to egress driver and invoke WriteData(). Return an error with not nil will
abort the hook chain, data process will preserve and retry whole hook chain until no error.
*/
type HookChain struct {
	hooks []struct {
		Name     string
		HookFunc HookFunc
		Args     []interface{}
	}
}

func (h *HookChain) PassThrough(data []*driver.Data) ([]*driver.Data, error) {
	ctx := &Ctx{
		hook: h,
		dataBatchWarp: &ctxDataBatchWarp{
			dataBatch: data,
		},
	}
	ctx.runHook()
	passData := make([]*driver.Data, 0, len(data))
	// preserve process data
	for i, v := range ctx.dataBatchWarp.dataBatch {
		if !ctx.dataBatchWarp.dropMap.Test(uint(i)) {
			passData = append(passData, v)
		}
	}
	return passData, ctx.err
}

func (h *HookChain) AppendHook(hook *Hook, args []string) error {
	if len(hook.expectArgsType) != len(args) {
		return HookArgErr{
			msg: fmt.Sprintf("hook %s expect %d args, got %d", hook.name, len(hook.expectArgsType), len(args)),
		}
	}
	var (
		err   error
		argsT = make([]interface{}, 0, len(args))
	)

	for i, v := range args {
		v = strings.TrimSpace(v)
		var c interface{}
		switch hook.expectArgsType[i] {
		case ArgTypeFloat:
			c, err = strconv.ParseFloat(v, 64)
			fallthrough
		case ArgTypeInt:
			c, err = strconv.ParseInt(v, 10, 64)
			fallthrough
		case ArgTypeStr:
			if err != nil {
				return err
			}
			c = v
			argsT = append(argsT, c)
		default:
			return HookArgErr{
				msg: fmt.Sprintf("unknown arg type on hook %s, args index %d", hook.name, i),
			}
		}
	}
	if hook.argValidateFunc != nil {
		if err := hook.argValidateFunc(argsT); err != nil {
			return err
		}
	}
	h.hooks = append(h.hooks, struct {
		Name     string
		HookFunc HookFunc
		Args     []interface{}
	}{
		Name:     hook.name,
		HookFunc: hook.fn,
		Args:     argsT,
	})
	return nil
}
