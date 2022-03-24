package canal

import (
	"github.com/enustah/db-canal/hook"
	"github.com/enustah/db-canal/register"
	"github.com/enustah/db-canal/util"
	"github.com/kr/pretty"
	"strings"
)

type Canal interface {
	Run() error
	Stop()
}

type CanalBuilder interface {
	BuildInput()
	BuildOutput()
	GetCanal() (Canal, error)
}

func ParseHookChain(s []string) (hook.HookChain, error) {
	hookChain := hook.HookChain{}
	for _, v := range s {
		h, args, err := parseHook(v)
		if err != nil {
			util.GetLog().WithField("hook", v).
				WithField("error", err).
				Errorf("hook parse fail")
			return hook.HookChain{}, err
		}
		if err = hookChain.AppendHook(h, args); err != nil {
			util.GetLog().WithField("hook", v).
				WithField("error", err).
				Errorf("hook parse fail")
			return hook.HookChain{}, err
		}
		util.GetLog().WithField("hook", v).
			WithField("args", pretty.Sprint(args)).
			Debugf("hook parse done, args")
	}
	return hookChain, nil
}

func parseHook(s string) (h *hook.Hook, args []string, err error) {
	s = strings.TrimSpace(s)
	hs := hookRe.FindStringSubmatch(s)
	if len(hs) != 3 {
		return nil, nil, hook.NewHookStrErr(s)
	}
	hookName := hs[1]
	argsStr := hs[2]
	h, err = register.RegisterGetHook(hookName)
	if err != nil {
		return nil, nil, err
	}
	if argsStr != "" {
		args = strings.Split(argsStr, ",")
	}
	return h, args, nil
}
