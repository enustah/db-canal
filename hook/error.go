package hook

import "fmt"

type HookArgErr struct {
	msg string
}

func (h HookArgErr) Error() string {
	return h.msg
}

type HookStrErr struct {
	hookStr string
}

func NewHookStrErr(hookStr string) HookStrErr {
	return HookStrErr{
		hookStr: hookStr,
	}
}

func (h HookStrErr) Error() string {
	return fmt.Sprintf("hook %s parse fail", h.hookStr)
}
