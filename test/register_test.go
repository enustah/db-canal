package test

import (
	"errors"
	"fmt"
	"github.com/enustah/db-canal/hook"
	"github.com/enustah/db-canal/register"
	"github.com/enustah/db-canal/util"
	"testing"
)

func TestRegisterHook(t *testing.T) {
	util.Must(register.RegisterHook(func(ctx *hook.Ctx, args []interface{}) error {
		fmt.Println(args)
		fmt.Println("b 1")
		ctx.Next()
		fmt.Println("a 1")
		return errors.New("f1 a err")
	}, "f1", []hook.Arg{hook.ArgTypeStr}))

	fmt.Println(register.RegisterHook(func(ctx *hook.Ctx, args []interface{}) error {
		return nil
	}, "f1", []hook.Arg{}))

	util.Must(register.RegisterHook(func(ctx *hook.Ctx, args []interface{}) error {
		fmt.Println("b 2")
		fmt.Println("bb 2")
		return nil
	}, "f2", []hook.Arg{}))

	util.Must(register.RegisterHook(func(ctx *hook.Ctx, args []interface{}) error {
		fmt.Println("b 3")
		// ctx.Abort()
		fmt.Println("a 3")
		return errors.New("f3 err")
	}, "f3", nil))

	util.Must(register.RegisterHook(func(ctx *hook.Ctx, args []interface{}) error {
		fmt.Println("b 4")
		fmt.Println("a 4")
		return nil
	}, "f4", []hook.Arg{}))
}
