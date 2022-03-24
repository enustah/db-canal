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
				// drop indicate the data will drop
				// stop indicate stop iterate
				ctx.ForEach(func(data *driver.Data) (drop bool, stop bool) {
					// do sth with data
					return
				})

				// like gin middleware, ctx.Next() will run next hook
				// ctx.Next()

				// ctx.Abort() will stop the hook
				// ctx.Abort()

				// return error will retry whole hook chain.
				// don't return error as much as possible.
				// and must not return permanent error.
				return nil
			},
			"hook1",
			[]hook.Arg{hook.ArgTypeStr, hook.ArgTypeFloat, hook.ArgTypeInt},
			// option args validate
			// func(args []interface{}) error {
			//
			// },
		))
}
