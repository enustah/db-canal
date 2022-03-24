package test

import (
	"fmt"
	"github.com/enustah/db-canal/canal"
	"github.com/enustah/db-canal/driver"
	"github.com/enustah/db-canal/hook"
	"github.com/enustah/db-canal/register"
	"github.com/enustah/db-canal/util"
	"github.com/kr/pretty"
	"testing"
)

func TestHook(t *testing.T) {
	TestRegisterHook(t)
	hooks := []string{
		"dataFilter(test,-,aa,=,11)",
		"testFail()",
		"f1(zz)",
		"f2()",
		// "delay(5s)",
		"f3()",
		"f4()",
	}
	i := 0
	util.Must(register.RegisterHook(
		func(ctx *hook.Ctx, args []interface{}) error {
			ctx.Next()
			ctx.ForEach(func(data *driver.Data) (drop bool, stop bool) {
				pretty.Println(data)
				if _, ok := data.RawMap["bb"]; ok {
					return true, false
				}
				return false, false
			})
			ctx.Abort()
			if i == 0 {
				i += 1
				// return errors.New("")
			}
			return nil
		}, "testFail", nil))

	hc, err := canal.ParseHookChain(hooks)
	util.Must(err)
	data := []*driver.Data{
		{
			RawMap: map[string]interface{}{
				"aa": 11,
			},
			Table: &driver.Table{
				Name: "test",
			},
			Database: &driver.Database{
				Name: "test",
			},
		},
		{
			RawMap: map[string]interface{}{
				// "aa": 11,
				"bb": 22,
			},
			Table: &driver.Table{
				Name: "test1",
			},
			Database: &driver.Database{
				Name: "test",
			},
		},
		{
			RawMap: map[string]interface{}{
				"cc": 33,
			},
			Table: &driver.Table{
				Name: "test3",
			},
			Database: &driver.Database{
				Name: "test3",
			},
		},
	}
	_, err = hc.PassThrough(data)
	fmt.Println(err)
}
