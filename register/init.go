package register

import (
	"fmt"
	"github.com/enustah/db-canal/driver"
	"github.com/enustah/db-canal/hook"
	"github.com/enustah/db-canal/util"
	"github.com/shopspring/decimal"
	"reflect"
	"strconv"
	"time"
)

// register builtin hook. Current implement delay(timeStr) and fieldFilter(field,operator,val)
func init() {
	util.Must(
		RegisterHook(
			func(ctx *hook.Ctx, args []interface{}) error {
				t, err := util.ParseTimeStr(args[0].(string))
				if err != nil {
					return err
				}
				<-time.After(t)
				return nil
			},
			"delay",
			[]hook.Arg{hook.ArgTypeStr},
			func(args []interface{}) error { // arg validator func
				_, err := time.ParseDuration(args[0].(string))
				return err
			},
		),
	)

	util.Must(
		RegisterHook(
			func(ctx *hook.Ctx, args []interface{}) error {
				var (
					dbName    = args[0].(string)
					tableName = args[1].(string)
					fieldName = args[2].(string)
					operator  = args[3].(string)
					val       = args[4].(string)
				)

				ctx.ForEach(func(data *driver.Data) (drop bool, stop bool) {
					_, ok := data.RawMap[fieldName]
					// skip when field not in data or dbName or tableName not match
					if !ok || (dbName != "-" && data.Database.Name != dbName) || (tableName != "-" && data.Table.Name != tableName) {
						return
					}
					drop = matchFieldVal(data, fieldName, operator, val)
					return
				})
				return nil
			},
			"dataFilter",
			// args: dbName tableName fieldName operator value
			[]hook.Arg{hook.ArgTypeStr, hook.ArgTypeStr, hook.ArgTypeStr, hook.ArgTypeStr, hook.ArgTypeStr},
			func(args []interface{}) error {
				return validateOperator(args[3].(string))
			},
		),
	)
}

const (
	eq  = "="
	neq = "!="
	gte = ">="
	gt  = ">"
	lt  = "<"
	lte = "<="
)

func compareVal[T float64 | string](i, f T, operator string) bool {
	switch operator {
	case eq:
		return i == f
	case neq:
		return i != f
	case gt:
		return i > f
	case gte:
		return i < f
	case lt:
		return i < f
	case lte:
		return i <= f
	}
	return true
}

func matchFieldVal(data *driver.Data, fieldName, operator, matchVal string) bool {
	var (
		valF, matchValF float64
	)

	val := reflect.ValueOf(data.RawMap[fieldName])
	// time.Time type convert to int64
	if val.Type() == reflect.TypeOf(time.Time{}) {
		val = reflect.ValueOf(val.Interface().(time.Time).Unix())
	}
	if val.Kind() != reflect.String {
		m, err := decimal.NewFromString(matchVal)
		if err != nil {
			panic(err)
		}
		matchValF, _ = m.Float64()
	}

	switch val.Kind() {
	case reflect.String:
		return compareVal(val.String(), matchVal, operator)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		valF, _ = decimal.NewFromInt(val.Int()).Float64()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		d, _ := decimal.NewFromString(strconv.FormatUint(val.Uint(), 10))
		valF, _ = d.Float64()
	case reflect.Float32, reflect.Float64:
		valF, _ = decimal.NewFromFloat(val.Float()).Float64()
	default:
		panic(fmt.Sprintf("%s is not num or float or str or time type", fieldName))
	}
	return compareVal(valF, matchValF, operator)
}

func validateOperator(o string) error {
	switch o {
	case eq, neq, gte, gt, lt, lte:
		return nil
	default:
		return fmt.Errorf("fieldFilter get unknown oprtator %s", o)
	}
}
