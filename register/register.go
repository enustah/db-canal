package register

import (
	"github.com/enustah/db-canal/driver"
	"github.com/enustah/db-canal/hook"
	"github.com/enustah/db-canal/util"
	"reflect"
	"sync"
)

var (
	driverRegisterMap = make(map[string]interface{})
	hookRegisterMap   = make(map[string]*hook.Hook)
	lock              = &sync.RWMutex{}
)

func register(m interface{}, name string, i interface{}) error {
	lock.Lock()
	defer lock.Unlock()
	mValue := reflect.ValueOf(m)
	if mValue.MapIndex(reflect.ValueOf(name)) != (reflect.Value{}) {
		return ErrRegisterRepeated{
			Name: name,
		}
	}
	mValue.SetMapIndex(reflect.ValueOf(name), reflect.ValueOf(i))
	return nil
}
func registerGet(m interface{}, name string) (interface{}, error) {
	lock.RLock()
	defer lock.RUnlock()
	mValue := reflect.ValueOf(m)
	val := mValue.MapIndex(reflect.ValueOf(name))
	if val == (reflect.Value{}) {
		return nil, ErrRegisterNotFound{
			Name: name,
		}
	}
	return val.Interface(), nil
}

// TODO maybe go generic will support interface{} in the future
func RegisterIngressDriver(name string, driver driver.IngressDriver) error {
	util.GetLog().WithField("name", name).Infof("register ingress driver")
	return register(driverRegisterMap, name, driver)
}
func RegisterEgressDriver(name string, driver driver.EgressDriver) error {
	util.GetLog().WithField("name", name).Infof("register egress driver")
	return register(driverRegisterMap, name, driver)
}

func RegisterHook(fn hook.HookFunc, name string, expectArgsType []hook.Arg, argValidateFunc ...func(args []interface{}) error) error {
	util.GetLog().WithField("name", name).Infof("register hook")
	return register(hookRegisterMap, name, hook.NewHook(fn, name, expectArgsType, argValidateFunc...))
}

func RegisterGetDriver(name string, typ driver.Type) (interface{}, error) {
	d, err := registerGet(driverRegisterMap, name)
	if err != nil {
		return nil, err
	}
	if _, ok := d.(driver.IngressDriver); ok && typ == driver.TypeIngress {
		return d, nil
	} else if _, ok := d.(driver.EgressDriver); ok && typ == driver.TypeEgress {
		return d, nil
	}
	return nil, ErrRegisterTypeNotMatch{
		Name: name,
	}
}

func RegisterGetHook(name string) (*hook.Hook, error) {
	h, err := registerGet(hookRegisterMap, name)
	if err != nil {
		return nil, err
	}
	return h.(*hook.Hook), nil
}
