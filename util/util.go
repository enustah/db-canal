package util

import (
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func Must(err error) {
	if err != nil {
		panic(err)
	}
}

// ParseTimeStr example param: 1s 2m ...
func ParseTimeStr(s string) (time.Duration, error) {
	if len(s) < 2 {
		return 0, fmt.Errorf("time string parse fail: %s not a validate time string", s)
	}
	d := string(s[len(s)-1])
	i := s[:len(s)-1]
	t, err := strconv.ParseInt(i, 10, 0)
	if err != nil {
		return 0, fmt.Errorf("time string parse fail: %s not an integer", i)
	}
	switch strings.ToLower(d) {
	case "s":
		return time.Duration(t) * time.Second, nil
	case "m":
		return time.Duration(t) * time.Minute, nil
	default:
		return 0, fmt.Errorf("time string parse fail: %s not a validate time unit", d)
	}
}

func IgnoreEOFErr(err error) error {
	if err == io.EOF {
		return nil
	}
	return err
}

func DeepCopyMap[K comparable, V any, T map[K]V](m T) T {
	newMap := make(map[K]V)
	for k, v := range m {
		newMap[k] = v
	}
	return newMap
}

func ConvertIntegerTo64(val interface{}) interface{} {
	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint()
	default:
		panic(fmt.Sprintf("can not convert %v to int64 or uint64", val))
	}
}
