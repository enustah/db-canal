package register

import (
	"fmt"
)

type ErrRegisterRepeated struct {
	Name string
}

func (r ErrRegisterRepeated) Error() string {
	return fmt.Sprintf("driver or hook %s register repeated", r.Name)
}

type ErrRegisterNotFound struct {
	Name string
}

func (r ErrRegisterNotFound) Error() string {
	return fmt.Sprintf("driver or hook %s not found", r.Name)
}

type ErrRegisterTypeNotMatch struct {
	Name string
}

func (r ErrRegisterTypeNotMatch) Error() string {
	return fmt.Sprintf("driver or hook %s type not match", r.Name)
}
