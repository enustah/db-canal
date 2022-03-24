package driver

import (
	"github.com/enustah/db-canal/config"
)

type Type string

const (
	TypeIngress Type = "ingress"
	TypeEgress  Type = "egress"
)

type EgressDriver interface {
	Init(config config.EgressConfig) error
	Start() error
	WriteData(dataBatch []*Data) error
	// TODO Backoff Do
	// BackoffDo(func())
	Stop()
}

type IngressDriver interface {
	Init(config config.IngressConfig) error
	// data chan should not close until Stop()
	Start() (<-chan *Data, error)
	// SavePoint save the point or offset of data position. such as mysql binlog position. the data is the latest
	// sync data in canal
	SavePoint(data *Data) error
	Stop()
}
