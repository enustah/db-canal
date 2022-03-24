package config

import (
	"fmt"
	"github.com/enustah/db-canal/util"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"time"
)

type IngressConfig struct {
	Driver  string                 `yaml:"driver"`
	Dsn     string                 `yaml:"dsn"`
	Options map[string]interface{} `yaml:"options"`
}

type EgressConfig struct {
	Driver    string                 `yaml:"driver"`
	Url       string                 `yaml:"url"`
	Options   map[string]interface{} `yaml:"options"`
	HookChain []string               `yaml:"hookChain"`
}

type CanalConfig struct {
	Name string `yaml:"name"`
	// max time to wait data, in millisecond. inspire by elasticsearch refresh
	// if less than or equal zero, will wait to dataBatch length reach MaxDataBatch
	MaxWaitTime int `yaml:"maxWaitTime"`
	// max data batch to write once.
	MaxDataBatch uint `yaml:"maxDataBatch"`
	RetryOption  struct {
		// in millisecond
		InitialInterval int `yaml:"initialInterval"`
		// in millisecond
		MaxInterval int     `yaml:"maxInterval"`
		Multiplier  float64 `yaml:"multiplier"`
	} `yaml:"retryOption"`
}

type Config struct {
	Ingress     IngressConfig  `yaml:"ingress"`
	CanalConfig CanalConfig    `yaml:"canalConfig"`
	Egress      []EgressConfig `yaml:"egress"`
}

type FullConfig struct {
	Config       []*Config `yaml:"config"`
	LogLevel     string    `yaml:"logLevel"`
	TimeLocation string    `yaml:"timeLocation"`
}

func GetDefaultCanalConfig() CanalConfig {
	return CanalConfig{
		MaxWaitTime:  0,
		MaxDataBatch: 1,
		RetryOption: struct {
			InitialInterval int     `yaml:"initialInterval"`
			MaxInterval     int     `yaml:"maxInterval"`
			Multiplier      float64 `yaml:"multiplier"`
		}{
			InitialInterval: 1500,
			MaxInterval:     10000,
			Multiplier:      2.0,
		},
	}
}

func FromYaml(c string) ([]*Config, error) {
	var (
		err  error
		conf = &FullConfig{}
	)
	if err = yaml.Unmarshal([]byte(c), &conf); err != nil {
		return nil, err
	}
	if err = setTimeLocation(conf.TimeLocation); err == nil {
		err = setLogLevel(conf.LogLevel)
	}
	return conf.Config, err
}

func setTimeLocation(t string) error {
	if t == "" {
		return nil
	}
	var err error
	time.Local, err = time.LoadLocation(t)
	return err
}

func setLogLevel(l string) error {
	var logLevel log.Level
	switch l {
	case "", "info":
		logLevel = log.InfoLevel
	case "warn":
		logLevel = log.WarnLevel
	case "debug":
		logLevel = log.DebugLevel
	default:
		return fmt.Errorf("unknown log level %s", l)
	}
	util.SetLogLevel(logLevel)
	return nil
}
