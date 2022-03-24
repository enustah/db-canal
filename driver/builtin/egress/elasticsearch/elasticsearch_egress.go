package elasticsearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/enustah/db-canal/config"
	"github.com/enustah/db-canal/driver"
	"github.com/enustah/db-canal/register"
	"github.com/enustah/db-canal/util"
	"github.com/mitchellh/mapstructure"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func init() {
	util.Must(register.RegisterEgressDriver("elasticsearch_egress", &ElasticsearchEgress{}))
}

type esEgressOption struct {
	// map<table_name>idColumnName, require
	IDColumn map[string]string `mapstructure:"idColumn"`
	// the follow option relate to esutil.BulkIndexerConfig
	NumWorkers    int    `mapstructure:"numWorkers"`
	FlushBytes    int    `mapstructure:"flushBytes"`
	FlushInterval string `mapstructure:"flushInterval"`
	// the follow option relate to elasticsearch.Config (github.com/elastic/go-elasticsearch/v8)
	Username       string `mapstructure:"username"`
	Password       string `mapstructure:"password"`
	APIKey         string `mapstructure:"apiKey"`
	ServiceToken   string `mapstructure:"serviceToken"`
	Proxy          string `mapstructure:"proxy"`
	TlsSni         string `mapstructure:"tlsSni"`
	CaCert         string `mapstructure:"caCert"`
	SkipCertVerify bool   `mapstructure:"skipCertVerify"`
}

type ElasticsearchEgress struct {
	ctx      context.Context
	client   *elasticsearch.Client
	idColumn map[string]string

	FlushBytes    int
	FlushInterval time.Duration
	NumWorkers    int
}

func (e *ElasticsearchEgress) Init(config config.EgressConfig) error {
	option := &esEgressOption{}
	if err := mapstructure.Decode(config.Options, option); err != nil {
		return err
	}

	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}
	if option.CaCert != "" {
		if ok := rootCAs.AppendCertsFromPEM([]byte(option.CaCert)); !ok {
			return errors.New("can not append root ca")
		}
	}

	var (
		err           error
		proxy         func(r *http.Request) (*url.URL, error)
		flushInterval time.Duration
		tlsConfig     = &tls.Config{
			ServerName:         option.TlsSni,
			RootCAs:            rootCAs,
			InsecureSkipVerify: option.SkipCertVerify,
		}
		addr = strings.Split(config.Url, ",")
	)
	if option.FlushInterval != "" {
		flushInterval, err = util.ParseTimeStr(option.FlushInterval)
		if err != nil {
			return err
		}
	}
	if option.Proxy != "" {
		proxy = func(r *http.Request) (*url.URL, error) {
			return url.Parse(option.Proxy)
		}
	}

	e.ctx = context.Background()
	e.FlushInterval = flushInterval
	e.FlushBytes = option.FlushBytes
	e.NumWorkers = option.NumWorkers
	e.idColumn = option.IDColumn
	e.client, err = elasticsearch.NewClient(elasticsearch.Config{
		Addresses:    addr,
		Username:     option.Username,
		Password:     option.Password,
		APIKey:       option.APIKey,
		ServiceToken: option.ServiceToken,
		Transport: &http.Transport{
			Proxy:           proxy,
			TLSClientConfig: tlsConfig,
		},
	})
	return err
}

func (e *ElasticsearchEgress) Start() error {
	resp, err := e.client.Info()
	if err == nil {
		if resp.StatusCode != 200 {
			err = fmt.Errorf("get info return status code %d", resp.StatusCode)
		}
	}
	return err
}

func (e *ElasticsearchEgress) WriteData(dataBatch []*driver.Data) error {
	data := splitData(dataBatch, e.idColumn)
	bulk, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        e.client,
		FlushBytes:    e.FlushBytes,
		FlushInterval: e.FlushInterval,
		NumWorkers:    e.NumWorkers,
	})
	util.Must(err)
	for _, v := range data {
		for _, item := range v {
			i := *item
			i.OnFailure = func(_ context.Context, req esutil.BulkIndexerItem, resp esutil.BulkIndexerResponseItem, err error) {
				b, _ := ioutil.ReadAll(req.Body)
				errStr := ""
				if err != nil {
					errStr = err.Error()
				} else {
					errStr = resp.Error.Reason + ", " + resp.Error.Cause.Reason
				}
				util.GetLog().
					WithField("index", req.Index).
					WithField("action", req.Action).
					WithField("_id", req.DocumentID).
					WithField("req_data", string(b)).
					WithField("error", errStr).
					WithField("result", resp.Result).
					Errorf("elasticsearch write data fail")
			}
			bulk.Add(e.ctx, i)
		}
	}
	if err := bulk.Close(e.ctx); err != nil {
		return err
	}
	failCount := bulk.Stats().NumFailed
	if failCount != 0 {
		return fmt.Errorf("es write data not all success.total: %d, fail: %d", len(dataBatch), failCount)
	}
	return nil
}

func (e *ElasticsearchEgress) Stop() {

}

// split data according table name
func splitData(data []*driver.Data, idColumn map[string]string) map[string][]*esutil.BulkIndexerItem {
	m := make(map[string][]*esutil.BulkIndexerItem)
	for _, v := range data {
		var (
			table            = v.Table.Name
			idColumnName, ok = idColumn[table]
			idColumn         *driver.Column
			_id              string
		)
		if !ok {
			panic(fmt.Sprintf("id column of index %s not config", v.Table.Name))
		}
		_, ok = m[table]
		if !ok {
			m[table] = make([]*esutil.BulkIndexerItem, 0)
		}

		// get id column
		for _, v := range v.Table.Column {
			if v.Name == idColumnName {
				idColumn = v
				break
			}
		}
		if idColumn == nil {
			panic(fmt.Sprintf("id colunm `%s` not found", idColumnName))
		}

		// deep copy the rawMap , get id column as es _id.
		rawMap := util.DeepCopyMap(v.RawMap)

		switch idColumn.Type {
		case driver.ColumnTypeNumber:
			_id = strconv.FormatInt(rawMap[idColumnName].(int64), 10)
		case driver.ColumnTypeString:
			_id = rawMap[idColumnName].(string)
		default:
			panic("id column type must string or number")
		}

		if v.Event == driver.EventDelete { // delete doc
			m[table] = append(m[table], &esutil.BulkIndexerItem{
				Index:      table,
				Action:     "delete",
				DocumentID: _id,
				Body:       bytes.NewReader([]byte{}),
			})
		} else { // insert or update doc
			delete(rawMap, idColumnName)
			b, _ := json.Marshal(rawMap)
			m[table] = append(m[table], &esutil.BulkIndexerItem{
				Index:      table,
				Action:     "index",
				DocumentID: _id,
				Body:       bytes.NewReader(b),
			})
		}
	}

	return m
}
