package main

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

// Run executes a single Query on a single connection
func (q *Query) Run(conn *connection) error {
	if q.log == nil {
		q.log = log.NewNopLogger()
	}
	if q.desc == nil {
		return fmt.Errorf("metrics descriptor is nil")
	}
	if q.Query == "" {
		return fmt.Errorf("query is empty")
	}
	if conn == nil || conn.conn == nil {
		return fmt.Errorf("db connection not initialized (should not happen)")
	}
	// execute query
	rows, err := conn.conn.Queryx(q.Query)
	if err != nil {
		return err
	}
	defer rows.Close()

	updated := 0
	metrics := make([]prometheus.Metric, 0, len(q.metrics))
	for rows.Next() {
		res := make(map[string]interface{})
		err := rows.MapScan(res)
		if err != nil {
			level.Error(q.log).Log("msg", "Failed to scan", "err", err, "host", conn.host, "db", conn.database)
			continue
		}
		m, err := q.updateMetrics(conn, res)
		if err != nil {
			level.Error(q.log).Log("msg", "Failed to update metrics", "err", err, "host", conn.host, "db", conn.database)
			continue
		}
		metrics = append(metrics, m...)
		updated++
	}

	if updated < 1 {
		return fmt.Errorf("zero rows returned")
	}

	// update the metrics cache
	q.Lock()
	q.metrics[conn] = metrics
	q.Unlock()

	return nil
}

func (q *Query) SetDesc(conn *connection, jobName string) error {
	if q.log == nil {
		q.log = log.NewNopLogger()
	}
	if q.Query == "" {
		return fmt.Errorf("query is empty")
	}
	if conn == nil || conn.conn == nil {
		return fmt.Errorf("db connection not initialized (should not happen)")
	}
	// execute query
	rows, err := conn.conn.Queryx(q.Query)
	if err != nil {
		return err
	}
	defer rows.Close()
	updated := 0
	for rows.Next() {
		res := make(map[string]interface{})
		err := rows.MapScan(res)
		if err != nil {
			level.Error(q.log).Log("msg", "Failed to scan", "err", err, "host", conn.host, "db", conn.database)
			continue
		}
		keys := reflect.ValueOf(res).MapKeys()
		valueNames := make([]string, len(keys))
		for i := 0; i < len(keys); i++ {
			valueNames[i] = keys[i].String()
		}

		labels := append(q.Labels, "driver", "host", "database", "user", "col")
		q.desc = prometheus.NewDesc(
			MetricNameRE.ReplaceAllString("sql_" + q.Name, ""),
			q.Help,
			append(labels, valueNames...),
			prometheus.Labels{
				"sql_job": jobName,
			},
		)
		updated++
	}
	if updated < 1 {
		return fmt.Errorf("zero rows returned")
	}
	return nil
}


// updateMetrics parses the result set and returns a slice of const metrics
func (q *Query) updateMetrics(conn *connection, res map[string]interface{}) ([]prometheus.Metric, error) {
	updated := 0
	metrics := make([]prometheus.Metric, 0, len(q.Values))

	keys := reflect.ValueOf(res).MapKeys()
	valueNames := make([]string, len(keys))

	for i := 0; i < len(keys); i++ {
		valueNames[i] = keys[i].String()
	}

	for _, valueName := range valueNames {
		if !strings.HasPrefix(valueName, "metric_") {
			continue
		}
		m, err := q.updateMetric(conn, res, valueName, valueNames)
		if err != nil {
			level.Error(q.log).Log(
				"msg", "Failed to update metric",
				"value", valueName,
				"err", err,
				"host", conn.host,
				"db", conn.database,
			)
			continue
		}
		metrics = append(metrics, m)
		updated++
	}
	if updated < 1 {
		return nil, fmt.Errorf("zero values found")
	}
	return metrics, nil
}

// updateMetrics parses a single row and returns a const metric
func (q *Query) updateMetric(conn *connection, res map[string]interface{}, valueName string, valueNames []string) (prometheus.Metric, error) {
	var value float64
	if i, ok := res[valueName]; ok {
		switch f := i.(type) {
		case int:
			value = float64(f)
		case int32:
			value = float64(f)
		case int64:
			value = float64(f)
		case uint:
			value = float64(f)
		case uint32:
			value = float64(f)
		case uint64:
			value = float64(f)
		case float32:
			value = float64(f)
		case float64:
			value = float64(f)
		case []uint8:
			val, err := strconv.ParseFloat(string(f), 64)
			if err != nil {
				return nil, fmt.Errorf("Column '%s' must be type float, is '%T' (val: %s)", valueName, i, f)
			}
			value = val
		case string:
			val, err := strconv.ParseFloat(f, 64)
			if err != nil {
				return nil, fmt.Errorf("Column '%s' must be type float, is '%T' (val: %s)", valueName, i, f)
			}
			value = val
		default:
			return nil, fmt.Errorf("Column '%s' must be type float, is '%T' (val: %s)", valueName, i, f)
		}
	}
	// make space for all defined variable label columns and the "static" labels
	// added below
	labels := make([]string, 0, len(q.Labels)+5)
	//for _, label := range valueNames {
	for _, label := range labels {
		// we need to fill every spot in the slice or the key->value mapping
		// won't match up in the end.
		//
		// ORDER MATTERS!
		lv := ""
		if i, ok := res[label]; ok {
			switch str := i.(type) {
			case string:
				lv = str
			case []uint8:
				lv = string(str)
			default:
				return nil, fmt.Errorf("Column '%s' must be type text (string)", label)
			}
		}
		labels = append(labels, lv)
	}

	labels = append(labels, conn.driver)
	labels = append(labels, conn.host)
	labels = append(labels, conn.database)
	labels = append(labels, conn.user)
	labels = append(labels, valueName)

	for _, name := range  valueNames {
		lv := ""
		if i, ok := res[name]; ok {
			switch str := i.(type) {
			case string:
				lv = str
			case []uint8:
				lv = string(str)
			default:
				return nil, fmt.Errorf("Column '%s' must be type text (string)", name)
			}
		}
		labels = append(labels, lv)
	}

	// create a new immutable const metric that can be cached and returned on
	// every scrape. Remember that the order of the lable values in the labels
	// slice must match the order of the label names in the descriptor!
	return prometheus.NewConstMetric(q.desc, prometheus.GaugeValue, value, labels...)
}
