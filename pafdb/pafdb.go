package pafdb

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/denisenkom/go-mssqldb"

	"github.com/intelsdi-x/snap-plugin-lib-go/v1/plugin"
	log "github.com/sirupsen/logrus"
)

const (
	Name       = "pafdb"
	Version    = 1
	PluginType = "publisher"
)

var (
	conn *sql.DB
	// Mutex for synchronizing connection changes
	m           = &sync.Mutex{}
	initialized = false
)

func init() {
}

// New returns an instance of the InfluxDB publisher
func New() *PafDbPublisher {
	return &PafDbPublisher{}
}

// PafDbPublisher the PAF snap publisher plugin
type PafDbPublisher struct {
}

type configuration struct {
	host, database, user, password, logLevel string
	port, testRun                            int64
}

func getConfig(config plugin.Config) (configuration, error) {
	cfg := configuration{}
	var err error

	cfg.host, err = config.GetString("host")
	if err != nil {
		return cfg, fmt.Errorf("%s: %s", err, "host")
	}

	cfg.database, err = config.GetString("database")
	if err != nil {
		return cfg, fmt.Errorf("%s: %s", err, "database")
	}

	cfg.user, err = config.GetString("user")
	if err != nil {
		return cfg, fmt.Errorf("%s: %s", err, "user")
	}

	cfg.password, err = config.GetString("password")
	if err != nil {
		return cfg, fmt.Errorf("%s: %s", err, "password")
	}

	cfg.logLevel, err = config.GetString("log-level")
	if err != nil {
		cfg.logLevel = "undefined"
	}

	cfg.port, err = config.GetInt("port")
	if err != nil {
		return cfg, fmt.Errorf("%s: %s", err, "port")
	}

	cfg.testRun, err = config.GetInt("test-run")
	if err != nil {
		return cfg, fmt.Errorf("%s: %s", err, "test-run")
	}

	return cfg, nil
}

// GetConfigPolicy -
func (pp *PafDbPublisher) GetConfigPolicy() (plugin.ConfigPolicy, error) {
	policy := plugin.NewConfigPolicy()

	policy.AddNewStringRule([]string{""}, "host", true)
	policy.AddNewIntRule([]string{""}, "port", false, plugin.SetDefaultInt(8086))
	policy.AddNewStringRule([]string{""}, "database", true)
	policy.AddNewStringRule([]string{""}, "user", true)
	policy.AddNewStringRule([]string{""}, "password", true)
	policy.AddNewIntRule([]string{""}, "test-run", true)

	return *policy, nil
}

// Publish publishes metric data to PAF database
func (pp *PafDbPublisher) Publish(metrics []plugin.Metric, pluginConfig plugin.Config) error {
	config, err := getConfig(pluginConfig)
	if err != nil {
		return err
	}

	logger := getLogger(config)

	if !initialized {
		m.Lock()
		connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s", config.host, config.user, config.password, config.port, config.database)
		conn, err = sql.Open("mssql", connString)
		if err != nil {
			log.Fatal("Open connection failed:", err.Error())
		} else {
			initialized = true
		}
		m.Unlock()
	}
	//defer conn.Close()

	txn, err := conn.Begin()
	if err != nil {
		log.Fatal(err)
	}

	textStmt, err := txn.Prepare(mssql.CopyIn("dpa_sql", mssql.BulkOptions{}, "hash", "sql"))
	if err != nil {
		log.Fatal(err.Error())
	}

	waitStmt, err := txn.Prepare(mssql.CopyIn("dpa_wait", mssql.BulkOptions{}, "test_run_id", "hash", "type", "event_date", "value"))
	if err != nil {
		log.Fatal(err.Error())
	}

	for _, m := range metrics {
		logger.Infof("metric namespace %s", m.Namespace.String())
		if strings.HasSuffix(m.Namespace.String(), "/sql") {
			hash := m.Tags["hash"]
			sql := m.Tags["text"]
			_, err = textStmt.Exec(hash, sql)
			if err != nil {
				log.Info(err.Error())
			}
		} else if strings.HasSuffix(m.Namespace.String(), "/wait") {
			hash := m.Tags["hash"]
			if hash == "" {
				hash = m.Tags["sql"]
			}
			wait := m.Tags["type"]
			value := m.Data.(int32)
			_, err = waitStmt.Exec(config.testRun, hash, wait, time.Now(), value)
			if err != nil {
				log.Info(err.Error())
			}
		}
	}

	_, err = textStmt.Exec()
	if err != nil {
		log.Info(err)
	}

	err = textStmt.Close()
	if err != nil {
		log.Info(err)
	}

	_, err = waitStmt.Exec()
	if err != nil {
		log.Info(err)
	}

	err = waitStmt.Close()
	if err != nil {
		log.Info(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Info(err)
	}

	return nil
}

func getLogger(config configuration) *log.Entry {
	logger := log.WithFields(log.Fields{
		"plugin-name":    Name,
		"plugin-version": Version,
		"plugin-type":    PluginType,
	})

	// default
	log.SetLevel(log.WarnLevel)

	levelValue := config.logLevel
	if levelValue != "undefined" {
		if level, err := log.ParseLevel(strings.ToLower(levelValue)); err == nil {
			log.SetLevel(level)
		} else {
			log.WithFields(log.Fields{
				"value":             strings.ToLower(levelValue),
				"acceptable values": "warn, error, debug, info",
			}).Warn("Invalid log-level config value")
		}
	}
	return logger
}
