// Package config loads the collector's configuration from environment variables
// with sane defaults for a private-cluster deployment.
package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"
)

// ClickHouse holds the ClickHouse connection parameters the sink writes to.
type ClickHouse struct {
	Host     string
	Port     int
	User     string
	Password string
	DB       string
	Table    string
	// Cluster, when set, makes the schema be created ON CLUSTER <name> with the
	// replicated engine. Empty means a single, non-replicated server.
	Cluster string
}

// SinkType selects the downstream writer.
const (
	SinkClickHouse = "clickhouse" // production: writes to ClickHouse
	SinkStdout     = "stdout"     // demo/lab: prints each event, commits anyway
)

// Config is the full collector configuration.
type Config struct {
	MQAddr        string
	MQTokenFile   string
	MQToken       string
	ConsumerID    string
	RegistryTTL   time.Duration
	RegistryPoll  time.Duration
	ConsumePoll   time.Duration
	BatchSize     int
	FlushInterval time.Duration
	InsertRetries int
	InsertBackoff time.Duration
	ShutdownGrace time.Duration
	DedupPreCheck bool
	DeadLetterDir string
	HealthAddr    string
	// ReadyMaxStall is how long the collector may go without reading, writing
	// or committing anything before it reports itself NotReady. 0 disables the
	// check. It is the condition that catches a stall with no error attached:
	// a message queue that stops acking leaves the collector blocked and silent.
	ReadyMaxStall time.Duration
	Sink          string // SinkClickHouse | SinkStdout
	ClickHouse    ClickHouse
}

var idRE = regexp.MustCompile(`^[a-zA-Z0-9_.:+-]{1,256}$`)

// Validate rejects unsafe values (identifier injection, useless batch sizes).
func (c *Config) Validate() error {
	if c.ConsumerID == "" || !idRE.MatchString(c.ConsumerID) {
		return fmt.Errorf("invalid CONSUMER_ID %q", c.ConsumerID)
	}
	if c.BatchSize < 1 {
		return fmt.Errorf("BATCH_SIZE must be >= 1")
	}
	if c.RegistryTTL <= 0 || c.RegistryPoll <= 0 {
		return fmt.Errorf("REGISTRY_TTL_S and REGISTRY_POLL_INTERVAL_MS must be > 0")
	}
	if !idRE.MatchString(c.ClickHouse.DB) || !idRE.MatchString(c.ClickHouse.Table) {
		return fmt.Errorf("CLICKHOUSE_DB/CLICKHOUSE_TABLE must be identifier-safe")
	}
	if c.ClickHouse.Cluster != "" && !idRE.MatchString(c.ClickHouse.Cluster) {
		return fmt.Errorf("CLICKHOUSE_CLUSTER must be identifier-safe")
	}
	switch c.Sink {
	case SinkClickHouse, SinkStdout:
	default:
		return fmt.Errorf("invalid SINK %q: must be %q or %q", c.Sink, SinkClickHouse, SinkStdout)
	}
	return nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func envMS(key string, def time.Duration) time.Duration {
	return time.Duration(envInt(key, int(def/time.Millisecond))) * time.Millisecond
}

func envS(key string, def time.Duration) time.Duration {
	return time.Duration(envInt(key, int(def/time.Second))) * time.Second
}

// Load reads every setting from the environment.
func Load() *Config {
	dflt := "default"
	if u := os.Getenv("CLICKHOUSE_USER"); u != "" {
		dflt = u
	}
	return &Config{
		MQAddr:        env("MQ_ADDR", "localhost:50051"),
		MQTokenFile:   env("MQ_TOKEN_FILE", ""),
		MQToken:       env("MQ_TOKEN", ""),
		ConsumerID:    env("CONSUMER_ID", "collector-0"),
		RegistryTTL:   envS("REGISTRY_TTL_S", 15*time.Second),
		RegistryPoll:  envMS("REGISTRY_POLL_INTERVAL_MS", 5*time.Second),
		ConsumePoll:   envMS("CONSUME_POLL_INTERVAL_MS", 500*time.Millisecond),
		BatchSize:     envInt("BATCH_SIZE", 5000),
		FlushInterval: envMS("FLUSH_INTERVAL_MS", 500*time.Millisecond),
		InsertRetries: envInt("INSERT_RETRIES", 3),
		InsertBackoff: envMS("INSERT_BACKOFF_MS", 500*time.Millisecond),
		ShutdownGrace: envMS("SHUTDOWN_GRACE_MS", 5*time.Second),
		DedupPreCheck: envBool("DEDUP_PRE_CHECK", false),
		DeadLetterDir: env("DEAD_LETTER_DIR", os.TempDir()),
		HealthAddr:    env("HEALTH_ADDR", ":8082"),
		ReadyMaxStall: time.Duration(envInt("READY_MAX_STALL_SECONDS", 300)) * time.Second,
		Sink:          env("SINK", SinkClickHouse),
		ClickHouse: ClickHouse{
			Host:     env("CLICKHOUSE_HOST", "localhost"),
			Port:     envInt("CLICKHOUSE_PORT", 9000),
			User:     dflt,
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
			DB:       env("CLICKHOUSE_DB", "telemetry"),
			Table:    env("CLICKHOUSE_TABLE", "events"),
			Cluster:  env("CLICKHOUSE_CLUSTER", ""),
		},
	}
}
