package config

import (
	"os"
	"strconv"
	"time"
)

const (
	// DefaultCSVPath is the default location where the metrics CSV is mounted
	// from the Kubernetes configMap.
	DefaultCSVPath = "/mnt/data/metrics.csv"
	// DefaultMQAddr is the default gRPC endpoint of the message queue service.
	DefaultMQAddr = "localhost:50051"
	// DefaultCluster tags events with a cluster identifier when none is set.
	DefaultCluster = "ai-prod-01"
)

// Config holds all runtime configuration for the streamer. Values originate
// from environment variables injected by the Kubernetes pod spec (see
// k8s/deployment.yaml) and can be overridden locally for development.
type Config struct {
	// PodName identifies this replica (random Deployment pod name). It is
	// also the partition-registry consumer id, from which the replica's
	// (index, total) partition assignment is derived at runtime.
	PodName string
	// Cluster is the tag inserted into every event's tags map.
	Cluster string
	// HealthAddr is where /healthz and /readyz are served for the kubelet
	// probes. Empty disables the listener.
	HealthAddr string
	// ReadyMaxStall is how long the streamer may go without a single
	// acknowledged publish before it reports itself NotReady. 0 disables the
	// check. It catches the failure where the message queue accepts publishes
	// but never acknowledges them: with sync replication a stalled follower
	// leaves the leader withholding acks, the streamer blocks in Send, and
	// nothing errors - the pipeline just stops.
	ReadyMaxStall time.Duration

	// CSVPath is the configMap-mounted CSV file path.
	CSVPath string
	// MQAddr is the gRPC address of the message queue (host:port).
	MQAddr string
	// MQTokenFile points at a file holding the bearer token used to authenticate
	// to the MQ. Preferred over MQToken because the file is re-read per RPC, so a
	// rotated Secret applies without restarting the pod.
	MQTokenFile string
	// MQToken is an inline token, used only when MQTokenFile is unset. A token in
	// the environment is readable by anything that can exec into the pod, so a
	// mounted Secret is the better home for it.
	MQToken string
	// ConsumerID is the identity used for GetOffset(consumer_id). Defaults
	// to the pod name so recovery is per-pod.
	ConsumerID string

	// ReloadCheckInterval controls how often the CSV file is stat'ed to pick
	// up configMap rotations (seconds).
	ReloadCheckInterval int
	// BatchSize is the number of events buffered per publish stream flush.
	BatchSize int
	// FlushIntervalMs is the max time a buffered batch waits before send.
	FlushIntervalMs int
	// RowDelayMs paces the read loop (0 = as fast as possible).
	RowDelayMs int
	// ShutdownGraceMs bounds how long graceful shutdown waits for already
	// buffered/in-flight events to reach the MQ before forcing exit.
	ShutdownGraceMs int

	// RegistryTTLSec is the heartbeat TTL the streamer requests when joining
	// the MQ partition registry (the only source of (index, total)). The MQ
	// expires consumers that stop heartbeating after this long.
	// the partition registry (seconds). The MQ expires consumers that stop
	// heartbeating after this long.
	RegistryTTLSec int
	// RegistryPollIntervalMs is how often the partition registry is polled
	// (join/heartbeat), which is also how quickly scale up/down is detected.
	RegistryPollIntervalMs int
}

// Load reads the environment and returns a validated Config.
func Load() (*Config, error) {
	cfg := &Config{
		PodName:                getEnv("POD_NAME", "telemetry-streamer-0"),
		Cluster:                getEnv("CLUSTER", DefaultCluster),
		HealthAddr:             getEnv("HEALTH_ADDR", ":8082"),
		ReadyMaxStall:          time.Duration(getEnvInt("READY_MAX_STALL_SECONDS", 300)) * time.Second,
		CSVPath:                getEnv("CSV_PATH", DefaultCSVPath),
		MQAddr:                 getEnv("MQ_ADDR", DefaultMQAddr),
		MQTokenFile:            getEnv("MQ_TOKEN_FILE", ""),
		MQToken:                getEnv("MQ_TOKEN", ""),
		ReloadCheckInterval:    getEnvInt("RELOAD_CHECK_INTERVAL_S", 5),
		BatchSize:              getEnvInt("BATCH_SIZE", 100),
		FlushIntervalMs:        getEnvInt("FLUSH_INTERVAL_MS", 200),
		RowDelayMs:             getEnvInt("ROW_DELAY_MS", 5),
		ShutdownGraceMs:        getEnvInt("SHUTDOWN_GRACE_MS", 5000),
		RegistryTTLSec:         getEnvInt("REGISTRY_TTL_S", 15),
		RegistryPollIntervalMs: getEnvInt("REGISTRY_POLL_INTERVAL_MS", 5000),
	}

	if cfg.ConsumerID = getEnv("CONSUMER_ID", ""); cfg.ConsumerID == "" {
		cfg.ConsumerID = cfg.PodName
	}
	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}
