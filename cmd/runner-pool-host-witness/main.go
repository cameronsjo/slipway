// runner-pool-host-witness is the privileged Linux observer. It serves only
// bounded inventory facts over a peer-authenticated Unix socket; it never owns
// GitHub, Kubernetes, runner-registration, or bootstrap credentials.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	backendkube "github.com/cameronsjo/slipway/internal/backend/kubernetes"
)

type configFile struct {
	SocketPath          string                           `json:"socket_path"`
	SocketGID           int                              `json:"socket_gid"`
	ControllerUID       uint32                           `json:"controller_uid"`
	Namespace           string                           `json:"namespace"`
	OwnerUID            string                           `json:"owner_uid"`
	RequestTimeout      string                           `json:"request_timeout"`
	CRIExecutable       string                           `json:"cri_executable"`
	RuntimeEndpoint     string                           `json:"runtime_endpoint"`
	ProcRoot            string                           `json:"proc_root"`
	IdentityLedger      string                           `json:"identity_ledger"`
	RuntimeHandler      string                           `json:"runtime_handler"`
	RuntimeClass        string                           `json:"runtime_class"`
	CTRExecutable       string                           `json:"ctr_executable"`
	ContainerdAddress   string                           `json:"containerd_address"`
	ContainerdNS        string                           `json:"containerd_namespace"`
	Snapshotter         string                           `json:"snapshotter"`
	RuntimeFingerprints []backendkube.RuntimeFingerprint `json:"runtime_fingerprints"`
	// The witness no longer runs lvs. It reads a report a root host timer
	// publishes; see readThinPoolReport for the two measured reasons. The
	// old lvs_executable key is not accepted: DisallowUnknownFields makes a
	// config still carrying it fail to load, rather than starting a witness
	// that silently ignores half its storage configuration.
	ThinPoolReportPath   string `json:"thin_pool_report_path"`
	ThinPoolReportMaxAge int    `json:"thin_pool_report_max_age_seconds"`
	VolumeGroup          string `json:"volume_group"`
	ThinPool             string `json:"thin_pool"`
	ImageCachePath       string `json:"image_cache_path"`
	ImageCacheBudget     int64  `json:"image_cache_budget_bytes"`
	LogPath              string `json:"log_path"`
	LogBudget            int64  `json:"log_budget_bytes"`
	ProcMeminfo          string `json:"proc_meminfo"`
	StorageReserve       int64  `json:"storage_reserve_bytes"`
	ContainerRootfsBytes int64  `json:"container_rootfs_bytes"`
	RuntimeOverhead      struct {
		CPUMilli     int64 `json:"cpu_milli"`
		MemoryBytes  int64 `json:"memory_bytes"`
		StorageBytes int64 `json:"storage_bytes"`
	} `json:"runtime_overhead"`
	MaxMetadataPercent float64 `json:"max_metadata_percent"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	path := flag.String("config", "", "non-secret immutable host witness configuration")
	flag.Parse()
	if flag.NArg() != 0 || *path == "" {
		return errors.New("host witness requires exactly one config file")
	}
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		return errors.New("host witness requires Linux root for the reviewed host mounts")
	}
	config, err := loadConfig(*path)
	if err != nil {
		return err
	}
	// Text rather than JSON: these lines are read by a person running
	// `kubectl logs` on a fenced pool, not by a collector. Stderr rather than
	// stdout so a future stdout protocol cannot be corrupted by a log line.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	server, err := buildServer(config, backendkube.NewWitnessLogger(logger, time.Now))
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return server.Serve(ctx)
}

func loadConfig(path string) (configFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return configFile{}, errors.New("read host witness configuration")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config configFile
	if err := decoder.Decode(&config); err != nil {
		return configFile{}, errors.New("decode host witness configuration")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return configFile{}, errors.New("host witness configuration contains trailing data")
	}
	return config, nil
}

func buildServer(config configFile, witnessLog *backendkube.WitnessLogger) (*backendkube.HostWitnessServer, error) {
	timeout, err := time.ParseDuration(config.RequestTimeout)
	if err != nil || timeout <= 0 {
		return nil, errors.New("host witness request timeout must be positive")
	}
	processes, err := backendkube.NewHostProcessCollector(backendkube.HostProcessConfig{
		CRIExecutable: config.CRIExecutable, RuntimeEndpoint: config.RuntimeEndpoint, ProcRoot: config.ProcRoot,
		LedgerFile: config.IdentityLedger, Namespace: config.Namespace, OwnerUID: config.OwnerUID,
		RuntimeHandler: config.RuntimeHandler, RuntimeClass: config.RuntimeClass,
	})
	if err != nil {
		return nil, err
	}
	storage, err := backendkube.NewHostStorageCollector(backendkube.HostStorageConfig{
		CTRExecutable: config.CTRExecutable, ContainerdAddress: config.ContainerdAddress,
		ContainerdNamespace: config.ContainerdNS, DedicatedSnapshotter: config.Snapshotter,
		ProcessIdentitySource: processes,
		RuntimeFingerprints:   config.RuntimeFingerprints,
	})
	if err != nil {
		return nil, err
	}
	capacity, err := backendkube.NewHostCapacityCollector(backendkube.HostCapacityConfig{
		Namespace: config.Namespace, ThinPoolReportPath: config.ThinPoolReportPath,
		ThinPoolReportMaxAgeSeconds: config.ThinPoolReportMaxAge,
		VolumeGroup:                 config.VolumeGroup, ThinPool: config.ThinPool,
		ImageCachePath: config.ImageCachePath, ImageCacheBudget: config.ImageCacheBudget,
		LogPath: config.LogPath, LogBudget: config.LogBudget, ProcMeminfo: config.ProcMeminfo,
		StorageReserveBytes:  config.StorageReserve,
		ContainerRootfsBytes: config.ContainerRootfsBytes,
		RuntimeOverhead: backendkube.Footprint{CPUMilli: config.RuntimeOverhead.CPUMilli,
			MemoryBytes: config.RuntimeOverhead.MemoryBytes, StorageBytes: config.RuntimeOverhead.StorageBytes},
		MaxMetadataPercent: config.MaxMetadataPercent,
	})
	if err != nil {
		return nil, err
	}
	// The one line emitted when nothing is wrong. It states the configured
	// collectors and whether every runtime fingerprint matched, so a witness
	// that came up against a changed host says so at startup instead of at the
	// first fence. It does NOT change startup behaviour: a mismatch is a
	// warning here and still a refusal at the request that reads it.
	// The storage collector logs its own tolerated skips, which nothing else
	// can do for it: by the time the pass returns, a skip has left no trace.
	storage.SetLogger(witnessLog)

	witnessLog.Startup([]string{"processes", "storage", "capacity"},
		storage.RuntimeFingerprintCount(), storage.VerifyRuntimeFingerprints())

	return &backendkube.HostWitnessServer{SocketPath: config.SocketPath, SocketGID: config.SocketGID,
		ExpectedClientUID: config.ControllerUID, Namespace: config.Namespace, OwnerUID: config.OwnerUID,
		Timeout: timeout, Provider: backendkube.HostWitnessSources{
			Processes: processes, Storage: storage, Capacity: capacity, Log: witnessLog}}, nil
}
