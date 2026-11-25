package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	backendkube "github.com/cameronsjo/slipway/internal/backend/kubernetes"
	"github.com/cameronsjo/slipway/internal/controller"
	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type platformBackendFactory struct{}

func (platformBackendFactory) New(ctx context.Context, runtime RuntimeConfig, _ *controller.Store) (controller.WorkerBackend, error) {
	if runtime.Platform != "linux" {
		return nil, fmt.Errorf("%s worker backend is not configured", runtime.Platform)
	}
	if runtime.PlatformConfigFile == "" {
		return nil, errors.New("linux daemon requires platform-config-file")
	}
	platform, err := loadLinuxPlatformConfig(runtime.PlatformConfigFile)
	if err != nil {
		return nil, err
	}
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.New("cannot load in-cluster Kubernetes configuration")
	}
	// The API clientset gets a per-request timeout; restConfig itself does not.
	// Transport builds its attach executor from restConfig
	// (internal/backend/kubernetes/transport.go), and a Timeout there would
	// also cut the runner attach stream. A hung API call used to wait forever
	// under a cycle with no deadline (review R3, D8).
	apiConfig := rest.CopyConfig(restConfig)
	apiConfig.Timeout = kubernetesAPITimeout
	client, err := kubeclient.NewForConfig(apiConfig)
	if err != nil {
		return nil, errors.New("cannot create in-cluster Kubernetes client")
	}
	return composeLinuxBackend(ctx, runtime, platform, restConfig, client)
}

// kubernetesAPITimeout bounds one API request from the controller's clientset.
// Every call it makes is a single get, list, create or delete in one
// namespace; 30 s is generous for that and far under the cycle deadline.
const kubernetesAPITimeout = 30 * time.Second

type linuxPlatformConfig struct {
	Namespace        string `json:"namespace"`
	OwnerUID         string `json:"owner_uid"`
	NodeName         string `json:"node_name"`
	RuntimeClassName string `json:"runtime_class_name"`
	RunnerImage      string `json:"runner_image"`
	DockerImage      string `json:"docker_image"`
	WorkerCommand    string `json:"worker_command"`
	RunnerRoot       string `json:"runner_root"`
	CAFile           string `json:"ca_file"`
	WitnessSocket    string `json:"witness_socket"`
	WitnessServerUID uint32 `json:"witness_server_uid"`
	WitnessTimeout   string `json:"witness_timeout"`

	RunnerResources linuxContainerResources `json:"runner_resources"`
	DockerResources linuxContainerResources `json:"docker_resources"`
	RuntimeOverhead linuxFootprint          `json:"runtime_overhead"`
	Volumes         linuxVolumes            `json:"volumes"`

	PerWorkerImageCacheBytes int64 `json:"per_worker_image_cache_bytes"`
	PerWorkerLogBytes        int64 `json:"per_worker_log_bytes"`
	ContainerRootfsBytes     int64 `json:"container_rootfs_bytes"`
}

type linuxFootprint struct {
	CPUMilli     int64 `json:"cpu_milli"`
	MemoryBytes  int64 `json:"memory_bytes"`
	StorageBytes int64 `json:"storage_bytes"`
}

type linuxContainerResources struct {
	Requests linuxFootprint `json:"requests"`
	Limits   linuxFootprint `json:"limits"`
}

// linuxVolumes are the worker's memory-backed volumes. There is no docker
// volume: dockerd's data root is on its own devmapper disk (homelab#983), and
// the loader's DisallowUnknownFields refuses a config still naming one.
type linuxVolumes struct {
	WorkBytes   int64 `json:"work_bytes"`
	SocketBytes int64 `json:"socket_bytes"`
	TmpBytes    int64 `json:"tmp_bytes"`
}

func loadLinuxPlatformConfig(path string) (linuxPlatformConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return linuxPlatformConfig{}, errors.New("read Linux platform configuration")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config linuxPlatformConfig
	if err := decoder.Decode(&config); err != nil {
		return linuxPlatformConfig{}, errors.New("decode Linux platform configuration")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return linuxPlatformConfig{}, errors.New("Linux platform configuration contains trailing data")
	}
	return config, nil
}

func composeLinuxBackend(_ context.Context, runtime RuntimeConfig, platform linuxPlatformConfig, restConfig *rest.Config, client kubeclient.Interface) (controller.WorkerBackend, error) {
	witnessTimeout, err := time.ParseDuration(platform.WitnessTimeout)
	if err != nil || witnessTimeout <= 0 {
		return nil, errors.New("Linux witness timeout must be a positive duration")
	}
	worker := backendkube.Config{
		Namespace: platform.Namespace, OwnerUID: platform.OwnerUID, NodeName: platform.NodeName,
		RuntimeClassName: platform.RuntimeClassName, RunnerImage: platform.RunnerImage, DockerImage: platform.DockerImage,
		WorkerCommand: platform.WorkerCommand, RunnerRoot: platform.RunnerRoot, CAFile: platform.CAFile,
		BootstrapURL: runtime.BootstrapURL, Capacity: runtime.Capacity, LegacySlots: runtime.LegacySlots,
		RunnerResources: resourcesFromFile(platform.RunnerResources), DockerResources: resourcesFromFile(platform.DockerResources),
		Overhead: footprintFromFile(platform.RuntimeOverhead),
		Volumes: backendkube.VolumeBudget{Work: platform.Volumes.WorkBytes,
			Socket: platform.Volumes.SocketBytes, Tmp: platform.Volumes.TmpBytes},
		ContainerRootfsBytes: platform.ContainerRootfsBytes,
	}
	host := &backendkube.HostWitnessClient{SocketPath: platform.WitnessSocket, ExpectedServerUID: platform.WitnessServerUID, Timeout: witnessTimeout}
	pods, err := backendkube.NewKubernetesPodSource(client)
	if err != nil {
		return nil, err
	}
	workloads, err := backendkube.NewKubernetesCapacitySource(client, platform.NodeName, platform.Namespace, platform.OwnerUID)
	if err != nil {
		return nil, err
	}
	capacity := &backendkube.ConservativeCapacitySource{Kubernetes: workloads, Host: host}
	budget := backendkube.AdmissionBudget{MaxWorkers: runtime.Capacity, LegacySlots: runtime.LegacySlots,
		ContainerRootfsBytes: platform.ContainerRootfsBytes,
		PerWorker: backendkube.Footprint{
			CPUMilli:     platform.RunnerResources.Requests.CPUMilli + platform.DockerResources.Requests.CPUMilli + platform.RuntimeOverhead.CPUMilli,
			MemoryBytes:  platform.RunnerResources.Requests.MemoryBytes + platform.DockerResources.Requests.MemoryBytes + platform.RuntimeOverhead.MemoryBytes,
			StorageBytes: backendkube.PerWorkerStorageBytes(platform.ContainerRootfsBytes, platform.RuntimeOverhead.StorageBytes)},
		PerWorkerImageCacheBytes: platform.PerWorkerImageCacheBytes, PerWorkerLogBytes: platform.PerWorkerLogBytes}
	inventory, err := backendkube.NewTrustedInventory(backendkube.TrustedPhysicalSources{Pods: pods, Processes: host, Storage: host, Capacity: capacity}, worker, budget)
	if err != nil {
		return nil, err
	}
	transport, err := backendkube.NewTransport(restConfig, client, inventory, worker)
	if err != nil {
		return nil, err
	}
	return backendkube.New(transport, worker)
}

func footprintFromFile(value linuxFootprint) backendkube.Footprint {
	return backendkube.Footprint{CPUMilli: value.CPUMilli, MemoryBytes: value.MemoryBytes, StorageBytes: value.StorageBytes}
}

func resourcesFromFile(value linuxContainerResources) backendkube.ContainerResources {
	return backendkube.ContainerResources{Requests: footprintFromFile(value.Requests), Limits: footprintFromFile(value.Limits)}
}
