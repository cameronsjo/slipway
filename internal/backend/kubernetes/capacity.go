package kubernetes

import (
	"context"
	"errors"
	"fmt"
)

// ConservativeCapacitySource combines disjoint observations. Kubernetes owns
// CPU/RAM allocatable and non-pool reservations; the host witness owns the
// dedicated thin pool, cache, logs, and configured runtime reserve.
type ConservativeCapacitySource struct {
	Kubernetes CapacitySource
	Host       CapacitySource
}

var _ CapacitySource = (*ConservativeCapacitySource)(nil)

func (s *ConservativeCapacitySource) ObserveCapacity(ctx context.Context, namespace string) (CapacitySnapshot, error) {
	if s == nil || s.Kubernetes == nil || s.Host == nil {
		return CapacitySnapshot{Complete: false}, errors.New("capacity sources are incomplete")
	}
	kube, err := s.Kubernetes.ObserveCapacity(ctx, namespace)
	if err != nil || !kube.Complete {
		// The cause used to be replaced here rather than wrapped, so a real
		// refusal from the source below arrived at the controller as the flat
		// sentence "incomplete" -- true of a source that failed and of one that
		// simply returned Complete false, which are different faults. A nil err
		// keeps the fixed string, because there is no cause to carry.
		//
		// WHAT THE WRAP ACTUALLY CARRIES, corrected: the witness's
		// closed-vocabulary REASON CODE and the byte count from
		// safeWitnessFailure. It does NOT carry the failed tool's exit status
		// or its stderr -- those deliberately stop at the witness, which logs
		// them, and never cross the socket. An earlier version of this comment
		// claimed otherwise, which would send an operator looking in the
		// controller log for a line only the witness ever writes.
		if err != nil {
			return CapacitySnapshot{Complete: false}, fmt.Errorf("Kubernetes capacity observation is incomplete: %w", err)
		}
		return CapacitySnapshot{Complete: false}, errors.New("Kubernetes capacity observation is incomplete")
	}
	host, err := s.Host.ObserveCapacity(ctx, namespace)
	if err != nil || !host.Complete {
		// Same wrap, and this is the one that mattered: every thin-pool refusal
		// -- stale report, no logical volume, wrong owner -- reached the
		// controller as "host capacity observation is incomplete" and nothing
		// else, which is the diagnostics gap #843 closed one layer up and left
		// open here.
		if err != nil {
			return CapacitySnapshot{Complete: false}, fmt.Errorf("host capacity observation is incomplete: %w", err)
		}
		return CapacitySnapshot{Complete: false}, errors.New("host capacity observation is incomplete")
	}
	if kube.CPUMilliTotal <= 0 || kube.MemoryBytesTotal <= 0 ||
		kube.WritableStorageBytesTotal != 0 || kube.WritableStorageBytesUsed != 0 || kube.WritableStorageBytesOtherWorkloads != 0 ||
		kube.ContainerRootfsBytes != 0 ||
		kube.WritableStorageBytesRuntimeOverhead != 0 || kube.ImageCacheBytesTotal != 0 || kube.ImageCacheBytesUsed != 0 ||
		kube.LogBytesTotal != 0 || kube.LogBytesUsed != 0 || host.CPUMilliTotal != 0 || host.CPUMilliOtherWorkloads != 0 ||
		host.MemoryBytesTotal != 0 || host.MemoryBytesOtherWorkloads != 0 || host.MemoryBytesAvailable <= 0 ||
		kube.MemoryBytesAvailable != 0 || host.WritableStorageBytesTotal <= 0 ||
		host.ContainerRootfsBytes <= 0 ||
		host.ImageCacheBytesTotal <= 0 || host.LogBytesTotal <= 0 {
		return CapacitySnapshot{Complete: false}, errors.New("capacity sources overlap or omit their assigned dimensions")
	}
	return CapacitySnapshot{Complete: true,
		CPUMilliTotal: kube.CPUMilliTotal, CPUMilliOtherWorkloads: kube.CPUMilliOtherWorkloads,
		CPUMilliRuntimeOverhead: kube.CPUMilliRuntimeOverhead + host.CPUMilliRuntimeOverhead,
		MemoryBytesTotal:        kube.MemoryBytesTotal, MemoryBytesAvailable: host.MemoryBytesAvailable,
		MemoryBytesOtherWorkloads:  kube.MemoryBytesOtherWorkloads,
		MemoryBytesRuntimeOverhead: kube.MemoryBytesRuntimeOverhead + host.MemoryBytesRuntimeOverhead,
		WritableStorageBytesTotal:  host.WritableStorageBytesTotal, WritableStorageBytesUsed: host.WritableStorageBytesUsed,
		WritableStorageBytesOtherWorkloads:  host.WritableStorageBytesOtherWorkloads,
		WritableStorageBytesRuntimeOverhead: host.WritableStorageBytesRuntimeOverhead,
		ContainerRootfsBytes:                host.ContainerRootfsBytes,
		ImageCacheBytesTotal:                host.ImageCacheBytesTotal, ImageCacheBytesUsed: host.ImageCacheBytesUsed,
		LogBytesTotal: host.LogBytesTotal, LogBytesUsed: host.LogBytesUsed}, nil
}
