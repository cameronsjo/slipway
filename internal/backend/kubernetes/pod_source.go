package kubernetes

import (
	"context"
	"errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// KubernetesPodSource gives TrustedInventory its independently authenticated
// complete Pod view. It pages the exact immutable selector and returns only the
// fields later checked against the CRI/storage witnesses.
type KubernetesPodSource struct {
	client kubernetes.Interface
}

var _ PodSource = (*KubernetesPodSource)(nil)

func NewKubernetesPodSource(client kubernetes.Interface) (*KubernetesPodSource, error) {
	if client == nil {
		return nil, errors.New("Kubernetes Pod source requires a client")
	}
	return &KubernetesPodSource{client: client}, nil
}

func (s *KubernetesPodSource) ListPods(ctx context.Context, namespace, selector string) (PodScope, error) {
	if s == nil || ctx == nil || namespace == "" || selector == "" {
		return PodScope{Complete: false}, errors.New("Kubernetes Pod source scope is empty")
	}
	options := metav1.ListOptions{LabelSelector: selector, Limit: listPageSize}
	var items []PodObject
	for {
		page, err := s.client.CoreV1().Pods(namespace).List(ctx, options)
		if err != nil || page == nil {
			return PodScope{Complete: false}, errors.New("Kubernetes Pod source listing is incomplete")
		}
		for _, pod := range page.Items {
			items = append(items, PodObject{Namespace: pod.Namespace, Name: pod.Name, UID: string(pod.UID),
				Labels: cloneStrings(pod.Labels), Annotations: cloneStrings(pod.Annotations), RuntimeClassName: valueOrEmpty(pod.Spec.RuntimeClassName)})
		}
		if page.Continue == "" {
			return PodScope{Complete: true, Items: items}, nil
		}
		options.Continue = page.Continue
	}
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
