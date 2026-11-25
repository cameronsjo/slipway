package kubernetes

import (
	"context"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestKubernetesPodSourceRequiresCompletePaginatedScope(t *testing.T) {
	runtimeClass := "sjo-kata-qemu-runner"
	client := fake.NewSimpleClientset()
	var calls atomic.Int32
	client.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		list := action.(k8stesting.ListAction)
		if list.GetNamespace() != "github-runners" || list.GetListRestrictions().Labels.String() != "app=test" {
			t.Fatalf("unexpected Pod scope: %q %q", list.GetNamespace(), list.GetListRestrictions().Labels.String())
		}
		if calls.Add(1) == 1 {
			return true, &corev1.PodList{ListMeta: metav1.ListMeta{Continue: "next"}, Items: []corev1.Pod{{
				ObjectMeta: metav1.ObjectMeta{Name: "one", Namespace: "github-runners", UID: "uid-one", Labels: map[string]string{"app": "test"}},
				Spec:       corev1.PodSpec{RuntimeClassName: &runtimeClass}}}}, nil
		}
		return true, &corev1.PodList{Items: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{
			Name: "two", Namespace: "github-runners", UID: "uid-two", Labels: map[string]string{"app": "test"}},
			Spec: corev1.PodSpec{RuntimeClassName: &runtimeClass}}}}, nil
	})
	source, err := NewKubernetesPodSource(client)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := source.ListPods(context.Background(), "github-runners", "app=test")
	if err != nil || !scope.Complete || len(scope.Items) != 2 || calls.Load() != 2 || scope.Items[1].UID != "uid-two" {
		t.Fatalf("Pod scope was incomplete: %+v calls=%d err=%v", scope, calls.Load(), err)
	}
}
