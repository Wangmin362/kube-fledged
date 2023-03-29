/*
Copyright 2018 The kube-fledged authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package images

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"
	fledgedv1alpha2 "github.com/senthilrch/kube-fledged/pkg/apis/kubefledged/v1alpha2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// newImagePullJob constructs a job manifest for pulling an image to a node
func newImagePullJob(imagecache *fledgedv1alpha2.ImageCache, image string, node *corev1.Node,
	imagePullPolicy string, busyboxImage string, serviceAccountName string,
	jobPriorityClassName string) (*batchv1.Job, error) {
	var pullPolicy corev1.PullPolicy = corev1.PullIfNotPresent
	hostname := node.Labels["kubernetes.io/hostname"]
	if imagecache == nil {
		glog.Error("imagecache pointer is nil")
		return nil, fmt.Errorf("imagecache pointer is nil")
	}
	if imagePullPolicy == string(corev1.PullAlways) {
		pullPolicy = corev1.PullAlways
	} else if imagePullPolicy == string(corev1.PullIfNotPresent) {
		pullPolicy = corev1.PullIfNotPresent
		if latestimage := strings.Contains(image, ":latest") || !strings.Contains(image, ":"); latestimage {
			pullPolicy = corev1.PullAlways
		}
	}

	labels := map[string]string{
		"app":         "kubefledged",
		"kubefledged": "kubefledged-image-manager",
		"imagecache":  imagecache.Name,
		"controller":  controllerAgentName,
	}

	backoffLimit := int32(0)
	activeDeadlineSeconds := int64((time.Hour).Seconds())

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: imagecache.Name + "-",
			Namespace:    imagecache.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(imagecache, schema.GroupVersionKind{
					Group:   fledgedv1alpha2.SchemeGroupVersion.Group,
					Version: fledgedv1alpha2.SchemeGroupVersion.Version,
					Kind:    "ImageCache",
				}),
			},
			Labels: labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          &backoffLimit,
			ActiveDeadlineSeconds: &activeDeadlineSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: imagecache.Namespace,
					Labels:    labels,
				},
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{
						"kubernetes.io/hostname": hostname,
					},
					InitContainers: []corev1.Container{
						{
							Name:    "busybox",
							Image:   busyboxImage,
							Command: []string{"cp", "/bin/echo", "/tmp/bin"},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "tmp-bin",
									MountPath: "/tmp/bin",
								},
							},
							ImagePullPolicy: corev1.PullIfNotPresent,
						},
					},
					Containers: []corev1.Container{
						{
							Name:    "imagepuller",
							Image:   image,
							Command: []string{"/tmp/bin/echo", "Image pulled successfully!"},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "tmp-bin",
									MountPath: "/tmp/bin",
								},
							},
							ImagePullPolicy: pullPolicy,
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "tmp-bin",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
					RestartPolicy:    corev1.RestartPolicyNever,
					ImagePullSecrets: imagecache.Spec.ImagePullSecrets,
					Tolerations: []corev1.Toleration{
						{
							Operator: corev1.TolerationOpExists,
						},
					},
				},
			},
		},
	}
	if serviceAccountName != "" {
		job.Spec.Template.Spec.ServiceAccountName = serviceAccountName
	}
	if jobPriorityClassName != "" {
		job.Spec.Template.Spec.PriorityClassName = jobPriorityClassName
	}
	return job, nil
}

// newImageDeleteJob constructs a job manifest to delete an image from a node
func newImageDeleteJob(imagecache *fledgedv1alpha2.ImageCache, image string, node *corev1.Node,
	containerRuntimeVersion string, dockerclientimage string, serviceAccountName string,
	imageDeleteJobHostNetwork bool, jobPriorityClassName string, criSocketPath string) (*batchv1.Job, error) {
	hostname := node.Labels["kubernetes.io/hostname"]
	socketPath := criSocketPath
	if imagecache == nil {
		glog.Error("imagecache pointer is nil")
		return nil, fmt.Errorf("imagecache pointer is nil")
	}

	labels := map[string]string{
		"app":         "kubefledged",
		"kubefledged": "kubefledged-image-manager",
		"imagecache":  imagecache.Name,
		"controller":  controllerAgentName,
	}

	hostpathtype := corev1.HostPathSocket
	backoffLimit := int32(0)
	activeDeadlineSeconds := int64((time.Hour).Seconds())

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: imagecache.Name + "-",
			Namespace:    imagecache.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(imagecache, schema.GroupVersionKind{
					Group:   fledgedv1alpha2.SchemeGroupVersion.Group,
					Version: fledgedv1alpha2.SchemeGroupVersion.Version,
					Kind:    "ImageCache",
				}),
			},
			Labels: labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          &backoffLimit,
			ActiveDeadlineSeconds: &activeDeadlineSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: imagecache.Namespace,
					Labels:    labels,
				},
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{
						"kubernetes.io/hostname": hostname,
					},
					Containers: []corev1.Container{
						{
							Name:    "docker-cri-client",
							Image:   dockerclientimage,
							Command: []string{"/bin/bash"},
							Args:    []string{"-c", "exec /usr/bin/docker image rm -f " + image + " > /dev/termination-log 2>&1"},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "runtime-sock",
									MountPath: "/var/run/docker.sock",
								},
							},
							ImagePullPolicy: corev1.PullIfNotPresent,
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "runtime-sock",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/var/run/docker.sock",
									Type: &hostpathtype,
								},
							},
						},
					},
					RestartPolicy:    corev1.RestartPolicyNever,
					ImagePullSecrets: imagecache.Spec.ImagePullSecrets,
					HostNetwork:      imageDeleteJobHostNetwork,
					Tolerations: []corev1.Toleration{
						{
							Operator: corev1.TolerationOpExists,
						},
					},
				},
			},
		},
	}
	if strings.Contains(containerRuntimeVersion, "containerd") {
		if criSocketPath == "" {
			socketPath = "/run/containerd/containerd.sock"
		}
		deleteCommand := "exec /usr/bin/crictl --runtime-endpoint=unix://" + socketPath + " --image-endpoint=unix://" + socketPath + " rmi " + image + " > /dev/termination-log 2>&1"
		job.Spec.Template.Spec.Containers[0].Args = []string{"-c", deleteCommand}
		job.Spec.Template.Spec.Containers[0].VolumeMounts[0].MountPath = socketPath
		job.Spec.Template.Spec.Volumes[0].VolumeSource.HostPath.Path = socketPath
	}
	if strings.Contains(containerRuntimeVersion, "crio") || strings.Contains(containerRuntimeVersion, "cri-o") {
		if criSocketPath == "" {
			socketPath = "/var/run/crio/crio.sock"
		}
		deleteCommand := "exec /usr/bin/crictl --runtime-endpoint=unix://" + socketPath + " --image-endpoint=unix://" + socketPath + " rmi " + image + " > /dev/termination-log 2>&1"
		job.Spec.Template.Spec.Containers[0].Args = []string{"-c", deleteCommand}
		job.Spec.Template.Spec.Containers[0].VolumeMounts[0].MountPath = socketPath
		job.Spec.Template.Spec.Volumes[0].VolumeSource.HostPath.Path = socketPath
	}
	if strings.Contains(containerRuntimeVersion, "docker") {
		if criSocketPath == "" {
			socketPath = "/var/run/docker.sock"
		}
		job.Spec.Template.Spec.Containers[0].VolumeMounts[0].MountPath = socketPath
		job.Spec.Template.Spec.Volumes[0].VolumeSource.HostPath.Path = socketPath
	}
	if serviceAccountName != "" {
		job.Spec.Template.Spec.ServiceAccountName = serviceAccountName
	}
	if jobPriorityClassName != "" {
		job.Spec.Template.Spec.PriorityClassName = jobPriorityClassName
	}
	return job, nil
}

func checkIfImageNeedsToBePulled(imagePullPolicy string, image string, node *corev1.Node) (bool, error) {
	if imagePullPolicy == string(corev1.PullIfNotPresent) {
		if !strings.Contains(image, ":") && !strings.Contains(image, "@sha") {
			// 如果没有指定镜像的具体版本，默认只能拉取此镜像的latest版本
			return true, nil
		}
		if strings.Contains(image, ":latest") {
			// 如果镜像的TAG为latest，那么只能重新拉取镜像，因为本地的latest镜像可能已经不是最新的，此时需要重新拉取镜像
			return true, nil
		}
		// 判断当前节点是否存在该镜像
		imageAlreadyPresent, err := imageAlreadyPresentInNode(image, node)
		if err != nil {
			return false, err
		}
		if imageAlreadyPresent {
			// 如果已经存在该镜像了，那么不需要重新
			return false, nil
		}
	}
	return true, nil
}

func imageAlreadyPresentInNode(image string, node *corev1.Node) (bool, error) {
	// 直接从Node的状态当中获取当前Node的镜像
	// TODO 如果该节点已经有了该镜像，但是重来都没有通过这个镜像启动任何的Pod，镜像的信息会存在Node.status当中么？
	// 答：经过测试，Node.Status.Images中的镜像信息一定是当前节点上启动过K8S Pod的镜像信息，手动在节点上拉取下来的镜像并不会出现
	// 但是在这里不会有bug，因为kube-fledged是通过Job拉取镜像，因此Node.Status.Image一定会出现已经拉去过的镜像信息
	imagesByteSlice, err := json.Marshal(node.Status.Images)
	if err != nil {
		return false, err
	}
	if strings.Contains(string(imagesByteSlice), image) {
		return true, nil
	}
	return false, nil
}
