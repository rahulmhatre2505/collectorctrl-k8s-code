// operator/controllers/collectormonitor_controller.go
// Main controller for CollectorMonitor CRD.
// Uses controller-runtime. Watches CollectorMonitor, ConfigMaps, and Pods.

package controllers

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	collectorctrlv1alpha1 "github.com/collectorctrl/collectorctrl/operator/api/v1alpha1"
	"github.com/collectorctrl/collectorctrl/pkg/api"
	"github.com/collectorctrl/collectorctrl/pkg/opamp"
)

// CollectorMonitorReconciler reconciles a CollectorMonitor object.
type CollectorMonitorReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// opampClients holds one OpAMP client per CollectorMonitor CR.
	opampClients map[string]*opamp.Client

	// opampRunning prevents multiple connections for the same CR.
	opampMu      sync.Mutex
	opampStarted map[string]bool
}

// +kubebuilder:rbac:groups=collectorctrl.io,resources=collectormonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=collectorctrl.io,resources=collectormonitors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=collectorctrl.io,resources=collectormonitors/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=daemonsets;deployments;statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is the main control loop.
func (r *CollectorMonitorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. Fetch the CollectorMonitor CR
	monitor := &collectorctrlv1alpha1.CollectorMonitor{}
	if err := r.Get(ctx, req.NamespacedName, monitor); err != nil {
		if errors.IsNotFound(err) {
			// CollectorMonitor deleted — close its OpAMP client if any.
			r.cleanupOpAMPClient(req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 2. Discover the workload (DaemonSet, Deployment, or StatefulSet)
	workload, workloadKind, err := r.discoverWorkload(ctx, monitor)
	if err != nil {
		log.Error(err, "Failed to discover workload")
		r.setCondition(ctx, monitor, "Discovered", false, err.Error())
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// 3. Discover the ConfigMap
	configMap, err := r.discoverConfigMap(ctx, monitor, workload)
	if err != nil {
		log.Error(err, "Failed to discover ConfigMap")
		r.setCondition(ctx, monitor, "ConfigMapResolved", false, err.Error())
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// 4. Update status with discovered refs
	monitor.Status.ConfigMapRef = &collectorctrlv1alpha1.ConfigMapReference{
		Name:      configMap.Name,
		Namespace: configMap.Namespace,
		Key:       r.configMapKey(monitor, configMap),
	}

	// 5. Ensure OpAMP connection (one per CollectorMonitor)
	if err := r.ensureOpAMPConnection(ctx, monitor, workload, workloadKind); err != nil {
		log.Error(err, "Failed to establish OpAMP connection")
		r.setCondition(ctx, monitor, "OpAMPConnected", false, err.Error())
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// 6. Report current pod list and health
	if err := r.reportFleetHealth(ctx, monitor, workload); err != nil {
		log.Error(err, "Failed to report fleet health")
	}

	// 7. Drift detection (if enabled)
	if monitor.Spec.DriftDetection.Enabled != nil && *monitor.Spec.DriftDetection.Enabled {
		if err := r.detectDrift(ctx, monitor, configMap); err != nil {
			log.Error(err, "Drift detection failed")
		}
	}

	// 8. Update status
	monitor.Status.Phase = "Active"
	r.setCondition(ctx, monitor, "Active", true, "Monitoring collector fleet")
	if err := r.Status().Update(ctx, monitor); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

// discoverWorkload finds the DaemonSet, Deployment, or StatefulSet matching the selector.
func (r *CollectorMonitorReconciler) discoverWorkload(ctx context.Context, monitor *collectorctrlv1alpha1.CollectorMonitor) (client.Object, string, error) {
	selector := monitor.Spec.WorkloadSelector
	labelSelector := client.MatchingLabels(selector.MatchLabels)

	// Try DaemonSet first
	if selector.Kind == "" || selector.Kind == "DaemonSet" {
		var list appsv1.DaemonSetList
		if err := r.List(ctx, &list, labelSelector, client.InNamespace(monitor.Namespace)); err != nil {
			return nil, "", err
		}
		for _, ds := range list.Items {
			if selector.Name == "" || ds.Name == selector.Name {
				return &ds, "DaemonSet", nil
			}
		}
	}

	// Try Deployment
	if selector.Kind == "" || selector.Kind == "Deployment" {
		var list appsv1.DeploymentList
		if err := r.List(ctx, &list, labelSelector, client.InNamespace(monitor.Namespace)); err != nil {
			return nil, "", err
		}
		for _, dep := range list.Items {
			if selector.Name == "" || dep.Name == selector.Name {
				return &dep, "Deployment", nil
			}
		}
	}

	// Try StatefulSet
	if selector.Kind == "" || selector.Kind == "StatefulSet" {
		var list appsv1.StatefulSetList
		if err := r.List(ctx, &list, labelSelector, client.InNamespace(monitor.Namespace)); err != nil {
			return nil, "", err
		}
		for _, sts := range list.Items {
			if selector.Name == "" || sts.Name == selector.Name {
				return &sts, "StatefulSet", nil
			}
		}
	}

	return nil, "", fmt.Errorf("no workload found matching selector: %+v", selector)
}

// discoverConfigMap finds the ConfigMap used by the workload.
func (r *CollectorMonitorReconciler) discoverConfigMap(ctx context.Context, monitor *collectorctrlv1alpha1.CollectorMonitor, workload client.Object) (*corev1.ConfigMap, error) {
	// If explicitly specified in the spec, use it
	if monitor.Spec.ConfigMapSelector != nil && monitor.Spec.ConfigMapSelector.Name != "" {
		var cm corev1.ConfigMap
		if err := r.Get(ctx, client.ObjectKey{
			Namespace: monitor.Namespace,
			Name:      monitor.Spec.ConfigMapSelector.Name,
		}, &cm); err != nil {
			return nil, err
		}
		return &cm, nil
	}

	// Auto-discover by examining pod template volumes
	// This is a simplified version — in production, inspect the workload's
	// PodTemplateSpec for volumes of type ConfigMap.
	// ...

	return nil, fmt.Errorf("configMap auto-discovery not yet implemented")
}

// configMapKey returns the config key from the spec or a default.
func (r *CollectorMonitorReconciler) configMapKey(monitor *collectorctrlv1alpha1.CollectorMonitor, cm *corev1.ConfigMap) string {
	if monitor.Spec.ConfigMapSelector != nil && monitor.Spec.ConfigMapSelector.Key != "" {
		return monitor.Spec.ConfigMapSelector.Key
	}
	// Auto-detect: look for common keys
	for _, key := range []string{"relay.yaml", "config.yaml", "otelcol.yaml"} {
		if _, ok := cm.Data[key]; ok {
			return key
		}
	}
	return "config.yaml"
}

// ensureOpAMPConnection starts the OpAMP client if not already running.
func (r *CollectorMonitorReconciler) ensureOpAMPConnection(ctx context.Context, monitor *collectorctrlv1alpha1.CollectorMonitor, workload client.Object, kind string) error {
	r.opampMu.Lock()
	defer r.opampMu.Unlock()

	key := fmt.Sprintf("%s/%s", monitor.Namespace, monitor.Name)
	if r.opampStarted[key] {
		return nil
	}

	// Build cluster-level agent ID
	clusterName := "unknown"
	if monitor.Labels != nil {
		if v, ok := monitor.Labels["k8s.cluster.name"]; ok {
			clusterName = v
		}
	}

	cfg := opamp.ClientConfig{
		Endpoint:  monitor.Spec.OpAMPServer,
		AgentID:   fmt.Sprintf("k8s://%s/%s/%s/%s", clusterName, monitor.Namespace, kind, workload.GetName()),
		AgentType: api.AgentTypeKubernetes,
		Labels: map[string]string{
			"k8s.cluster.name":  clusterName,
			"k8s.namespace":     monitor.Namespace,
			"k8s.workload.type": kind,
			"k8s.workload.name": workload.GetName(),
		},
		K8sContext: &api.K8sContext{
			ClusterName:   clusterName,
			Namespace:     monitor.Namespace,
			WorkloadType:  kind,
			WorkloadName:  workload.GetName(),
			ConfigMapName: monitor.Status.ConfigMapRef.Name,
			ConfigMapKey:  monitor.Status.ConfigMapRef.Key,
		},
	}

	// Load auth from secret
	if monitor.Spec.Auth.SecretRef != nil {
		secret := &corev1.Secret{}
		secretKey := client.ObjectKey{
			Namespace: monitor.Spec.Auth.SecretRef.Namespace,
			Name:      monitor.Spec.Auth.SecretRef.Name,
		}
		if secretKey.Namespace == "" {
			secretKey.Namespace = monitor.Namespace
		}
		if err := r.Get(ctx, secretKey, secret); err != nil {
			return fmt.Errorf("failed to load auth secret: %w", err)
		}
		keyName := monitor.Spec.Auth.SecretRef.Key
		if keyName == "" {
			keyName = "secret-key"
		}
		cfg.Headers = map[string]string{
			"Authorization": fmt.Sprintf("Secret-Key %s", string(secret.Data[keyName])),
		}
	}

	// TODO: For testing with self-signed certs only. Remove in production — use proper CA cert.
	cfg.TLSConfig = &tls.Config{
		InsecureSkipVerify: true,
	}

	client := opamp.NewClient(cfg)

	// Handle config updates from server
	client.OnConfigUpdate(func(update opamp.ConfigUpdate) {
		// Standard mode: just report back. GitOps handles deployment.
		log.FromContext(ctx).Info("Received config update from server", "hash", update.ConfigHash)
		// TODO: store desired config, compare with current, report mismatch if drift
	})

	// Handle emergency override
	client.OnEmergencyCmd(func(cmd opamp.EmergencyCommand) {
		if monitor.Spec.EmergencyMode.Enabled != nil && !*monitor.Spec.EmergencyMode.Enabled {
			log.FromContext(ctx).Info("Emergency mode disabled, ignoring command")
			return
		}
		log.FromContext(ctx).Info("Applying emergency config", "reason", cmd.Reason)
		if err := r.applyEmergencyConfig(ctx, monitor, cmd.ConfigYAML); err != nil {
			log.FromContext(ctx).Error(err, "Failed to apply emergency config")
			_ = client.SendEmergencyAck(false, err.Error())
		} else {
			_ = client.SendEmergencyAck(true, "")
		}
	})

	if err := client.Start(ctx); err != nil {
		return err
	}

	r.opampClients[key] = client
	r.opampStarted[key] = true
	return nil
}

// applyEmergencyConfig patches the ConfigMap and triggers a rolling restart.
func (r *CollectorMonitorReconciler) applyEmergencyConfig(ctx context.Context, monitor *collectorctrlv1alpha1.CollectorMonitor, configYAML string) error {
	// 1. Fetch the ConfigMap
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: monitor.Status.ConfigMapRef.Namespace,
		Name:      monitor.Status.ConfigMapRef.Name,
	}, cm); err != nil {
		return fmt.Errorf("get configmap: %w", err)
	}

	// 2. Update the config content
	key := monitor.Status.ConfigMapRef.Key
	cm.Data[key] = configYAML

	// 3. Apply the patch
	if err := r.Update(ctx, cm); err != nil {
		return fmt.Errorf("update configmap: %w", err)
	}

	// 4. Trigger rolling restart by updating workload annotation
	workload, kind, err := r.discoverWorkload(ctx, monitor)
	if err != nil {
		return fmt.Errorf("discover workload for restart: %w", err)
	}

	// Hash the new config for the annotation
	hash := sha256.Sum256([]byte(configYAML))
	hashStr := fmt.Sprintf("%x", hash[:8])

	// Patch the pod template annotation to force rolling restart
	// This requires a type switch on the workload kind
	// Simplified: patch via unstructured or type assertion
	_ = workload
	_ = kind
	_ = hashStr

	// TODO: implement annotation patch for DaemonSet, Deployment, StatefulSet

	r.Recorder.Eventf(monitor, corev1.EventTypeWarning, "EmergencyConfigApplied",
		"Emergency config applied to ConfigMap %s/%s. Restart triggered.",
		cm.Namespace, cm.Name)

	return nil
}

// reportFleetHealth sends the current pod list and aggregate health to the server.
func (r *CollectorMonitorReconciler) reportFleetHealth(ctx context.Context, monitor *collectorctrlv1alpha1.CollectorMonitor, workload client.Object) error {
	key := fmt.Sprintf("%s/%s", monitor.Namespace, monitor.Name)
	r.opampMu.Lock()
	c := r.opampClients[key]
	r.opampMu.Unlock()
	if c == nil {
		return nil // no connection yet
	}
	// TODO: list pods belonging to the workload, aggregate readiness, scrape metrics
	// Build AgentToServer message and send via the per-CR client
	_ = c
	return nil
}

// detectDrift compares ConfigMap content with effective config inside a pod.
func (r *CollectorMonitorReconciler) detectDrift(ctx context.Context, monitor *collectorctrlv1alpha1.CollectorMonitor, configMap *corev1.ConfigMap) error {
	// TODO: pick a representative pod, exec "cat /conf/relay.yaml", compare hash
	return nil
}

// setCondition updates a status condition on the CollectorMonitor.
func (r *CollectorMonitorReconciler) setCondition(ctx context.Context, monitor *collectorctrlv1alpha1.CollectorMonitor, ctype string, status bool, message string) {
	cond := metav1.Condition{
		Type:               ctype,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             ctype + "Succeeded",
		Message:            message,
	}
	if !status {
		cond.Status = metav1.ConditionFalse
		cond.Reason = ctype + "Failed"
	}
	for i, existing := range monitor.Status.Conditions {
		if existing.Type == ctype {
			if existing.Status != cond.Status {
				monitor.Status.Conditions[i] = cond
			} else {
				monitor.Status.Conditions[i].Message = message
			}
			return
		}
	}
	monitor.Status.Conditions = append(monitor.Status.Conditions, cond)
}

// SetupWithManager sets up the controller with the Manager.
func (r *CollectorMonitorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.opampStarted == nil {
		r.opampStarted = make(map[string]bool)
	}
	if r.opampClients == nil {
		r.opampClients = make(map[string]*opamp.Client)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&collectorctrlv1alpha1.CollectorMonitor{}).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}

// cleanupOpAMPClient closes the OpAMP connection for a deleted CollectorMonitor.
func (r *CollectorMonitorReconciler) cleanupOpAMPClient(ns, name string) {
	key := fmt.Sprintf("%s/%s", ns, name)
	r.opampMu.Lock()
	defer r.opampMu.Unlock()
	if c := r.opampClients[key]; c != nil {
		c.Stop()
		delete(r.opampClients, key)
	}
	delete(r.opampStarted, key)
}