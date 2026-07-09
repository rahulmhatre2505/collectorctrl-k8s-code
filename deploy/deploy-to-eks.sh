#!/bin/bash
# CollectorCtrl Operator — Quick Deploy to EKS
# Server: 172.31.30.24:4320
# Image: ghcr.io/rahulmhatre2505/collectorctrl-k8s-code/operator:latest

set -e

SERVER_IP="172.31.30.24"
SERVER_PORT="4320"
NAMESPACE="collectorctrl"
OPAMP_URL="wss://${SERVER_IP}:${SERVER_PORT}/v1/opamp"

echo "=========================================="
echo " CollectorCtrl Operator Deploy Script"
echo "=========================================="
echo ""
echo "Server:     ${SERVER_IP}:${SERVER_PORT}"
echo "OpAMP URL:  ${OPAMP_URL}"
echo "Namespace:  ${NAMESPACE}"
echo ""

# Step 0: Verify connectivity from EKS to CollectorCtrl server
echo "[Step 0/6] Verifying connectivity from EKS to CollectorCtrl server..."
echo ""
echo "Running a test pod to check reachability..."
if kubectl run test-connect --rm -i --restart=Never --image=busybox --timeout=30s -- sh -c "
  nc -z ${SERVER_IP} ${SERVER_PORT} 2>/dev/null && echo 'TCP REACHABLE' || echo 'TCP UNREACHABLE'
" 2>/dev/null | grep -q "REACHABLE"; then
  echo "✅ Server ${SERVER_IP}:${SERVER_PORT} is reachable from EKS"
else
  echo "⚠️  WARNING: Could not verify connectivity to ${SERVER_IP}:${SERVER_PORT}"
  echo "   This may mean:"
  echo "   - Security group blocks port 4320 from EKS nodes"
  echo "   - CollectorCtrl server is not running on this machine"
  echo "   - Network routing issue between EKS and EC2"
  echo ""
  echo "   Fix: Add inbound rule to EC2 security group for port 4320"
  echo "        from the EKS node security group or cluster CIDR."
  echo ""
  read -p "Continue anyway? (y/n) " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
  fi
fi
echo ""

# Step 1: Create namespace
echo "[Step 1/6] Creating namespace ${NAMESPACE}..."
kubectl apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: ${NAMESPACE}
  labels:
    app.kubernetes.io/name: collectorctrl-operator
    app.kubernetes.io/managed-by: kubectl
EOF
echo "✅ Namespace created"
echo ""

# Step 2: Create auth secret
echo "[Step 2/6] Creating auth secret..."
echo ""
if kubectl get secret collectorctrl-auth -n ${NAMESPACE} >/dev/null 2>&1; then
  echo "Secret 'collectorctrl-auth' already exists in ${NAMESPACE}"
  read -p "Update secret key? (y/n) " -n 1 -r
  echo
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    read -sp "Enter new OpAMP shared secret key: " SECRET_KEY
    echo
    kubectl create secret generic collectorctrl-auth \
      --namespace ${NAMESPACE} \
      --from-literal=secret-key="${SECRET_KEY}" \
      --dry-run=client -o yaml | kubectl apply -f -
    echo "✅ Secret updated"
  fi
else
  # Default test key for quick testing. CHANGE IN PRODUCTION.
  echo "Creating auth secret with default test key..."
  echo "   ⚠️  WARNING: This uses a default test key. For production, set your own."
  echo ""
  read -sp "Enter OpAMP shared secret key (or press Enter for default test key): " SECRET_KEY
  echo
  if [ -z "$SECRET_KEY" ]; then
    SECRET_KEY="collectorctrl-test-key-2024"
    echo "   Using default test key: collectorctrl-test-key-2024"
  fi
  kubectl create secret generic collectorctrl-auth \
    --namespace ${NAMESPACE} \
    --from-literal=secret-key="${SECRET_KEY}" \
    --dry-run=client -o yaml | kubectl apply -f -
  echo "✅ Secret created"
fi
echo ""

# Step 3: Apply CRD
echo "[Step 3/6] Applying CRD..."
kubectl apply -f deploy/crd.yaml
echo "✅ CRD applied"
echo ""

# Step 4: Apply RBAC
echo "[Step 4/6] Applying RBAC..."
kubectl apply -f deploy/rbac.yaml
echo "✅ RBAC applied"
echo ""

# Step 5: Apply operator deployment
echo "[Step 5/6] Applying operator deployment..."
kubectl apply -f deploy/deployment.yaml
echo "✅ Deployment applied"
echo ""

# Step 6: Wait for pod readiness
echo "[Step 6/6] Waiting for operator pod to be ready..."
echo ""
kubectl wait --for=condition=ready pod -n ${NAMESPACE} -l app.kubernetes.io/name=collectorctrl-operator --timeout=120s || {
  echo "⚠️  Pod did not become ready within 120s. Checking logs..."
  kubectl logs -n ${NAMESPACE} -l app.kubernetes.io/name=collectorctrl-operator --tail=50
  exit 1
}
echo "✅ Operator pod is ready"
echo ""

echo "=========================================="
echo " Operator deployed successfully!"
echo "=========================================="
echo ""
echo "Next steps:"
echo ""
echo "1. Verify operator is running:"
echo "   kubectl get pods -n ${NAMESPACE}"
echo ""
echo "2. View operator logs:"
echo "   kubectl logs -n ${NAMESPACE} -l app.kubernetes.io/name=collectorctrl-operator -f"
echo ""
echo "3. Create CollectorMonitor for Coralogix collectors:"
echo "   kubectl apply -f deploy/coralogix-collectormonitor.yaml"
echo ""
echo "4. Verify CollectorMonitor status:"
echo "   kubectl get collectormonitor -n observability"
echo "   kubectl describe collectormonitor coralogix-agent -n observability"
echo ""
echo "5. Check the CollectorCtrl UI — you should see the cluster appear as a K8s agent!"
echo ""
