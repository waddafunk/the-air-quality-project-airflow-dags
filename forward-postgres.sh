source .env
source ./get_postgres_credentials.sh

# Delete forwarder if it exist already
kubectl delete pods pg-forwarder --ignore-not-found=true

# Create a temporary pod with both PostgreSQL client tools and socat for port forwarding
kubectl run pg-forwarder --image=ubuntu --restart=Never -- sleep 3600

# Wait for it to be ready
kubectl wait --for=condition=ready pod/pg-forwarder --timeout=60s

# Install necessary tools in the pod
kubectl exec pg-forwarder -- apt-get update
kubectl exec pg-forwarder -- apt-get install -y postgresql-client socat

# Set up port forwarding with socat inside the pod (background process)
kubectl exec pg-forwarder -- socat TCP-LISTEN:5432,fork TCP:$POSTGRES_HOST:5432 &

# Now forward from your local machine to the pod
kubectl port-forward pod/pg-forwarder 5432:5432