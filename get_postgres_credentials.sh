source .env

KV_NAME="airqualitykubedbkv${ENVIRONMENT}"

# Get the PostgreSQL host from Key Vault
POSTGRES_HOST=$(az keyvault secret show --vault-name airqualitykubedbkvdev --name "airflow-postgres-host" --query value -o tsv)
POSTGRES_USER=$(az keyvault secret show --vault-name "$KV_NAME" --name "airflow-postgres-user" --query value -o tsv 2>/dev/null || echo "airflow_admin")
POSTGRES_PASSWORD=$(az keyvault secret show --vault-name "$KV_NAME" --name "airflow-postgres-password" --query value -o tsv)

sed -i "s/POSTGRES_USER=.*/POSTGRES_USER=$POSTGRES_USER/" .env
sed -i "s/POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=$POSTGRES_PASSWORD/" .env

echo $POSTGRES_HOST

# don't set host since we forward it to localhost
# sed -i "s/POSTGRES_HOST=.*/POSTGRES_HOST=$POSTGRES_HOST/" .env 