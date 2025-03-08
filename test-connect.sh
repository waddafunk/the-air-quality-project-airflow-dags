# Get credentials
POSTGRES_USER=$(az keyvault secret show --vault-name airqualitykubedbkvdev --name "airflow-postgres-user" --query value -o tsv)
POSTGRES_PASSWORD=$(az keyvault secret show --vault-name airqualitykubedbkvdev --name "airflow-postgres-password" --query value -o tsv)

# Connect locally
PGPASSWORD=$POSTGRES_PASSWORD psql -h localhost -U $POSTGRES_USER -d airflow