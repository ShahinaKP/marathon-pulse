#!/usr/bin/env bash
# devops/provision.sh
#
# Provisions everything Marathon Pulse needs in Azure and deploys the worker.
# Run from the repo root in Git Bash, WSL, macOS/Linux or Azure Cloud Shell:
#
#   az login
#   export PG_ADMIN_PASSWORD='ChangeMe123abc'   # 12+ chars, letters+digits, upper+lower+digit
#   export LOCATION=northeurope                 # region for everything except (optionally) Postgres
#   bash devops/provision.sh infra              # 1) RG, storage, queue, Postgres, ACR, env
#   python devops/apply_schema.py               # 2) create tables (uses server/.env)
#   bash devops/provision.sh deploy             # 3) build worker image in ACR + deploy app
#
# Optional: PG_LOCATION=<region> puts ONLY the Postgres server in another region.
#   Needed when your subscription can't create the Postgres SKU in LOCATION
#   (error: SkuNotAvailable). The server may live in a different region than
#   its resource group.
#
# Re-running any step is safe: every command is create-or-update.
set -euo pipefail

# ----------------------------------------------------------------------------
# Settings (override with environment variables)
# ----------------------------------------------------------------------------
PREFIX="${PREFIX:-mpulse}"
LOCATION="${LOCATION:-northeurope}"
PG_LOCATION="${PG_LOCATION:-$LOCATION}"
RG="${RG:-rg-${PREFIX}}"

# Globally-unique names need a stable suffix. Stored locally so re-runs reuse it.
SUFFIX_FILE="devops/.suffix"
if [[ -f "$SUFFIX_FILE" ]]; then
  SUFFIX="$(cat "$SUFFIX_FILE")"
else
  SUFFIX="$(printf '%05x' $(( (RANDOM << 15 | RANDOM) % 1048576 )))"
  echo "$SUFFIX" > "$SUFFIX_FILE"
fi

STORAGE="st${PREFIX}${SUFFIX}"          # 3-24 chars, lowercase+digits only
PG_SERVER="pg-${PREFIX}-${SUFFIX}"
PG_ADMIN="${PG_ADMIN:-mpadmin}"
PG_DB="marathon_pulse"
ACR="acr${PREFIX}${SUFFIX}"
LAW="law-${PREFIX}"
CAE="cae-${PREFIX}"
APP="ca-${PREFIX}-worker"
BLOB_CONTAINER="excel-uploads"
QUEUE="excel-jobs"
IMAGE_TAG="${IMAGE_TAG:-$(date +%Y%m%d%H%M%S)}"
GENERATED_ENV="devops/generated.env"    # contains secrets; *.env is gitignored

# ----------------------------------------------------------------------------
# Checks
# ----------------------------------------------------------------------------
require_password() {
  : "${PG_ADMIN_PASSWORD:?Set PG_ADMIN_PASSWORD first (12+ chars, letters and digits)}"
  if ! [[ "$PG_ADMIN_PASSWORD" =~ ^[A-Za-z0-9]{12,}$ \
        && "$PG_ADMIN_PASSWORD" =~ [a-z] && "$PG_ADMIN_PASSWORD" =~ [A-Z] \
        && "$PG_ADMIN_PASSWORD" =~ [0-9] ]]; then
    echo "PG_ADMIN_PASSWORD must be 12+ characters, letters and digits only," >&2
    echo "with at least one lowercase, one uppercase and one digit (it goes into a URL)." >&2
    exit 1
  fi
}

# Fails early (and starts registration) if a resource provider is not registered,
# instead of failing half-way through with MissingSubscriptionRegistration.
preflight() {
  az account show -o none 2>/dev/null || { echo "Not logged in. Run: az login" >&2; exit 1; }
  local pending=0 p state
  for p in Microsoft.Storage Microsoft.DBforPostgreSQL Microsoft.ContainerRegistry \
           Microsoft.OperationalInsights Microsoft.App; do
    state="$(az provider show -n "$p" --query registrationState -o tsv 2>/dev/null || true)"
    if [[ "$state" != "Registered" ]]; then
      echo "  provider $p is '${state:-unknown}'"
      if [[ "$state" != "Registering" ]]; then
        az provider register --namespace "$p" -o none || true
      fi
      pending=1
    fi
  done
  if (( pending )); then
    echo "Some providers are still registering. Wait 1-3 minutes, then re-run this command." >&2
    exit 1
  fi
}

# ----------------------------------------------------------------------------
infra() {
  require_password
  echo "==> Pre-flight checks"
  preflight

  echo "==> Resource group"
  if az group show -n "$RG" -o none 2>/dev/null; then
    EXISTING_LOC="$(az group show -n "$RG" --query location -o tsv)"
    if [[ "$EXISTING_LOC" != "$LOCATION" ]]; then
      echo "    '$RG' already exists in '$EXISTING_LOC'; using that region for all resources (LOCATION was '$LOCATION')."
      LOCATION="$EXISTING_LOC"
    fi
  else
    az group create -n "$RG" -l "$LOCATION" -o none
  fi

  echo "==> Storage account (blob container + queue)"
  az storage account create -n "$STORAGE" -g "$RG" -l "$LOCATION" \
    --sku Standard_LRS --kind StorageV2 --allow-blob-public-access false \
    --min-tls-version TLS1_2 -o none
  STORAGE_CONN="$(az storage account show-connection-string -n "$STORAGE" -g "$RG" --query connectionString -o tsv)"
  az storage container create -n "$BLOB_CONTAINER" --connection-string "$STORAGE_CONN" -o none
  az storage queue create -n "$QUEUE" --connection-string "$STORAGE_CONN" -o none

  echo "==> PostgreSQL Flexible Server (Burstable B1ms) in '${PG_LOCATION}' - takes 5-10 minutes"
  MY_IP="${MY_IP:-$(curl -s --max-time 10 https://api.ipify.org || true)}"
  if ! [[ "$MY_IP" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Could not detect your public IP. Re-run with: MY_IP=<your-ip> bash devops/provision.sh infra" >&2
    exit 1
  fi

  PG_STATE="$(az postgres flexible-server show --name "$PG_SERVER" --resource-group "$RG" \
              --query state -o tsv 2>/dev/null || true)"
  case "$PG_STATE" in
    Ready)
      echo "    server $PG_SERVER already exists and is Ready - skipping create"
      ;;
    "")
      if ! az postgres flexible-server create --name "$PG_SERVER" --resource-group "$RG" \
            --location "$PG_LOCATION" \
            --admin-user "$PG_ADMIN" --admin-password "$PG_ADMIN_PASSWORD" \
            --sku-name Standard_B1ms --tier Burstable --storage-size 32 --version 16 \
            --public-access "$MY_IP" --yes -o none; then
        echo "" >&2
        echo "Postgres creation failed. If the error was SkuNotAvailable, your subscription cannot" >&2
        echo "use this size in '${PG_LOCATION}'. Try another region, e.g.:" >&2
        echo "    PG_LOCATION=uksouth bash devops/provision.sh infra" >&2
        exit 1
      fi
      ;;
    *)
      echo "Server $PG_SERVER exists but its state is '$PG_STATE' (not Ready)." >&2
      echo "Wait for it to become Ready, or delete it: az postgres flexible-server delete -g $RG -n $PG_SERVER --yes" >&2
      exit 1
      ;;
  esac

  DB_COUNT="$(az postgres flexible-server db list --resource-group "$RG" --server-name "$PG_SERVER" \
              --query "[?name=='${PG_DB}'] | length(@)" -o tsv 2>/dev/null || echo 0)"
  if [[ "$DB_COUNT" != "1" ]]; then
    az postgres flexible-server db create --resource-group "$RG" --server-name "$PG_SERVER" \
      --name "$PG_DB" -o none
  fi
  # Let Azure-hosted workloads (the Container App) connect: 0.0.0.0 = "Azure services"
  az postgres flexible-server firewall-rule create --resource-group "$RG" --server-name "$PG_SERVER" \
    --name AllowAzureServices --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0 -o none
  # And keep your current IP allowed (it changes between networks)
  az postgres flexible-server firewall-rule create --resource-group "$RG" --server-name "$PG_SERVER" \
    --name AllowMyIp --start-ip-address "$MY_IP" --end-ip-address "$MY_IP" -o none

  echo "==> Container Registry, Log Analytics, Container Apps environment"
  az acr create -n "$ACR" -g "$RG" -l "$LOCATION" --sku Basic --admin-enabled true -o none
  az monitor log-analytics workspace create -g "$RG" -n "$LAW" -l "$LOCATION" -o none
  LAW_ID="$(az monitor log-analytics workspace show -g "$RG" -n "$LAW" --query customerId -o tsv)"
  LAW_KEY="$(az monitor log-analytics workspace get-shared-keys -g "$RG" -n "$LAW" --query primarySharedKey -o tsv)"
  az containerapp env create -n "$CAE" -g "$RG" -l "$LOCATION" \
    --logs-workspace-id "$LAW_ID" --logs-workspace-key "$LAW_KEY" -o none

  PG_HOST="${PG_SERVER}.postgres.database.azure.com"
  DB_URL="postgresql://${PG_ADMIN}:${PG_ADMIN_PASSWORD}@${PG_HOST}:5432/${PG_DB}?sslmode=require"

  echo "==> Writing ${GENERATED_ENV} and server/.env, worker/.env (secrets - never commit)"
  cat > "$GENERATED_ENV" <<EOF
DATABASE_URL="${DB_URL}"
AZURE_STORAGE_CONNECTION_STRING="${STORAGE_CONN}"
BLOB_CONTAINER_NAME="${BLOB_CONTAINER}"
QUEUE_NAME="${QUEUE}"
EOF
  cp "$GENERATED_ENV" server/.env
  cp "$GENERATED_ENV" worker/.env
  echo "Done. Next: python devops/apply_schema.py"
}

# ----------------------------------------------------------------------------
deploy() {
  [[ -f "$GENERATED_ENV" ]] || { echo "Run '$0 infra' first."; exit 1; }
  # shellcheck disable=SC1090
  set -a; source "$GENERATED_ENV"; set +a

  echo "==> Building worker image in ACR (no local Docker needed)"
  az acr build -r "$ACR" -t "marathon-worker:${IMAGE_TAG}" worker/

  ACR_SERVER="$(az acr show -n "$ACR" --query loginServer -o tsv)"
  ACR_USER="$(az acr credential show -n "$ACR" --query username -o tsv)"
  ACR_PASS="$(az acr credential show -n "$ACR" --query 'passwords[0].value' -o tsv)"
  IMAGE="${ACR_SERVER}/marathon-worker:${IMAGE_TAG}"

  if az containerapp show -n "$APP" -g "$RG" -o none 2>/dev/null; then
    echo "==> Updating Container App '${APP}' to the new image"
    az containerapp update -n "$APP" -g "$RG" --image "$IMAGE" -o none
  else
    echo "==> Creating Container App '${APP}' (min replicas 0, KEDA azure-queue scaler)"
    # NOTE: scale-rule flags need a recent Azure CLI; run 'az upgrade' and
    # 'az extension update -n containerapp' if they are rejected.
    az containerapp create -n "$APP" -g "$RG" --environment "$CAE" \
      --image "$IMAGE" \
      --registry-server "$ACR_SERVER" --registry-username "$ACR_USER" --registry-password "$ACR_PASS" \
      --cpu 0.5 --memory 1.0Gi \
      --min-replicas 0 --max-replicas "${MAX_REPLICAS:-3}" \
      --secrets "storage-conn=${AZURE_STORAGE_CONNECTION_STRING}" "db-url=${DATABASE_URL}" \
      --env-vars \
        "DATABASE_URL=secretref:db-url" \
        "AZURE_STORAGE_CONNECTION_STRING=secretref:storage-conn" \
        "BLOB_CONTAINER_NAME=${BLOB_CONTAINER}" \
        "QUEUE_NAME=${QUEUE}" \
        "BATCH_SIZE=${BATCH_SIZE:-1000}" \
      --scale-rule-name queue-scaler \
      --scale-rule-type azure-queue \
      --scale-rule-metadata "accountName=${STORAGE}" "queueName=${QUEUE}" "queueLength=1" \
      --scale-rule-auth "connection=storage-conn" \
      -o none
  fi

  echo "Deployed ${IMAGE}"
  echo "Watch replicas : az containerapp replica list -n $APP -g $RG -o table"
  echo "Tail logs      : az containerapp logs show -n $APP -g $RG --follow"
}

# ----------------------------------------------------------------------------
case "${1:-}" in
  infra)  infra ;;
  deploy) deploy ;;
  *) echo "Usage: bash devops/provision.sh {infra|deploy}"; exit 1 ;;
esac