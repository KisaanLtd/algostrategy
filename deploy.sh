#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
#  deploy.sh — Full GCP setup for algostrategy
#  Run once from your local machine with gcloud CLI installed.
#  Safe to re-run — secrets use --data-file=- which creates a new version
#  if the secret already exists (use 'gcloud secrets versions add' on re-runs).
# ─────────────────────────────────────────────────────────────────────────────
set -e

PROJECT_ID="algostratgy"
REGION="asia-south1"                  # Mumbai — closest to NSE
IMAGE="gcr.io/$PROJECT_ID/algostrategy"
SA_NAME="algostrategy-sa"
SA_EMAIL="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"

echo "==> Setting project to $PROJECT_ID"
gcloud config set project $PROJECT_ID

# ── Enable required APIs ──────────────────────────────────────────────────────
echo ""
echo "==> Enabling APIs"
gcloud services enable \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  containerregistry.googleapis.com \
  cloudbuild.googleapis.com

# ── Service Account ───────────────────────────────────────────────────────────
echo ""
echo "==> Creating service account"
gcloud iam service-accounts create $SA_NAME \
  --display-name="AlgoStrategy Runner" 2>/dev/null || \
  echo "  (service account already exists — skipping)"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/secretmanager.secretAccessor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/run.invoker"

# ── Secrets ───────────────────────────────────────────────────────────────────
echo ""
echo "==> Creating secrets in Secret Manager"
for SECRET in db-host db-port db-user db-password db-name \
              breeze-api-key breeze-api-secret breeze-session-token; do
  read -sp "Enter value for $SECRET: " SECRET_VALUE
  echo ""
  # Create if not exists; otherwise add a new version
  if gcloud secrets describe $SECRET &>/dev/null; then
    echo -n "$SECRET_VALUE" | gcloud secrets versions add $SECRET --data-file=-
    echo "  $SECRET: new version added"
  else
    echo -n "$SECRET_VALUE" | gcloud secrets create $SECRET --data-file=-
    echo "  $SECRET: created"
  fi
done

echo ""
echo "==> Storing ca.pem as secret"
if gcloud secrets describe db-ca-cert &>/dev/null; then
  gcloud secrets versions add db-ca-cert --data-file=ca.pem
  echo "  db-ca-cert: new version added"
else
  gcloud secrets create db-ca-cert --data-file=ca.pem
  echo "  db-ca-cert: created"
fi

# ── Docker image ──────────────────────────────────────────────────────────────
echo ""
echo "==> Building and pushing Docker image (~5-8 min for TA-Lib compile)"
gcloud builds submit --tag $IMAGE .

# ── Flask web service ─────────────────────────────────────────────────────────
echo ""
echo "==> Deploying Flask web service (main.py)"
gcloud run deploy algostrategy-web \
  --image=$IMAGE \
  --platform=managed \
  --region=$REGION \
  --service-account="$SA_EMAIL" \
  --no-allow-unauthenticated \
  --memory=512Mi \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=2

# ── Cloud Run Jobs ────────────────────────────────────────────────────────────
echo ""
echo "==> Deploying Cloud Run Jobs"

# Job 1: OHLC data pipeline  (run_pipeline.sh)
#   Step 1 — tvdata.py        : truncate + bulk fetch 1000 bars via TV/Breeze
#   Step 2 — tvdata_update.py : live per-minute loop until 15:30
echo "  Deploying algostrategy-tvdata job..."
gcloud run jobs replace cloud-run-job-tvdata.yaml --region=$REGION

# Job 2: Indicator pipeline   (run_indicators.sh)
#   Step 1 — indicatordata_all.py : truncate + full recalculate all indicators
#   Step 2 — indicator_update.py  : live per-minute loop until 15:30
echo "  Deploying algostrategy-indicators job..."
gcloud run jobs replace cloud-run-job-indicators.yaml --region=$REGION

# ── Cloud Scheduler triggers ──────────────────────────────────────────────────
echo ""
echo "==> Setting up Cloud Scheduler triggers"

# Helper: update or create a scheduler trigger
_upsert_scheduler() {
  local NAME=$1 SCHEDULE=$2 URI=$3
  if gcloud scheduler jobs describe $NAME --location=$REGION &>/dev/null; then
    gcloud scheduler jobs update http $NAME \
      --schedule="$SCHEDULE" \
      --time-zone="UTC" \
      --uri="$URI" \
      --http-method=POST \
      --oauth-service-account-email="$SA_EMAIL" \
      --location=$REGION
    echo "  $NAME: updated"
  else
    gcloud scheduler jobs create http $NAME \
      --schedule="$SCHEDULE" \
      --time-zone="UTC" \
      --uri="$URI" \
      --http-method=POST \
      --oauth-service-account-email="$SA_EMAIL" \
      --location=$REGION
    echo "  $NAME: created"
  fi
}

BASE_URI="https://run.googleapis.com/v2/projects/$PROJECT_ID/locations/$REGION/jobs"

# tvdata job    → 09:14 IST = 03:44 UTC
_upsert_scheduler \
  "algostrategy-tvdata-trigger" \
  "44 3 * * 1-5" \
  "$BASE_URI/algostrategy-tvdata:run"

# indicators job → 09:19 IST = 03:49 UTC
# 5-min gap gives tvdata Step 1 (bulk fetch) time to complete first
_upsert_scheduler \
  "algostrategy-indicators-trigger" \
  "49 3 * * 1-5" \
  "$BASE_URI/algostrategy-indicators:run"

# ── Cleanup note ──────────────────────────────────────────────────────────────
echo ""
echo "✅ Deployment complete!"
echo ""
echo "   Web service  : https://algostrategy-web-xxxx-$REGION.run.app"
echo ""
echo "   Jobs and schedules:"
echo "   ┌─────────────────────────────┬────────────┬─────────────────────────────────────────┐"
echo "   │ Job                         │ IST Start  │ Script                                  │"
echo "   ├─────────────────────────────┼────────────┼─────────────────────────────────────────┤"
echo "   │ algostrategy-tvdata         │ 09:14      │ run_pipeline.sh                         │"
echo "   │                             │            │   1. tvdata.py (truncate + bulk fetch)  │"
echo "   │                             │            │   2. tvdata_update.py (live loop)       │"
echo "   ├─────────────────────────────┼────────────┼─────────────────────────────────────────┤"
echo "   │ algostrategy-indicators     │ 09:19      │ run_indicators.sh                       │"
echo "   │                             │            │   1. indicatordata_all.py (truncate+calc│"
echo "   │                             │            │   2. indicator_update.py (live loop)    │"
echo "   └─────────────────────────────┴────────────┴─────────────────────────────────────────┘"
echo ""
echo "   If old job 'algostrategy-pipeline' still exists, delete it:"
echo "   gcloud run jobs delete algostrategy-pipeline --region $REGION"
echo "   gcloud scheduler jobs delete algostrategy-market-trigger --location $REGION"