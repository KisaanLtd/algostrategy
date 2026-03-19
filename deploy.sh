#!/bin/bash
# ─────────────────────────────────────────────
#  deploy.sh — One-time GCP setup for algostrategy
#  Run from your local machine with gcloud CLI installed
# ─────────────────────────────────────────────

PROJECT_ID="algostratgy"         # ← CHANGE THIS
REGION="asia-south1"                       # Mumbai — closest to NSE
IMAGE="gcr.io/$PROJECT_ID/algostrategy"
SA_NAME="algostrategy-sa"

echo "==> Setting project"
gcloud config set project $PROJECT_ID

# ── Enable required APIs ──────────────────────
echo "==> Enabling APIs"
gcloud services enable \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  containerregistry.googleapis.com \
  cloudbuild.googleapis.com

# ── Create Service Account ────────────────────
echo "==> Creating service account"
gcloud iam service-accounts create $SA_NAME \
  --display-name="AlgoStrategy Runner"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/run.invoker"

# ── Store secrets in Secret Manager ──────────
echo "==> Creating secrets (fill values when prompted)"
for SECRET in db-host db-port db-user db-password db-name breeze-api-key breeze-api-secret breeze-session-token; do
  read -sp "Enter value for $SECRET: " SECRET_VALUE
  echo ""
  echo -n "$SECRET_VALUE" | gcloud secrets create $SECRET --data-file=-
done

# ── Copy ca.pem into Secret Manager ──────────
echo "==> Storing ca.pem as secret"
gcloud secrets create db-ca-cert --data-file=ca.pem

# ── Build and push Docker image ───────────────
echo "==> Building Docker image (this takes ~5-8 min for TA-Lib compile)"
gcloud builds submit --tag $IMAGE .

# ── Deploy Flask service (main.py) ────────────
echo "==> Deploying Flask web service"
gcloud run deploy algostrategy-web \
  --image=$IMAGE \
  --platform=managed \
  --region=$REGION \
  --service-account="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --no-allow-unauthenticated \
  --memory=512Mi \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=2

# ── Deploy data pipeline as Cloud Run Job ─────
echo "==> Deploying tvdata pipeline Job"
gcloud run jobs replace cloud-run-job.yaml \
  --region=$REGION

# ── Schedule Job at 09:14 IST Mon–Fri ─────────
echo "==> Creating Cloud Scheduler trigger"
gcloud scheduler jobs create http algostrategy-market-trigger \
  --schedule="44 3 * * 1-5" \
  --time-zone="UTC" \
  --uri="https://run.googleapis.com/v2/projects/$PROJECT_ID/locations/$REGION/jobs/algostrategy-pipeline:run" \
  --http-method=POST \
  --oauth-service-account-email="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --location=$REGION

echo ""
echo "✅ Deployment complete!"
echo "   Web service: https://algostrategy-web-xxxx-$REGION.run.app"
echo "   Pipeline Job: runs weekdays at 09:14 IST"