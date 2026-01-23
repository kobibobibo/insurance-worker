# Insurance Policy Worker

Background worker that processes insurance policy documents and extracts rights with evidence.

## Setup

### 1. Install Dependencies

```bash
npm install
```

### 2. Set Environment Variables

Create a `.env` file (for local testing):

```bash
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_SERVICE_ROLE_KEY=eyJhbGc...
UPSTASH_REDIS_REST_URL=https://xxxxx.upstash.io
UPSTASH_REDIS_REST_TOKEN=AYj9...
```

### 3. Run Locally

```bash
npm start
```

You should see:
```
🚀 Insurance Policy Worker Started
📋 Queue: policy-processing
⏱️  Poll Interval: 2000 ms
🔄 Max Retries: 3
──────────────────────────────────────────────────
```

## Deploy to Render.com

### 1. Push to GitHub

```bash
git init
git add .
git commit -m "Initial worker"
git remote add origin https://github.com/yourusername/insurance-worker.git
git push -u origin main
```

### 2. Create Background Worker on Render

1. Go to https://render.com
2. Click "New +" → "Background Worker"
3. Connect your GitHub repo
4. Fill in:
   - **Name:** insurance-policy-worker
   - **Language:** Node
   - **Branch:** main
   - **Region:** Frankfurt (EU Central)
   - **Build Command:** npm install
   - **Start Command:** node index.js
   - **Instance Type:** Starter ($7/month)

5. Add Environment Variables:
   - SUPABASE_URL
   - SUPABASE_SERVICE_ROLE_KEY
   - UPSTASH_REDIS_REST_URL
   - UPSTASH_REDIS_REST_TOKEN

6. Click "Deploy Background Worker"

### 3. Monitor

Watch logs in Render dashboard to see jobs being processed.

## How It Works

```
Frontend (Lovable) → API adds job to Redis queue
                     ↓
                   Worker polls Redis
                     ↓
                   Job found → Process pipeline
                     ↓
                   Update Supabase with results
                     ↓
                   Frontend shows results
```

## Pipeline Stages

1. **Intake:** Download PDFs, extract text
2. **Map:** Analyze document structure
3. **Harvest:** Find benefits with evidence
4. **Normalize:** Clean and standardize data
5. **Validate:** Ensure 100% evidence coverage
6. **Export:** Generate output files

## Notes

- Worker polls every 2 seconds
- Retries failed jobs up to 3 times
- Requires Supabase and Upstash Redis accounts
- Designed for Hebrew insurance policies
