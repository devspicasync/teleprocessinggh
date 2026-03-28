# Deployment & API Guide

This guide explains how to push your code to GitHub and deploy the API so you can access the Swagger UI.

## 1. Push to GitHub

1. **Initialize Git** (if not already done):
   ```bash
   git init
   git add .
   git commit -m "Initial commit: Joined PDF/CSV API with Docker support"
   ```

2. **Create a Repository** on GitHub (e.g., `telecom-anomaly-detection`).

3. **Link and Push**:
   ```bash
   git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
   git branch -M main
   git push -u origin main
   ```

---

## 2. Deploying to Vercel (Serverless)

Vercel is primarily for frontend, but you can run the FastAPI backend using their Python runtime.

1. **Install Vercel CLI**: `npm i -g vercel`
2. **Login**: `vercel login`
3. **Deploy**: Run `vercel` in the project root.
4. **Access Swagger**: Once deployed, go to `https://your-project-name.vercel.app/docs`.

**⚠️ Note:** If the PDF processing fails on Vercel due to "Java not found", you MUST use the Docker method below.

---

## 3. Recommended: Deploying with Docker (Railway / Render)

Since your project requires **Java** (for `tabula-py`), using a platform that supports your `Dockerfile` is much more reliable.

### Option A: Railway.app (Easiest)
1. Go to [Railway.app](https://railway.app/) and login with GitHub.
2. Click **New Project** -> **Deploy from GitHub repo**.
3. Select your repository.
4. Railway will see your `Dockerfile` and deploy it automatically.
5. Your Swagger UI will be at: `https://your-railway-url.up.railway.app/docs`.

### Option B: Render.com
1. Go to [Render.com](https://render.com/).
2. Click **New** -> **Web Service**.
3. Connect your GitHub and select the repo.
4. Select **Environment: Docker**.
5. Click **Deploy**.
6. Your Swagger UI will be at: `https://your-subdomain.onrender.com/docs`.

---

## 4. Accessing Swagger UI Locally

To test everything locally before pushing:

1. **Run with Uvicorn**:
   ```bash
   uvicorn telecom_anomaly.api:app --reload
   ```
2. **Open Browser**:
   - **Interactive Documentation (Swagger)**: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
   - **Alternative Docs (Redoc)**: [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

## 5. Using the Unified Endpoint

- **Endpoint**: `POST /analyze`
- **Function**: Upload a PDF (or CSV).
- **Parameters**: 
    - `save_results`: Set to `true` if you want to save files on the server (Docker only).
    - `filter_movements`: Set to `true` to clean up geographic data.
    - All other settings match your original logic.
