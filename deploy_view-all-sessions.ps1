# =====================================================================
# deploy_view-all-sessions.ps1  -  ASCII only (compatible Windows PS 5.1)
# =====================================================================
# Script para que Daniel deploye SOLO el codigo de la sesion
# claude/view-all-sessions-O6JEs (transporte / chofer / tracking) a Cloud Run
# sin depender de otra sesion Claude.
#
# Uso desde Windows PowerShell:
#   cd C:\Users\DANIE\OneDrive\Escritorio\Claude\Etiquetas
#   .\deploy_view-all-sessions.ps1
#
# Si "no se puede ejecutar scripts" lanza error:
#   powershell -ExecutionPolicy Bypass -File .\deploy_view-all-sessions.ps1
#
# Requisitos:
#   1. gcloud SDK instalado
#   2. SA key en  C:\Users\DANIE\Downloads\ilus-app-498503-9fd9b2388e9d.json
#   3. env.yaml   C:\Users\DANIE\ilus-migracion\env.yaml
#
# Lo que hace:
#   1. Crea worktree limpio en .claude/worktrees/view-all-sessions-deploy
#      con la rama claude/view-all-sessions-O6JEs (la mia)
#   2. git pull (HEAD al dia)
#   3. Activa SA con la key
#   4. gcloud run deploy --source ese worktree con env.yaml
#   5. Smoke test post-deploy
# =====================================================================

$ErrorActionPreference = "Stop"
$env:CLOUDSDK_CORE_DISABLE_PROMPTS = "1"

# --- Configuracion ---------------------------------------------------
$REPO_ROOT  = "C:\Users\DANIE\OneDrive\Escritorio\Claude\Etiquetas"
$WORKTREE   = "$REPO_ROOT\.claude\worktrees\view-all-sessions-deploy"
$BRANCH     = "claude/view-all-sessions-O6JEs"
$KEY_FILE   = "C:\Users\DANIE\Downloads\ilus-app-498503-9fd9b2388e9d.json"
$ENV_FILE   = "C:\Users\DANIE\ilus-migracion\env.yaml"
$GCLOUD     = "C:\Users\DANIE\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"

$PROJECT    = "ilus-app-498503"
$REGION     = "southamerica-west1"
$SERVICE    = "ilus-app"
$CLOUDSQL   = "ilus-app-498503:southamerica-west1:ilus-db"
$BASE_URL   = "https://ilus-app-469212710544.southamerica-west1.run.app"

# --- 1) Pre-requisitos -----------------------------------------------
Write-Host "=== 1. PRE-REQUISITOS ===" -ForegroundColor Cyan
if (-not (Test-Path $KEY_FILE)) { Write-Host "[X] FALTA key: $KEY_FILE" -ForegroundColor Red; exit 1 }
if (-not (Test-Path $ENV_FILE)) { Write-Host "[X] FALTA env.yaml: $ENV_FILE" -ForegroundColor Red; exit 1 }
if (-not (Test-Path $GCLOUD))   { Write-Host "[X] FALTA gcloud: $GCLOUD" -ForegroundColor Red; exit 1 }
Write-Host "[OK] Key, env.yaml y gcloud" -ForegroundColor Green

# --- 2) Worktree limpio ----------------------------------------------
Write-Host "`n=== 2. WORKTREE LIMPIO ===" -ForegroundColor Cyan
Push-Location $REPO_ROOT
if (Test-Path $WORKTREE) {
    Write-Host "Worktree existe, actualizando..."
    Push-Location $WORKTREE
    git fetch origin
    git checkout $BRANCH
    git pull origin $BRANCH
    Pop-Location
} else {
    Write-Host "Creando worktree nuevo..."
    git fetch origin
    git worktree add $WORKTREE $BRANCH
}
Pop-Location

# Confirmar HEAD
Push-Location $WORKTREE
$COMMIT = (git rev-parse --short HEAD).Trim()
$COMMIT_MSG = (git log -1 --format="%s").Trim()
Write-Host "[OK] Worktree en HEAD: $COMMIT" -ForegroundColor Green
Write-Host "  ($COMMIT_MSG)"
Pop-Location

# --- 3) Confirmacion -------------------------------------------------
Write-Host "`n=== 3. CONFIRMACION ===" -ForegroundColor Yellow
Write-Host "Vas a deployar a Cloud Run:"
Write-Host "  Servicio: $SERVICE"
Write-Host "  Region:   $REGION"
Write-Host "  Fuente:   $WORKTREE"
Write-Host "  Commit:   $COMMIT - $COMMIT_MSG"
$confirm = Read-Host "`nContinuar? (s/N)"
if ($confirm -ne "s" -and $confirm -ne "S") { Write-Host "Cancelado."; exit 0 }

# --- 4) Auth ---------------------------------------------------------
Write-Host "`n=== 4. AUTENTICANDO ===" -ForegroundColor Cyan
& $GCLOUD auth activate-service-account --key-file=$KEY_FILE --project=$PROJECT

# --- 5) Deploy -------------------------------------------------------
Write-Host "`n=== 5. DEPLOY (3-4 min: build Chromium + push imagen) ===" -ForegroundColor Cyan
& $GCLOUD run deploy $SERVICE `
    --source $WORKTREE `
    --region $REGION `
    --project $PROJECT `
    --add-cloudsql-instances $CLOUDSQL `
    --allow-unauthenticated `
    --min-instances 1 --max-instances 4 `
    --memory 2Gi --cpu 2 --timeout 300 `
    --env-vars-file $ENV_FILE

if ($LASTEXITCODE -ne 0) {
    Write-Host "[X] Deploy fallo (exit $LASTEXITCODE)" -ForegroundColor Red
    exit $LASTEXITCODE
}

# --- 6) Smoke test ---------------------------------------------------
Write-Host "`n=== 6. SMOKE TEST ===" -ForegroundColor Cyan
Start-Sleep -Seconds 15  # cold start

$tests = @(
    @{ path = "/version"; name = "endpoint /version"; status = 200 }
    @{ path = "/login";   name = "login page";        status = 200 }
)

$fail = 0
foreach ($t in $tests) {
    try {
        $r = Invoke-WebRequest -Uri "$BASE_URL$($t.path)" -UseBasicParsing -TimeoutSec 30
        if ($r.StatusCode -eq $t.status) {
            Write-Host "[OK] $($t.name) -> $($r.StatusCode)" -ForegroundColor Green
            if ($t.path -eq "/version") {
                Write-Host "  -> $($r.Content.Substring(0, [Math]::Min(200, $r.Content.Length)))" -ForegroundColor Gray
            }
        } else {
            Write-Host "[X] $($t.name) -> $($r.StatusCode) (esperado $($t.status))" -ForegroundColor Red
            $fail++
        }
    } catch {
        Write-Host "[X] $($t.name) -> ERROR: $_" -ForegroundColor Red
        $fail++
    }
}

if ($fail -eq 0) {
    Write-Host "`n[OK] DEPLOY EXITOSO. $COMMIT esta en produccion." -ForegroundColor Green
    Write-Host "Abrir en incognito: $BASE_URL/transporte/couriers" -ForegroundColor Cyan
} else {
    Write-Host "`n[WARN] Deploy termino pero $fail smoke test(s) fallaron." -ForegroundColor Yellow
    Write-Host "Revisar: gcloud run services describe $SERVICE --region=$REGION" -ForegroundColor Yellow
}
