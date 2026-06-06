# ═══════════════════════════════════════════════════════════════════════
# deploy_view-all-sessions.ps1
# ═══════════════════════════════════════════════════════════════════════
# Script para que Daniel deploye SOLO el código de la sesión
# claude/view-all-sessions-O6JEs (transporte / chofer / tracking) a Cloud Run
# sin depender de otra sesión Claude.
#
# Uso desde Windows PowerShell:
#   cd C:\Users\DANIE\OneDrive\Escritorio\Claude\Etiquetas
#   .\deploy_view-all-sessions.ps1
#
# Requisitos:
#   1. gcloud SDK instalado
#   2. Key del service account en C:\Users\DANIE\Downloads\ilus-app-498503-9fd9b2388e9d.json
#   3. env.yaml en C:\Users\DANIE\ilus-migracion\env.yaml
#
# Lo que hace:
#   1. Crea un worktree limpio en .claude/worktrees/view-all-sessions-deploy
#      con la rama claude/view-all-sessions-O6JEs (la mía)
#   2. Hace git pull origin claude/view-all-sessions-O6JEs (asegura HEAD al día)
#   3. Activa la SA con la key
#   4. gcloud run deploy --source ese worktree con env.yaml
#   5. Smoke test post-deploy
#
# ═══════════════════════════════════════════════════════════════════════

$ErrorActionPreference = "Stop"
$env:CLOUDSDK_CORE_DISABLE_PROMPTS = "1"

# ── Configuración ─────────────────────────────────────────────────────
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

# ── 1) Verificar pre-requisitos ───────────────────────────────────────
Write-Host "═══ 1. PRE-REQUISITOS ═══" -ForegroundColor Cyan
if (-not (Test-Path $KEY_FILE)) { Write-Host "✗ FALTA key: $KEY_FILE" -ForegroundColor Red; exit 1 }
if (-not (Test-Path $ENV_FILE)) { Write-Host "✗ FALTA env.yaml: $ENV_FILE" -ForegroundColor Red; exit 1 }
if (-not (Test-Path $GCLOUD))   { Write-Host "✗ FALTA gcloud: $GCLOUD" -ForegroundColor Red; exit 1 }
Write-Host "✓ Key, env.yaml y gcloud OK" -ForegroundColor Green

# ── 2) Worktree limpio con la rama de transporte ──────────────────────
Write-Host "`n═══ 2. WORKTREE LIMPIO ═══" -ForegroundColor Cyan
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
Write-Host "✓ Worktree en HEAD: $COMMIT" -ForegroundColor Green
Write-Host "  ($COMMIT_MSG)"
Pop-Location

# ── 3) Confirmación de Daniel ─────────────────────────────────────────
Write-Host "`n═══ 3. CONFIRMACIÓN ═══" -ForegroundColor Yellow
Write-Host "Vas a deployar a Cloud Run:"
Write-Host "  Servicio: $SERVICE"
Write-Host "  Región:   $REGION"
Write-Host "  Fuente:   $WORKTREE"
Write-Host "  Commit:   $COMMIT — $COMMIT_MSG"
$confirm = Read-Host "`n¿Continuar? (s/N)"
if ($confirm -ne "s" -and $confirm -ne "S") { Write-Host "Cancelado."; exit 0 }

# ── 4) Auth + deploy ──────────────────────────────────────────────────
Write-Host "`n═══ 4. AUTENTICANDO ═══" -ForegroundColor Cyan
& $GCLOUD auth activate-service-account --key-file=$KEY_FILE --project=$PROJECT

Write-Host "`n═══ 5. DEPLOY ═══" -ForegroundColor Cyan
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
    Write-Host "✗ Deploy falló (exit $LASTEXITCODE)" -ForegroundColor Red
    exit $LASTEXITCODE
}

# ── 6) Smoke test ─────────────────────────────────────────────────────
Write-Host "`n═══ 6. SMOKE TEST ═══" -ForegroundColor Cyan
Start-Sleep -Seconds 15  # cold start

$tests = @(
    @{ path = "/version";             name = "endpoint /version";    status = 200 }
    @{ path = "/login";               name = "login page";           status = 200 }
)

$fail = 0
foreach ($t in $tests) {
    try {
        $r = Invoke-WebRequest -Uri "$BASE_URL$($t.path)" -UseBasicParsing -TimeoutSec 30
        if ($r.StatusCode -eq $t.status) {
            Write-Host "✓ $($t.name) → $($r.StatusCode)" -ForegroundColor Green
            if ($t.path -eq "/version") {
                Write-Host "  → $($r.Content.Substring(0, [Math]::Min(200, $r.Content.Length)))" -ForegroundColor Gray
            }
        } else {
            Write-Host "✗ $($t.name) → $($r.StatusCode) (esperado $($t.status))" -ForegroundColor Red
            $fail++
        }
    } catch {
        Write-Host "✗ $($t.name) → ERROR: $_" -ForegroundColor Red
        $fail++
    }
}

if ($fail -eq 0) {
    Write-Host "`n✅ DEPLOY EXITOSO. $COMMIT está en producción." -ForegroundColor Green
    Write-Host "Abrí en incógnito: $BASE_URL/transporte/couriers" -ForegroundColor Cyan
} else {
    Write-Host "`n⚠️  Deploy terminó pero $fail smoke test(s) fallaron." -ForegroundColor Yellow
    Write-Host "Revisá: gcloud run services describe $SERVICE --region=$REGION" -ForegroundColor Yellow
}
