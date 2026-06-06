# ═══════════════════════════════════════════════════════════════════════
# smoke_test_prod.ps1
# ═══════════════════════════════════════════════════════════════════════
# Verifica si producción tiene el código de transporte/chofer/tracking.
# Útil después de cualquier deploy (mío o de la otra sesión) para confirmar
# que mi rediseño/fix/sistema sigue en pie.
#
# Uso:
#   .\smoke_test_prod.ps1
#
# Sin gcloud, sin credenciales. Solo HTTP.
# ═══════════════════════════════════════════════════════════════════════

$BASE = "https://ilus-app-469212710544.southamerica-west1.run.app"
$ok = 0; $fail = 0

function Test-Endpoint($path, $description, $expectStatus, $mustContain = $null) {
    global:ok = $global:ok
    try {
        $r = Invoke-WebRequest -Uri "$BASE$path" -UseBasicParsing -TimeoutSec 25 `
                               -MaximumRedirection 0 -ErrorAction Stop
        $st = $r.StatusCode
        $body = $r.Content
    } catch {
        # Si fue redirect (302), capturar status
        if ($_.Exception.Response) {
            $st = [int]$_.Exception.Response.StatusCode
            $body = ""
        } else {
            Write-Host "✗ $description → conexión falló: $_" -ForegroundColor Red
            $script:fail++; return
        }
    }
    if ($st -eq $expectStatus) {
        if ($mustContain -and ($body -notmatch $mustContain)) {
            Write-Host "✗ $description → status OK ($st) pero NO contiene '$mustContain'" -ForegroundColor Red
            $script:fail++
        } else {
            Write-Host "✓ $description → $st" -ForegroundColor Green
            $script:ok++
        }
    } else {
        Write-Host "✗ $description → $st (esperado $expectStatus)" -ForegroundColor Red
        $script:fail++
    }
}

Write-Host "═══ SMOKE TEST PRODUCCIÓN ═══" -ForegroundColor Cyan
Write-Host "URL: $BASE`n"

# ── A) Endpoints PÚBLICOS (no requieren login) ────────────────────────
Write-Host "── Endpoints públicos:" -ForegroundColor Yellow

# /version: debe existir (lo agregué yo) y devolver JSON con commit
Test-Endpoint "/version" "/version endpoint mío" 200 "commit"

# Login: siempre debe responder 200
Test-Endpoint "/login" "/login (sesión pública)" 200 $null

# /seguimiento: módulo público del cliente (mío)
Test-Endpoint "/seguimiento" "/seguimiento (módulo público mío)" 200 "seguimiento"

# /chofer/login: app del chofer (mía)
Test-Endpoint "/chofer/login" "/chofer/login (app móvil mía)" 200 "PIN"

# ── B) /version detalle ───────────────────────────────────────────────
Write-Host "`n── Versión exacta corriendo:" -ForegroundColor Yellow
try {
    $v = Invoke-RestMethod -Uri "$BASE/version" -TimeoutSec 15
    Write-Host "  Commit:   $($v.commit)" -ForegroundColor Cyan
    Write-Host "  Mensaje:  $($v.msg)" -ForegroundColor Gray
    Write-Host "  Revisión: $($v.revision)" -ForegroundColor Gray
    # Estos commits son míos
    $expectedMine = @("9d356e0", "66ee5b2", "222d716", "0bd3c57", "f8a250a")
    $hasMine = $false
    foreach ($e in $expectedMine) {
        if ($v.commit -like "$e*") { $hasMine = $true; break }
    }
    if ($hasMine) {
        Write-Host "  ✓ Es uno de MIS commits — mi código está vivo" -ForegroundColor Green
    } else {
        Write-Host "  ⚠️  No reconozco el commit. Puede ser de la otra sesión + merge." -ForegroundColor Yellow
    }
} catch {
    Write-Host "  ✗ No se pudo leer /version — quizá el deploy aún no incluye este endpoint" -ForegroundColor Red
}

# ── C) Reporte ────────────────────────────────────────────────────────
Write-Host "`n═══ RESULTADO ═══" -ForegroundColor Cyan
Write-Host "✓ $ok OK"     -ForegroundColor Green
Write-Host "✗ $fail FAIL" -ForegroundColor $(if ($fail -gt 0) { "Red" } else { "Gray" })

if ($fail -eq 0) {
    Write-Host "`n✅ Producción tiene el código de transporte/chofer/tracking." -ForegroundColor Green
} else {
    Write-Host "`n⚠️  Hay endpoints fallando. Mi código quizá NO entró en el último deploy." -ForegroundColor Yellow
}
