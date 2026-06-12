# =====================================================================
# smoke_test_prod.ps1  -  ASCII only (compatible Windows PS 5.1)
# =====================================================================
# Verifica si produccion tiene el codigo de transporte/chofer/tracking.
# Util despues de cualquier deploy (mio o de la otra sesion) para confirmar
# que mi rediseno/fix/sistema sigue en pie.
#
# Uso:
#   .\smoke_test_prod.ps1
#
# Si "no se puede ejecutar scripts":
#   powershell -ExecutionPolicy Bypass -File .\smoke_test_prod.ps1
#
# Sin gcloud, sin credenciales. Solo HTTP.
# =====================================================================

$BASE = "https://ilus-app-469212710544.southamerica-west1.run.app"
$script:ok = 0
$script:fail = 0

function Test-Endpoint($path, $description, $expectStatus, $mustContain = $null) {
    try {
        $r = Invoke-WebRequest -Uri "$BASE$path" -UseBasicParsing -TimeoutSec 25 `
                               -MaximumRedirection 0 -ErrorAction Stop
        $st = $r.StatusCode
        $body = $r.Content
    } catch {
        if ($_.Exception.Response) {
            $st = [int]$_.Exception.Response.StatusCode
            $body = ""
        } else {
            Write-Host "[X] $description -> conexion fallo: $_" -ForegroundColor Red
            $script:fail++; return
        }
    }
    if ($st -eq $expectStatus) {
        if ($mustContain -and ($body -notmatch $mustContain)) {
            Write-Host "[X] $description -> status OK ($st) pero NO contiene '$mustContain'" -ForegroundColor Red
            $script:fail++
        } else {
            Write-Host "[OK] $description -> $st" -ForegroundColor Green
            $script:ok++
        }
    } else {
        Write-Host "[X] $description -> $st (esperado $expectStatus)" -ForegroundColor Red
        $script:fail++
    }
}

Write-Host "=== SMOKE TEST PRODUCCION ===" -ForegroundColor Cyan
Write-Host "URL: $BASE`n"

# --- A) Endpoints publicos -------------------------------------------
Write-Host "-- Endpoints publicos:" -ForegroundColor Yellow

Test-Endpoint "/version" "/version endpoint" 200 "commit"
Test-Endpoint "/login" "/login (publica)" 200 $null
Test-Endpoint "/seguimiento" "/seguimiento" 200 "seguimiento"
Test-Endpoint "/chofer/login" "/chofer/login" 200 "PIN"

# --- B) /version detalle ---------------------------------------------
Write-Host "`n-- Version exacta corriendo:" -ForegroundColor Yellow
try {
    $v = Invoke-RestMethod -Uri "$BASE/version" -TimeoutSec 15
    Write-Host "  Commit:   $($v.commit)" -ForegroundColor Cyan
    Write-Host "  Mensaje:  $($v.msg)" -ForegroundColor Gray
    Write-Host "  Revision: $($v.revision)" -ForegroundColor Gray
    # Commits mios conocidos (mas recientes arriba)
    $expectedMine = @("01863d1", "87e51c0", "c805a38", "9d356e0", "66ee5b2", "222d716", "0bd3c57", "f8a250a")
    $hasMine = $false
    foreach ($e in $expectedMine) {
        if ($v.commit -like "$e*") { $hasMine = $true; break }
    }
    if ($hasMine) {
        Write-Host "  [OK] Es uno de MIS commits - mi codigo esta vivo" -ForegroundColor Green
    } else {
        Write-Host "  [?] No reconozco el commit. Puede ser de la otra sesion + merge." -ForegroundColor Yellow
    }
} catch {
    Write-Host "  [X] No se pudo leer /version - quiza el deploy aun no incluye este endpoint" -ForegroundColor Red
}

# --- C) Reporte ------------------------------------------------------
Write-Host "`n=== RESULTADO ===" -ForegroundColor Cyan
Write-Host "[OK]   $($script:ok)"   -ForegroundColor Green
Write-Host "[FAIL] $($script:fail)" -ForegroundColor $(if ($script:fail -gt 0) { "Red" } else { "Gray" })

if ($script:fail -eq 0) {
    Write-Host "`n[OK] Produccion tiene el codigo de transporte/chofer/tracking." -ForegroundColor Green
} else {
    Write-Host "`n[WARN] Hay endpoints fallando. Mi codigo quiza NO entro en el ultimo deploy." -ForegroundColor Yellow
}
