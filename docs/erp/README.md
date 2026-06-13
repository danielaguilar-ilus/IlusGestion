# Diccionarios del ERP Random — referencia para TODAS las sesiones

> **Lee esto antes de escribir cualquier consulta nueva al ERP Random.**
> El ERP es **READ-ONLY ABSOLUTO** (solo `SELECT` vía `_random_sql_query` o `erp_engine.fetch_*`).

## Archivos

| Archivo | Qué contiene |
|---|---|
| `Diccionario-Campos-Random-ERP.pdf` | Campos por tabla principal (productos, entidades, encabezado y detalle de documentos). Texto extraíble con `pdfplumber`/`pypdf`. |
| `TABLAS-BD-Random.pdf` | Estructura de MAEEDO / MAEDDO / MAEEN / MAEEDOOB y otras. |
| `Diccionario-Tablas-Sistema-1110.xls` | El más completo: 3 hojas — **244 tablas**, **600 campos con su significado**, y detalle de campos por tabla. Leer con `xlrd`. |

## Cheat-sheet (tablas que más se usan)

- **MAEEDO** (cabecera doc): `IDMAEEDO`, `TIDO` (BLV/GDV/FCV/NVV…), `NUDO`, `ENDO` (entidad → **es el `KOEN`, NO el RUT**), `FEEMDO`, `FEER`, `ESDO` ('NULO'=anulado), `VANEDO`/`VABRDO`.
- **MAEDDO** (líneas): `IDMAEEDO`, `KOPRCT` (SKU), `CAPRCO1` (cant), `CAPRAD1` (despachada), `PPPRNE` (precio unit), **`VANELI` (valor neto de línea = montos, ej. envío)**, `NOKOPR`.
- **MAEEN** (clientes): `KOEN` (código ← matchea `MAEEDO.ENDO`), `RTEN` (RUT, a veces con DV), `NOKOEN`/`NOKOENAMP` (nombre), `CMEN` (cód comuna), `CIEN` (cód ciudad), `SUEN` (sucursal — KOEN no es único).
- **MAEEDOOB** (obs): `OBDO`, `DIENDESP`, `TEXTO1..15`.
- **TABCM** (comunas): clave **(`KOCM`, `KOCI`)** → `NOKOCM`. **TABCI** = ciudades.
- **SKUs ZZ**: `ZZENVIO` (flete), `ZZRETIRO`, `ZZINSTALACION`, `ZZSERVTEC`, `ZZINGREPUESTO`, `ZZINGARREQUIP`.

## Gotchas críticos

1. **`ENDO = KOEN`, no el RUT.** Joinear MAEEN por `KOEN = ENDO` (no por RTEN). Es la causa #1 de "Consumidor Final" / comuna vacía en el monitor.
2. Campos `char` traen espacios a la derecha → `LTRIM(RTRIM())` para mostrar; en `=` los `char` ignoran espacios finales (no envolver la columna indexada, mata el índice).
3. Diagnóstico local read-only: `pymssql` + credenciales de `env.yaml` (`RANDOM_SQL_*`). Nunca hardcodear ni imprimir credenciales.
