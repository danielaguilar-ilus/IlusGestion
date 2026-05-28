# tools/archive — Scripts históricos sin uso en producción

Esta carpeta contiene **scripts one-shot administrativos y de migración** que en algún momento se ejecutaron manualmente para resolver situaciones puntuales. Ya no son parte del flujo activo de la aplicación.

## Razón de existencia

Antes del 2026-05-28 estos scripts vivían en la raíz del proyecto, mezclados con `app.py`, `pickups_module.py`, etc. Eso:

1. Confundía a quien abría el repo por primera vez.
2. Algunos tenían `--apply` como única protección — riesgo de ejecutarlos por accidente.
3. Pesaban en el listado del directorio raíz sin pertenecer al runtime.

Movidos como parte de la limpieza de código muerto documentada en el plan
`C:\Users\DANIE\.claude\plans\oye-mira-sabes-que-generic-ullman.md` (Fase 5).

## Inventario

| Script | Propósito original | Cuándo se usó |
|---|---|---|
| `_admin_borrar_ots.py` | Borrado masivo de OTs (`mant_visitas`) + sus hijas. Deslinda levantamientos (`visita_id=NULL`). Requiere `--apply` para ejecutar (dry-run default). | 22-may-2026 (autorizado por Daniel) |
| `_admin_promover_ot_pendientes.py` | Promover OTs aprobadas que no se promovieron por bug del deploy. | 22-may-2026 |
| `_admin_backfill_created_by_user_id.py` | Backfill `mant_visitas.created_by_user_id` desde `created_by` (string). Caso Aaron Urbina `ejecutivo_sstt`. | 22-may-2026 |
| `_apply_courier_audit_migrations.py` | Aplica migraciones `courier_tariff_audit` + seed Lo Barnechea. | 22-may-2026 |
| `test_fcv10644_couriers.py` | Test ad-hoc validación FCV 10644 Lo Barnechea (173.89 kg) vs Excel maestro. | 22-may-2026 |
| `_debug_500.py` | Reproducción local del 500 del tracking público retiro #7. Debug puntual. | 23-may-2026 |
| `DROPIT_AUTOCOMPLETE_PROMPT.md` | Prompt para feature Dropbox autocomplete (no implementado). | 23-may-2026 |

## Cómo recuperar uno

```bash
git mv tools/archive/<script>.py ./
```

Si ya no figura en `git log`, está en el tag `safepoint-deadcode-cleanup-2026-05-28`.

## NO ejecutar sin contexto

Los scripts `_admin_*` modifican datos en producción. **Nunca ejecutar `--apply` sin antes correr el dry-run y revisar el output.**
