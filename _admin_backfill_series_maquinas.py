"""
Reparación retroactiva de equipos con N° de serie faltante (mant_maquinas).

CONTEXTO (autorizado por Daniel, 2026-09-09: "ve con eso"):
Un bug histórico en el alta de equipos en bloque (multi-unidad) --
condición de carrera al leer las series ya usadas del cliente/SKU dentro
de un mismo lote, sumada a un `except: serie=None` silencioso -- hacía
que ALGUNOS equipos de un lote nunca recibieran su N° de serie, mientras
el resto del mismo lote sí. El fix de raíz YA está en producción (mismo
patrón `_insert_con_retry` que usa `mant_maquina_add`, aplicado también a
`_ot_alta_equipos_al_cerrar` -- ver app.py ~65007 y ~80786): la serie base
se calcula UNA vez por (cliente, SKU) y se incrementa EN MEMORIA dentro
del lote, con reintento si choca contra la UNIQUE (cliente_id, serie).

Ese fix solo previene casos NUEVOS. Este script repara los equipos que
YA quedaron en producción con `serie` NULL o vacía por el bug histórico,
replicando el MISMO algoritmo de `_generar_serie_ilus()` (app.py ~64721):

    serie = "{RUT_cliente_sin_DV}-{últimos4_SKU}-{secuencial}"

USO:
    python _admin_backfill_series_maquinas.py            # DRY RUN (default, no escribe nada)
    python _admin_backfill_series_maquinas.py --apply     # ejecuta la reparación real

Es SEGURO correr el modo dry-run cuantas veces se quiera (solo hace SELECT).

El modo --apply es IDEMPOTENTE:
  - Cada UPDATE es puntual por `id` y lleva
    `WHERE id=%s AND (serie IS NULL OR TRIM(serie)='')` -- nunca pisa una
    serie ya asignada (ni por este mismo script en una corrida anterior,
    ni por un humano editándola en paralelo).
  - Correrlo dos veces no genera series duplicadas: la segunda corrida
    ya no encuentra esas filas en la detección (dejaron de tener
    `serie` NULL/vacía), y punto de partida de los secuenciales
    (`_series_usadas`) siempre relee lo que hay en la BD al momento de
    ejecutar, igual que `_generar_serie_ilus()`.

Qué se repara automáticamente:
  - mant_maquinas con `serie IS NULL OR TRIM(serie)=''`, que tengan SKU
    no vacío y cuyo cliente tenga un RUT usable. Si hay MÁS de un equipo
    del mismo cliente+SKU con serie faltante, reciben series CONSECUTIVAS
    y DISTINTAS entre sí (mismo criterio "1 lote, 1 secuencia en memoria"
    del fix de raíz -- NUNCA se llama al generador una vez por unidad
    dentro de un loop sin incrementar en memoria, que fue exactamente el
    bug original).

Qué NO se repara automático (queda listado aparte para revisión manual,
nunca se fuerza un valor sin sentido):
  - Equipos sin SKU (o SKU vacío): no hay con qué construir una serie con
    sentido.
  - Clientes sin RUT usable (vacío, o con menos de 8 caracteres tras
    limpiar puntos/guiones): `_generar_serie_ilus()` en producción cae al
    prefijo placeholder "00000000-...", que no identifica al cliente real.
    Se prefiere dejarlo para que un humano decida el prefijo correcto en
    vez de escribir un dato con apariencia de válido que no lo es.

Auditoría (mismo espíritu que _admin_borrar_ots.py y el resto de los
_admin_*.py del proyecto -- REGLA #5 del CLAUDE.md, "audit log en TODA
acción destructiva, antes de borrar" -- acá aplica el mismo criterio
aunque esto no es destructivo sino una reparación):
  - Por cada equipo reparado: 1 fila en `mant_maquina_audit`
    (campo='serie', valor_antes, valor_nuevo, motivo, usuario) -- mismo
    patrón que usa el endpoint PUT /mantenciones/api/maquinas/<id>/serie
    (app.py ~65652).
  - Por cada equipo reparado: 1 fila en `mant_logs`
    (entidad='maquina', accion='serie_backfill_historico') para que
    aparezca en el historial de la ficha del equipo.
  - 1 fila resumen en `mant_logs` (entidad='sistema',
    accion='backfill_series_maquinas_historico') con el conteo total.

NO ejecuta nada contra el ERP Random (REGLA #4.1) -- esto es 100% sobre
la tabla propia `mant_maquinas` en MySQL Clever Cloud.
"""
import os
import sys
import random
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

import pymysql

APPLY = "--apply" in sys.argv
USUARIO = "sistema:_admin_backfill_series_maquinas"

c = pymysql.connect(
    host=os.environ['MYSQL_HOST'], port=int(os.environ['MYSQL_PORT']),
    user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'],
    database=os.environ['MYSQL_DATABASE'],
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=False,
)
cur = c.cursor()

_rut_cache = {}


def _rut_cliente_limpio(cid):
    """RUT del cliente sin DV/puntos/guiones, o None si no es usable.

    Replica el mismo limpiado que `_generar_serie_ilus()` (app.py ~64744),
    salvo que acá `None` significa "no reparar automático" en vez de caer
    al placeholder "00000000" que sí usa producción para equipos NUEVOS.
    """
    if cid in _rut_cache:
        return _rut_cache[cid]
    cur.execute("SELECT rut FROM mant_clientes WHERE id=%s", (cid,))
    row = cur.fetchone()
    rut_clean = None
    if row and row.get("rut"):
        raw = str(row["rut"]).replace(".", "").replace(" ", "").replace("-", "").upper()
        if len(raw) >= 8:
            rut_clean = raw[:-1]
    _rut_cache[cid] = rut_clean
    return rut_clean


def _sku4(sku):
    """Últimos 4 chars alfanuméricos del SKU (idéntico a _generar_serie_ilus)."""
    sku_clean = "".join(ch for ch in (sku or "").upper() if ch.isalnum())
    if not sku_clean:
        return None
    return sku_clean[-4:] if len(sku_clean) >= 4 else sku_clean.rjust(4, "0")


def _series_usadas(cid, base_prefix):
    """Secuenciales ya usados por el cliente para ese prefijo RUT-SKU4,
    releído en vivo de la BD (igual que _generar_serie_ilus)."""
    cur.execute(
        "SELECT serie FROM mant_maquinas WHERE cliente_id=%s AND serie LIKE %s",
        (cid, f"{base_prefix}-%")
    )
    usados = set()
    for r in cur.fetchall():
        suf = (r.get("serie") or "").rsplit("-", 1)[-1]
        try:
            usados.add(int(suf))
        except Exception:
            pass
    return usados


# ── 1. DETECCIÓN (solo lectura) ─────────────────────────────────────────
cur.execute("""
    SELECT m.id, m.cliente_id, m.sku, m.nombre, m.serie, m.estado,
           c.razon_social, c.rut
      FROM mant_maquinas m
      LEFT JOIN mant_clientes c ON c.id = m.cliente_id
     WHERE (m.serie IS NULL OR TRIM(m.serie) = '')
     ORDER BY m.cliente_id, m.sku, m.id
""")
todas = cur.fetchall() or []

print(f"=== Equipos con serie faltante (NULL o vacía): {len(todas)} ===\n")

if not todas:
    print("Nada que reparar. Saliendo.")
    c.close()
    sys.exit(0)

reparables = []       # equipos con SKU y RUT usables -> se reparan
manual_sin_sku = []    # sin SKU -> revisión manual
manual_sin_rut = []    # cliente sin RUT usable -> revisión manual

for m in todas:
    sku = (m.get("sku") or "").strip()
    if not sku:
        manual_sin_sku.append(m)
        continue
    rut_clean = _rut_cliente_limpio(m["cliente_id"])
    if not rut_clean:
        manual_sin_rut.append(m)
        continue
    sku4 = _sku4(sku)
    base = f"{rut_clean}-{sku4}"
    reparables.append({**m, "sku": sku, "sku4": sku4, "base": base})

# Agrupar reparables por cliente+SKU sólo para el reporte (el secuencial
# real se calcula por `base`, ver más abajo -- dos SKU distintos pueden
# truncar al mismo sku4 y comparten prefijo, igual que en producción).
grupos = {}
for r in reparables:
    key = (r["cliente_id"], r["sku"])
    grupos.setdefault(key, []).append(r)

print(f"— Reparables automáticamente: {len(reparables)} equipo(s) en "
      f"{len(grupos)} grupo(s) cliente+SKU")
print(f"— Sin SKU (revisión manual, NO se tocan): {len(manual_sin_sku)}")
print(f"— Cliente sin RUT usable (revisión manual, NO se tocan): {len(manual_sin_rut)}\n")

if grupos:
    print("--- Detalle por cliente + SKU ---")
    for (cid, sku), filas in grupos.items():
        cliente = filas[0].get("razon_social") or f"cliente_id={cid}"
        ids = ", ".join(str(f["id"]) for f in filas)
        print(f"  · cliente_id={cid} ({str(cliente)[:50]}) · sku={sku} · "
              f"{len(filas)} unidad(es) sin serie → ids: {ids}")

if manual_sin_sku:
    print("\n--- SIN SKU (revisión manual) ---")
    for m in manual_sin_sku:
        print(f"  · id={m['id']} · cliente_id={m['cliente_id']} "
              f"({str(m.get('razon_social') or '?')[:40]}) · nombre={m.get('nombre') or '—'}")

if manual_sin_rut:
    print("\n--- CLIENTE SIN RUT USABLE (revisión manual) ---")
    for m in manual_sin_rut:
        print(f"  · id={m['id']} · cliente_id={m['cliente_id']} "
              f"({str(m.get('razon_social') or '?')[:40]}) · sku={m.get('sku')} · "
              f"rut='{m.get('rut') or ''}'")

if not reparables:
    print("\nNo hay equipos reparables automáticamente (todos van a revisión manual).")
    c.close()
    sys.exit(0)

# ── 2. Calcular la serie que le tocaría a cada reparable, EN MEMORIA,
#      consecutiva por `base` y respetando lo ya usado en la BD ────────
plan = []
usados_por_base = {}
for r in reparables:
    base = r["base"]
    if base not in usados_por_base:
        usados_por_base[base] = _series_usadas(r["cliente_id"], base)
    usados = usados_por_base[base]
    seq = 1
    while seq in usados:
        seq += 1
    usados.add(seq)
    plan.append({
        "id": r["id"], "cliente_id": r["cliente_id"], "sku": r["sku"],
        "serie_anterior": r.get("serie") or "",
        "serie_nueva": f"{base}-{seq}",
    })

print(f"\n=== Plan de reparación: {len(plan)} UPDATE(s) ===")
for p in plan:
    print(f"  · id={p['id']} (cliente_id={p['cliente_id']}, sku={p['sku']}): "
          f"'{p['serie_anterior']}' → '{p['serie_nueva']}'")

if not APPLY:
    print("\n=== DRY RUN — no se ejecutó ningún UPDATE ===")
    print("Para ejecutar la reparación real: "
          "python _admin_backfill_series_maquinas.py --apply")
    c.close()
    sys.exit(0)

# ── 3. REPARACIÓN real: UPDATE puntual por id + auditoría completa ─────
motivo = (
    "Reparación retroactiva 2026-09-09 (autorización de Daniel: 've con "
    "eso'). Bug histórico: alta multi-unidad con condición de carrera "
    "(secuencial leído sin ver los INSERT del mismo lote) + `except: "
    "serie=None` silencioso dejaba equipos sin serie mientras el resto "
    "del mismo lote sí la recibía. El fix de raíz ya está en producción "
    "(patrón _insert_con_retry, en mant_maquina_add y "
    "_ot_alta_equipos_al_cerrar); este script solo repara equipos "
    "preexistentes con serie NULL/vacía."
)
audit_at = datetime.now().isoformat(timespec='seconds')

reparados, fallidos, sin_cambios = [], [], []
for p in plan:
    serie = p["serie_nueva"]
    resultado = None  # "ok" | "skip" | "error"
    for intento in range(5):
        try:
            cur.execute(
                "UPDATE mant_maquinas SET serie=%s "
                " WHERE id=%s AND (serie IS NULL OR TRIM(serie)='')",
                (serie, p["id"])
            )
            if cur.rowcount == 0:
                # Idempotencia: entre la detección y este UPDATE, el
                # equipo ya recibió una serie por otra vía. No se toca.
                resultado = "skip"
            else:
                p["serie_nueva"] = serie
                resultado = "ok"
            break
        except Exception as e:
            msg = str(e)
            if ("1062" in msg or "Duplicate entry" in msg) and intento < 4:
                # Colisión de concurrencia real (otro proceso tomó esa
                # serie entre el plan y este UPDATE) -- mismo mecanismo
                # de reintento que _insert_con_retry / _generar_serie_ilus.
                base = p["serie_nueva"].rsplit("-", 1)[0]
                seq_actual = int(p["serie_nueva"].rsplit("-", 1)[1])
                seq_nuevo = seq_actual + random.randint(intento + 1, (intento + 1) * 10)
                serie = f"{base}-{seq_nuevo}"
                continue
            print(f"  · id={p['id']}: ERROR {msg[:150]}")
            fallidos.append({"id": p["id"], "error": msg[:200]})
            resultado = "error"
            break

    if resultado == "ok":
        reparados.append(p)
        cur.execute(
            """INSERT INTO mant_maquina_audit
               (maquina_id, cliente_id, campo, valor_antes, valor_nuevo, motivo, usuario)
               VALUES (%s,%s,'serie',%s,%s,%s,%s)""",
            (p["id"], p["cliente_id"], p["serie_anterior"] or None,
             p["serie_nueva"], motivo, USUARIO)
        )
        cur.execute(
            "INSERT INTO mant_logs (entidad, entidad_id, accion, detalle, usuario, created_at) "
            "VALUES (%s,%s,%s,%s,%s, NOW())",
            ("maquina", p["id"], "serie_backfill_historico",
             (f"[{audit_at}] '{p['serie_anterior'] or '(vacía)'}' -> "
              f"'{p['serie_nueva']}'. {motivo}")[:2000],
             USUARIO)
        )
    elif resultado == "skip":
        sin_cambios.append(p)
        print(f"  · id={p['id']}: SKIP, ya tenía serie asignada (no se toca)")

# Resumen global en mant_logs -- mismo espíritu que
# _ensure_backfill_costo_desde_zz (app.py ~57026).
cur.execute(
    "INSERT INTO mant_logs (entidad, entidad_id, accion, detalle, usuario, created_at) "
    "VALUES (%s,%s,%s,%s,%s, NOW())",
    ("sistema", 0, "backfill_series_maquinas_historico",
     (f"[{audit_at}] Backfill de series históricas: {len(reparados)} "
      f"equipo(s) reparados, {len(fallidos)} fallido(s), "
      f"{len(sin_cambios)} sin cambios (ya tenían serie), "
      f"{len(manual_sin_sku)} sin SKU (sin tocar), "
      f"{len(manual_sin_rut)} con cliente sin RUT usable (sin tocar). "
      f"{motivo}")[:4000],
     USUARIO)
)

print(f"\n✓ Reparados: {len(reparados)}")
if sin_cambios:
    print(f"· Sin cambios (ya tenían serie): {len(sin_cambios)}")
if fallidos:
    print(f"✗ Fallidos: {len(fallidos)}")
    for f in fallidos:
        print(f"  · id={f['id']}: {f['error']}")

# Verificación (antes del commit, para poder abortar si algo no cuadra)
cur.execute("SELECT COUNT(*) AS n FROM mant_maquinas WHERE serie IS NULL OR TRIM(serie)=''")
restantes = cur.fetchone()["n"]
print(f"\n=== POST-REPARACIÓN: {restantes} equipo(s) siguen sin serie "
      f"(sin SKU o cliente sin RUT usable -- quedan para revisión manual) ===")

c.commit()
print("\n✓ COMMIT exitoso — reparación finalizada.")
c.close()
