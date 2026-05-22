"""
Recupera OTs tipo='levantamiento' o con levantamiento_id que ya fueron
aprobadas/cerradas pero NO se promovieron a la ficha del equipo.

Caso real 22/05/2026: OT 164 (Aarón aprobó pero promoción no corrió porque
el código en producción todavía es el viejo). Este script lo arregla
APLICANDO la promoción manualmente, sin esperar al deploy.

USO:
    python _admin_promover_ot_pendientes.py            # dry-run
    python _admin_promover_ot_pendientes.py --apply    # ejecutar
    python _admin_promover_ot_pendientes.py --vid 164 --apply   # solo 1 OT
"""
import os, sys, json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
import pymysql

APPLY = "--apply" in sys.argv
VID_FILTRO = None
if "--vid" in sys.argv:
    try:
        VID_FILTRO = int(sys.argv[sys.argv.index("--vid") + 1])
    except Exception:
        VID_FILTRO = None

c = pymysql.connect(
    host=os.environ['MYSQL_HOST'], port=int(os.environ['MYSQL_PORT']),
    user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'],
    database=os.environ['MYSQL_DATABASE'],
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=False,
)
cur = c.cursor()


def promover_visita(vid):
    """Replica _promover_levantamiento_a_maquina para una visita específica."""
    cur.execute(
        "SELECT id, tipo, estado, cliente_id, levantamiento_id, "
        "       fecha_realizada, firma_supervisor_at "
        "  FROM mant_visitas WHERE id=%s",
        (vid,)
    )
    v = cur.fetchone()
    if not v:
        print(f"  [vid={vid}] no existe")
        return 0
    if v["tipo"] != "levantamiento" and not v.get("levantamiento_id"):
        print(f"  [vid={vid}] no es levantamiento ni tiene levantamiento_id, skip")
        return 0
    if v["estado"] not in ("cerrada", "completada", "pendiente_aprobacion"):
        print(f"  [vid={vid}] estado={v['estado']} no procesa")
        return 0

    lev_id = v.get("levantamiento_id")

    # Buscar máquinas afectadas: por mant_visita_tareas O por items del levantamiento
    cur.execute(
        "SELECT DISTINCT maquina_id FROM mant_visita_tareas "
        " WHERE visita_id=%s AND maquina_id IS NOT NULL",
        (vid,)
    )
    maquinas_via_tareas = [r["maquina_id"] for r in cur.fetchall()]

    items_lev = []
    if lev_id:
        cur.execute(
            "SELECT id, maquina_id, nombre_snap, sku_snap, serie_snap, "
            "       estado_capturado, ubicacion, observaciones, anomalias, "
            "       marca, modelo, anio_fabricacion, voltaje, fecha_documento, "
            "       ultima_intervencion, n_fotos "
            "  FROM mant_levantamiento_items "
            " WHERE levantamiento_id=%s AND maquina_id IS NOT NULL",
            (lev_id,)
        )
        items_lev = list(cur.fetchall())

    maquinas_ids = set(maquinas_via_tareas) | {int(i["maquina_id"]) for i in items_lev if i.get("maquina_id")}
    if not maquinas_ids:
        print(f"  [vid={vid}] sin maquinas afectadas, skip")
        return 0

    aplicados = 0
    fecha_promocion = (v.get("firma_supervisor_at") or v.get("fecha_realizada")
                       or datetime.now()).date() if isinstance(
                           (v.get("firma_supervisor_at") or v.get("fecha_realizada")
                            or datetime.now()), datetime) else (
                           v.get("firma_supervisor_at") or v.get("fecha_realizada")
                           or datetime.now().date())

    for mid in maquinas_ids:
        # Idempotencia: ¿ya promovido?
        cur.execute(
            "SELECT id, aplicado_a_ficha FROM mant_maquina_levantamientos "
            " WHERE maquina_id=%s AND visita_id=%s",
            (mid, vid)
        )
        ml_row = cur.fetchone()
        if ml_row and ml_row.get("aplicado_a_ficha"):
            print(f"  [vid={vid}] maquina_id={mid} ya promovida, skip")
            continue

        # Item asociado a esta maquina (si hay)
        item = next((i for i in items_lev if int(i.get("maquina_id") or 0) == int(mid)), None)

        # Tomar máquina actual
        cur.execute("SELECT * FROM mant_maquinas WHERE id=%s", (mid,))
        m_actual = cur.fetchone()
        if not m_actual:
            print(f"  [vid={vid}] maquina_id={mid} no existe en mant_maquinas, skip")
            continue

        # Construir UPDATE (LLENAR_VACIO + SOBREESCRIBIR)
        upd_sets = []
        upd_vals = []

        def llenar_vacio(col, val):
            if val is None or (isinstance(val, str) and not val.strip()):
                return
            if m_actual.get(col) in (None, "", 0):
                upd_sets.append(f"{col}=%s")
                upd_vals.append(val)

        if item:
            llenar_vacio("marca", item.get("marca"))
            llenar_vacio("modelo", item.get("modelo"))
            llenar_vacio("anio_fabricacion", item.get("anio_fabricacion"))
            llenar_vacio("voltaje", item.get("voltaje"))
            llenar_vacio("serie", item.get("serie_snap"))
            llenar_vacio("nombre", item.get("nombre_snap"))
            llenar_vacio("fecha_instalacion", item.get("fecha_documento"))
            # SOBREESCRIBIR
            if item.get("ubicacion"):
                upd_sets.append("ubicacion_sala=%s")
                upd_vals.append(item["ubicacion"])
            if item.get("estado_capturado"):
                upd_sets.append("estado_capturado=%s")
                upd_vals.append(item["estado_capturado"])
                # Mapeo estado_op
                ec = item["estado_capturado"]
                estado_op = {
                    "operativo": "operativo",
                    "advertencia": "advertencia",
                    "falla": "fuera_servicio",
                    "fuera_servicio": "fuera_servicio",
                    "en_reparacion": "en_reparacion",
                    "dado_baja": "fuera_servicio",
                    "no_encontrado": "fuera_servicio",
                }.get(ec, "operativo")
                upd_sets.append("estado_op=%s")
                upd_vals.append(estado_op)
                upd_sets.append("tiene_dano=%s")
                upd_vals.append(1 if ec in ("advertencia", "falla", "en_reparacion") else 0)
            # Append observaciones
            obs_acc = []
            if item.get("observaciones"): obs_acc.append(str(item["observaciones"]))
            if item.get("anomalias"): obs_acc.append(f"[Anomalías] {item['anomalias']}")
            if obs_acc:
                obs_combined = f"[Levantamiento OT #{vid} {fecha_promocion}] " + " | ".join(obs_acc)
                obs_actual = (m_actual.get("observaciones") or "").strip()
                obs_new = (obs_combined + ("\n\n" + obs_actual if obs_actual else ""))[:5000]
                upd_sets.append("observaciones=%s")
                upd_vals.append(obs_new)

        # Siempre actualizar
        upd_sets.append("ultima_intervencion=%s")
        upd_vals.append(fecha_promocion)
        upd_sets.append("visitas_count=COALESCE(visitas_count,0)+1")
        upd_sets.append("last_visita_id=%s")
        upd_vals.append(vid)
        upd_sets.append("ultimo_levantamiento_vid=%s")
        upd_vals.append(vid)

        sql = f"UPDATE mant_maquinas SET {', '.join(upd_sets)} WHERE id=%s"
        upd_vals.append(mid)

        if APPLY:
            cur.execute(sql, tuple(upd_vals))
        print(f"  [vid={vid}] maquina_id={mid} UPDATE preparado: {len(upd_sets)} campos")

        # mant_maquina_levantamientos (idempotente)
        if APPLY:
            cur.execute(
                "INSERT IGNORE INTO mant_maquina_levantamientos "
                "  (maquina_id, visita_id, levantamiento_id, fecha_levantamiento, "
                "   aplicado_a_ficha, aplicado_at, aplicado_por) "
                "VALUES (%s,%s,%s,%s,1,NOW(),%s)",
                (mid, vid, lev_id, fecha_promocion, "admin_recovery_script")
            )

        # Evento (idempotente)
        if APPLY:
            cur.execute(
                "SELECT id FROM mant_maquina_eventos "
                " WHERE maquina_id=%s AND tipo='levantamiento' AND referencia_id=%s "
                " LIMIT 1",
                (mid, vid)
            )
            if not cur.fetchone():
                meta_ev = {
                    "vid": vid, "lev_id": lev_id,
                    "marca": (item or {}).get("marca"),
                    "modelo": (item or {}).get("modelo"),
                    "estado_capturado": (item or {}).get("estado_capturado"),
                    "fuente": "admin_recovery_script",
                }
                desc = (f"Ficha actualizada por levantamiento OT #{vid} "
                        f"(recuperación admin {datetime.now().strftime('%Y-%m-%d %H:%M')})")
                cur.execute(
                    "INSERT INTO mant_maquina_eventos "
                    "  (maquina_id, cliente_id, tipo, descripcion, referencia_tabla, "
                    "   referencia_id, metadata_json, created_by) "
                    "VALUES (%s,%s,'levantamiento',%s,'mant_visitas',%s,%s,%s)",
                    (mid, m_actual.get("cliente_id"), desc[:400], vid,
                     json.dumps(meta_ev, ensure_ascii=False, default=str)[:5000],
                     "admin_recovery_script")
                )

        # Audit log
        if APPLY:
            cur.execute(
                "INSERT INTO mant_logs (entidad, entidad_id, accion, detalle, usuario, created_at) "
                "VALUES ('maquina',%s,'promovida_admin_recovery',%s,%s,NOW())",
                (mid, f"Promoción manual OT #{vid} (script recuperación)", "admin_recovery_script")
            )

        aplicados += 1

    # Log a nivel visita
    if APPLY and aplicados > 0:
        cur.execute(
            "INSERT INTO mant_logs (entidad, entidad_id, accion, detalle, usuario, created_at) "
            "VALUES ('visita',%s,'levantamiento_completado_recovery',%s,%s,NOW())",
            (vid, f"Promoción recuperación: {aplicados} equipo(s) aplicado(s) a su ficha", "admin_recovery_script")
        )
    return aplicados


# 1) Buscar candidatas
if VID_FILTRO:
    cur.execute("SELECT id FROM mant_visitas WHERE id=%s", (VID_FILTRO,))
    candidatas = cur.fetchall()
else:
    cur.execute("""
        SELECT v.id
          FROM mant_visitas v
          LEFT JOIN mant_maquina_levantamientos ml
                 ON ml.visita_id = v.id AND ml.aplicado_a_ficha = 1
         WHERE (v.tipo='levantamiento' OR v.levantamiento_id IS NOT NULL)
           AND v.estado IN ('cerrada','completada','pendiente_aprobacion')
           AND ml.id IS NULL
         ORDER BY v.id
    """)
    candidatas = cur.fetchall()

print(f"=== OTs candidatas a promover: {len(candidatas)} ===")
print(f"Modo: {'APPLY' if APPLY else 'DRY-RUN'}")

total_aplicados = 0
for cand in candidatas:
    vid = cand["id"]
    print(f"\n--- Procesando vid={vid} ---")
    total_aplicados += promover_visita(vid)

if APPLY:
    c.commit()
    print(f"\n✓ COMMIT exitoso — {total_aplicados} equipo(s) promovido(s) en total")
else:
    print(f"\n(dry-run) habría promovido {total_aplicados} equipo(s). Re-ejecuta con --apply.")
c.close()
