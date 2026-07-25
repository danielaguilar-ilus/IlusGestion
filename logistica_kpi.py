"""
Modulo LOGISTICA KPI — Historial de despachos + Dashboard KPI de Transporte.

Vive en un archivo APARTE (no en app.py) por dos razones:
  1) app.py tiene ~75.000 lineas y hay otra sesion editandolo en paralelo
     ahora mismo (modulo de Cotizaciones de logistica, tr_agregar_item /
     tr_quitar_item / _tr_recalc_totales_manifiesto y el final del archivo).
  2) Todo lo que hay aca es AGREGADO: no toca ni redefine nada de Transporte
     existente (Regla #4.2). Solo LEE de transport_commitments /
     transport_commitment_lines / transport_manifests / transport_manifest_items
     (todas ya blindadas y en produccion) — cero escritura, cero migracion
     de schema nueva.

Se registra desde app.py con 3 lineas al FINAL del archivo (las agrega OTRO
agente, mismo patron que cubicador_plus.py):

    try:
        from logistica_kpi import register_logistica_kpi
        register_logistica_kpi(app)
        print("[ILUS] Modulo Logistica KPI registrado.")
    except Exception as _logkpi_reg_err:
        print(f"[ILUS][WARN] register_logistica_kpi: {_logkpi_reg_err}")

RUTA QUE EXPONE (gate: login + g.permissions['transporte'], igual que el
resto de /transporte/*)

    GET  /transporte/historial   -> Historial de despachos + dashboard KPI
                                     (server-rendered, filtros por GET query
                                     string: desde/hasta/estado/courier/comuna/q)

POR QUE ESTOS 4 KPI Y NO OTIF "REAL"
-------------------------------------------------------------------------
Investigacion de esquema (2026-07-25, sin tocar app.py) confirmo que HOY se
puede calcular con datos reales ya existentes:

  1) Fill rate por linea      = SUM(cant_despachada)/SUM(cantidad)
                                 FROM transport_commitment_lines
                                 JOIN transport_commitments (clasificacion='despacho')
  2) Tasa de entrega exitosa  = COUNT(estado_entrega='Entregado') / COUNT(*)
                                 FROM transport_manifest_items JOIN transport_manifests
  3) Tasa de incidencias      = COUNT(estado_entrega IN ('Entrega fallida','Devolucion')) / COUNT(*)
                                 (misma tabla/filtro que #2 — el inverso operativo)
  4) Lead time promedio       = AVG(DATEDIFF(delivered_at, fecha_emision))
                                 FROM transport_commitments WHERE delivered_at IS NOT NULL

Lo que NO se calcula (a proposito, no es un olvido):
  - OTIF real ("a tiempo"): requeriria comparar delivered_at contra una fecha
    COMPROMETIDA confiable. fecha_entrega es el campo crudo FEER del ERP (no
    garantiza estar poblado ni ser una promesa operativa de ILUS). fecha_agenda
    SI es la fecha que agenda el operador, pero se llena a mano campo por campo
    desde el modal del manifiesto — no hay garantia de cobertura 100%.
  - SLA cumplido por courier: no existe ninguna tabla que defina el umbral
    objetivo por courier (ej. "FedEx = 3 dias habiles"). Sin ese dato de
    negocio, "SLA cumplido" no es una query, es una decision pendiente de
    Daniel.
  Ver docstring completo de la investigacion en la conversacion que genero
  este archivo. Si el dia de mañana se define fecha_agenda como obligatoria
  y un umbral de SLA por courier, el lugar natural para agregar esos 2 KPIs
  es `_calcular_kpis()` mas abajo — no duplicar la pagina.

Autor: agente Claude — 2026-07-25. Reglas aplicadas: #1 (sin alert/confirm
nativos — esta pagina es 100% GET/lectura, ni siquiera los necesita), #2
(paleta), #3 (mobile-first), #4 (SQL parametrizado con %s, sin detalles
internos al cliente en errores), #4.1 (cero contacto con el ERP Random: todo
sale de las tablas propias de ILUS en MySQL), #4.2 (solo agrega: nueva ruta,
nuevo template, un link nuevo en el sidebar y en el Monitor — nada se quita),
#5 (columnas verificadas contra los CREATE TABLE de app.py antes de escribir
cada SELECT), #6 (delivered_at se muestra con el filtro chile_fmt en el
template, nunca crudo en UTC).
"""

import sys
import traceback
from datetime import date, timedelta
from functools import wraps

from flask import request, jsonify, g, flash, redirect, url_for, render_template

# ──────────────────────────────────────────────────────────────────────
#  Acceso a los helpers de app.py — IMPORT DIFERIDO (mismo patron que
#  cubicador_plus.py: getattr en tiempo de request, nunca import directo).
# ──────────────────────────────────────────────────────────────────────

_APP_MODULE_NAME = "app"


def _appmod():
    mod = sys.modules.get(_APP_MODULE_NAME)
    if mod is None:
        mod = sys.modules.get("app") or sys.modules.get("__main__")
    return mod


def _h(nombre, default=None):
    mod = _appmod()
    if mod is None:
        return default
    return getattr(mod, nombre, default)


# ──────────────────────────────────────────────────────────────────────
#  Gates de permiso — mismo criterio que _tr_required (app.py:19583):
#  requiere login + g.permissions['transporte']. Esta pagina es solo
#  lectura, asi que un unico gate de PAGINA alcanza (no hay endpoints JSON).
# ──────────────────────────────────────────────────────────────────────

def _hist_page(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        permisos = getattr(g, "permissions", None) or {}
        if not permisos.get("transporte"):
            flash("Sin acceso al modulo Transporte.", "danger")
            return redirect(url_for("index"))
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            print(f"[logistica_kpi][ERROR] {fn.__name__}: {e}", flush=True)
            try:
                print(traceback.format_exc(), flush=True)
            except Exception:
                pass
            flash("No se pudo cargar el historial. Intenta nuevamente.", "danger")
            return redirect(url_for("transporte_index"))

    login_required = _h("login_required")
    return login_required(wrapped) if callable(login_required) else wrapped


# ──────────────────────────────────────────────────────────────────────
#  Rango de fechas — MISMA logica de atajos que transporte_index()
#  (app.py:21642-21681): mes / 3m / año / todo / rango custom por querystring.
#  Se reimplementa (no se importa) porque transporte_index no la expone como
#  funcion reusable; son ~15 lineas y mantener el mismo comportamiento visual
#  importa mas que evitar la duplicacion minima.
# ──────────────────────────────────────────────────────────────────────

def _rango_fechas():
    periodo = request.args.get("periodo", "")
    hoy = date.today()
    default_desde = (hoy - timedelta(days=30)).strftime("%Y-%m-%d")
    default_hasta = hoy.strftime("%Y-%m-%d")

    if periodo == "mes":
        desde, hasta = default_desde, default_hasta
    elif periodo == "3m":
        desde = (hoy - timedelta(days=90)).strftime("%Y-%m-%d")
        hasta = default_hasta
    elif periodo == "año" or periodo == "ano":
        desde = hoy.strftime("%Y-01-01")
        hasta = default_hasta
    elif periodo == "todo":
        desde, hasta = "", ""
    else:
        desde = (request.args.get("desde", default_desde) or "").strip()
        hasta = (request.args.get("hasta", default_hasta) or "").strip()
        periodo = "custom" if (desde != default_desde or hasta != default_hasta) else ""
    return periodo, desde, hasta


# ──────────────────────────────────────────────────────────────────────
#  KPIs — 4 metricas, formula exacta documentada en el docstring del modulo.
#  Ninguna requiere tocar el ERP Random (Regla #4.1): las 4 salen de tablas
#  propias de ILUS en MySQL Clever Cloud.
# ──────────────────────────────────────────────────────────────────────

def _calcular_kpis(desde, hasta):
    mysql_fetchone = _h("mysql_fetchone")

    where_fecha = ""
    params_fecha = ()
    if desde:
        where_fecha += " AND c.fecha_emision >= %s"
        params_fecha += (desde,)
    if hasta:
        where_fecha += " AND c.fecha_emision <= %s"
        params_fecha += (hasta,)

    # 1) Fill rate por línea (columnas verificadas: app.py:2438-2454)
    r1 = mysql_fetchone(
        "SELECT COALESCE(SUM(l.cant_despachada),0) AS despachado, "
        "       COALESCE(SUM(l.cantidad),0) AS pedido "
        "FROM transport_commitment_lines l "
        "JOIN transport_commitments c ON c.id = l.commitment_id "
        "WHERE c.clasificacion='despacho'" + where_fecha,
        params_fecha,
    ) or {}
    pedido = float(r1.get("pedido") or 0)
    despachado = float(r1.get("despachado") or 0)
    fill_rate = round((despachado / pedido) * 100, 1) if pedido > 0 else None

    # 2) y 3) Tasa de entrega exitosa / tasa de incidencias — misma tabla,
    # filtradas por la FECHA DEL MANIFIESTO (columnas: app.py:2475-2510).
    where_fecha_m = ""
    params_fecha_m = ()
    if desde:
        where_fecha_m += " AND m.fecha >= %s"
        params_fecha_m += (desde,)
    if hasta:
        where_fecha_m += " AND m.fecha <= %s"
        params_fecha_m += (hasta,)
    r2 = mysql_fetchone(
        "SELECT COUNT(*) AS total, "
        "       SUM(CASE WHEN mi.estado_entrega='Entregado' THEN 1 ELSE 0 END) AS entregados, "
        "       SUM(CASE WHEN mi.estado_entrega IN ('Entrega fallida','Devolución') "
        "                THEN 1 ELSE 0 END) AS incidencias "
        "FROM transport_manifest_items mi "
        "JOIN transport_manifests m ON m.id = mi.manifest_id "
        "WHERE 1=1" + where_fecha_m,
        params_fecha_m,
    ) or {}
    total_items = int(r2.get("total") or 0)
    entregados = int(r2.get("entregados") or 0)
    incidencias = int(r2.get("incidencias") or 0)
    tasa_entrega = round((entregados / total_items) * 100, 1) if total_items > 0 else None
    tasa_incidencias = round((incidencias / total_items) * 100, 1) if total_items > 0 else None

    # 4) Lead time promedio (columna delivered_at: app.py:71325-71326/71380)
    r3 = mysql_fetchone(
        "SELECT AVG(DATEDIFF(delivered_at, fecha_emision)) AS lead_avg, COUNT(*) AS n "
        "FROM transport_commitments c "
        "WHERE delivered_at IS NOT NULL" + where_fecha,
        params_fecha,
    ) or {}
    lead_raw = r3.get("lead_avg")
    lead_time = round(float(lead_raw), 1) if lead_raw is not None else None
    lead_n = int(r3.get("n") or 0)

    return {
        "fill_rate": {
            "valor": fill_rate, "pedido": pedido, "despachado": despachado,
            "sub": (f"{despachado:,.0f} de {pedido:,.0f} unidades despachadas"
                    .replace(",", ".") if pedido > 0 else "Sin lineas de despacho en el periodo."),
        },
        "tasa_entrega": {
            "valor": tasa_entrega, "n": total_items, "entregados": entregados,
            "sub": (f"{entregados} de {total_items} items entregados"
                    if total_items > 0 else "Sin items de manifiesto en el periodo."),
        },
        "tasa_incidencias": {
            "valor": tasa_incidencias, "n": total_items, "incidencias": incidencias,
            "sub": (f"{incidencias} de {total_items} items con fallo o devolucion"
                    if total_items > 0 else "Sin items de manifiesto en el periodo."),
        },
        "lead_time": {
            "dias": lead_time, "n": lead_n,
            "sub": (f"Promedio sobre {lead_n} despacho(s) con entrega confirmada"
                    if lead_n > 0 else "Aun no hay despachos con entrega confirmada "
                                       "(delivered_at) en el periodo."),
        },
    }


# ──────────────────────────────────────────────────────────────────────
#  Listado — todos los transport_commitments del rango, con filtros.
#  El courier NO vive en transport_commitments (vive en transport_manifests,
#  ver app.py:21729/22828/etc.) — se resuelve con el ULTIMO manifest_item del
#  compromiso via subquery MAX(id) (mismo patron que app.py:74405-74423,
#  evita ROW_NUMBER por compatibilidad con MySQL 5.7).
# ──────────────────────────────────────────────────────────────────────

_PAGE_SIZE = 50


def _listar_compromisos(desde, hasta, estado, courier, comuna, q, page):
    mysql_fetchall = _h("mysql_fetchall")
    mysql_fetchone = _h("mysql_fetchone")

    where = ["1=1"]
    params = []

    if q:
        q = q.strip()
        if q.isdigit():
            where.append("(c.nudo = %s OR c.nudo LIKE %s OR c.cliente_nombre LIKE %s)")
            params += [q, f"%{q}", f"%{q}%"]
        else:
            where.append("(c.cliente_nombre LIKE %s OR c.comuna LIKE %s OR c.nudo LIKE %s)")
            ql = f"%{q}%"
            params += [ql, ql, ql]
    else:
        if desde:
            where.append("c.fecha_emision >= %s"); params.append(desde)
        if hasta:
            where.append("c.fecha_emision <= %s"); params.append(hasta)
        if estado:
            where.append("c.estado = %s"); params.append(estado)
        if comuna:
            where.append("c.comuna LIKE %s"); params.append(f"%{comuna}%")
        if courier:
            where.append("m2.courier = %s"); params.append(courier)

    where_sql = " AND ".join(where)

    base_from = (
        "FROM transport_commitments c "
        "LEFT JOIN ( "
        "  SELECT commitment_id, MAX(id) AS max_id "
        "  FROM transport_manifest_items GROUP BY commitment_id "
        ") lmx ON lmx.commitment_id = c.id "
        "LEFT JOIN transport_manifest_items mi2 ON mi2.id = lmx.max_id "
        "LEFT JOIN transport_manifests m2 ON m2.id = mi2.manifest_id "
    )

    total_row = mysql_fetchone(
        f"SELECT COUNT(*) AS n {base_from} WHERE {where_sql}", tuple(params)
    ) or {}
    total = int(total_row.get("n") or 0)

    page = max(1, int(page or 1))
    offset = (page - 1) * _PAGE_SIZE
    total_pages = max(1, (total + _PAGE_SIZE - 1) // _PAGE_SIZE)
    if page > total_pages:
        page = total_pages
        offset = (page - 1) * _PAGE_SIZE

    rows = mysql_fetchall(
        f"SELECT c.id, c.tido, c.nudo, c.cliente_nombre, c.comuna, c.estado, "
        f"       c.fecha_emision, c.fecha_agenda, c.delivered_at, c.clasificacion, "
        f"       c.public_token, c.preventa, "
        f"       m2.courier AS courier, mi2.estado_entrega AS estado_entrega, "
        f"       m2.correlativo AS manifiesto_correlativo "
        f"{base_from} "
        f"WHERE {where_sql} "
        f"ORDER BY c.fecha_emision DESC, c.id DESC "
        f"LIMIT %s OFFSET %s",
        tuple(params) + (_PAGE_SIZE, offset),
    ) or []

    for r in rows:
        nudo = str(r.get("nudo") or "")
        r["nudo_display"] = nudo.lstrip("0") or nudo

    return rows, total, page, total_pages


# ══════════════════════════════════════════════════════════════════════
#  REGISTRO DE RUTAS
# ══════════════════════════════════════════════════════════════════════

def register_logistica_kpi(app, ctx=None):
    global _APP_MODULE_NAME
    try:
        nombre_mod = getattr(app, "import_name", None)
        if nombre_mod and nombre_mod in sys.modules:
            _APP_MODULE_NAME = nombre_mod
    except Exception:
        pass

    @app.route("/transporte/historial", methods=["GET"])
    @_hist_page
    def tr_historial_kpi():
        """Historial de despachos + dashboard KPI (server-rendered).

        Filtros por querystring (todos opcionales, se conservan al paginar):
          periodo=mes|3m|año|todo   -> atajos de fecha (igual que el Monitor)
          desde=YYYY-MM-DD / hasta=YYYY-MM-DD -> rango custom
          estado=<uno de ESTADOS_COMPROMISO>
          courier=<uno de COURIERS>
          comuna=<texto>
          q=<texto o Nº doc>       -> buscador global (ignora los demas filtros
                                       de fecha/estado, mismo criterio que
                                       transporte_index para no "esconder" un
                                       documento puntual fuera del rango)
          page=<int>
        """
        periodo, desde, hasta = _rango_fechas()
        estado = (request.args.get("estado", "") or "").strip()
        courier = (request.args.get("courier", "") or "").strip()
        comuna = (request.args.get("comuna", "") or "").strip()
        q = (request.args.get("q", "") or "").strip()
        try:
            page = int(request.args.get("page", "1"))
        except (TypeError, ValueError):
            page = 1

        kpis = _calcular_kpis(desde, hasta)
        compromisos, total, page, total_pages = _listar_compromisos(
            desde, hasta, estado, courier, comuna, q, page
        )

        filtros = {
            "periodo": periodo, "desde": desde, "hasta": hasta,
            "estado": estado, "courier": courier, "comuna": comuna, "q": q,
        }

        return render_template(
            "transporte/historial.html",
            kpis=kpis,
            compromisos=compromisos,
            total=total,
            page=page,
            total_pages=total_pages,
            page_size=_PAGE_SIZE,
            filtros=filtros,
            estados=_h("ESTADOS_COMPROMISO") or [],
            couriers=_h("COURIERS") or [],
            estado_colors=_h("ESTADO_COLORS") or {},
        )

    return app
