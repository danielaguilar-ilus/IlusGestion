"""
Modulo CUADRATURA — factura vs picking real, por SKU.

Vive en un archivo APARTE (no en app.py) por las mismas dos razones que
cubicador_plus.py:
  1) app.py tiene ~75.000+ lineas y hay otra sesion editandolo en paralelo
     (modulo Cotizaciones de logistica, logistica_cotizaciones.py).
  2) Todo lo que hay aca es AGREGADO: no toca ni redefine nada del Cubicador
     actual ni de cubicador_plus.py (Regla #4.2). Solo LEE documentos del
     ERP llamando a `_cubicador_fetch` (blindada, NO se reimplementa) y
     agrega UNA tabla propia nueva para el conteo real de picking.

Se registra desde app.py con 3 lineas, al FINAL del archivo (mismo patron
que cubicador_plus.py / logistica_cotizaciones.py):

    try:
        from logistica_cuadratura import register_logistica_cuadratura
        register_logistica_cuadratura(app)
        print("[logistica_cuadratura] rutas registradas")
    except Exception as _e_cuad:
        print(f"[logistica_cuadratura] NO se pudo registrar: {_e_cuad}")

RUTAS QUE EXPONE (todas con el gate del Cubicador: login + g.permissions['cubicador'])

    GET  /cubicador/api/cuadratura/<tido>/<nudo>
         -> por SKU: cantidad facturada (ERP) vs cantidad de picking real
            (tabla propia) + diferencia + semaforo. NUNCA inventa una
            "cantidad de nota de venta": ver INVESTIGACION abajo.

    POST /cubicador/api/cuadratura/<tido>/<nudo>/picking
         -> guarda el conteo real de bodega (upsert por SKU).
            Body: {"items": [{"sku": "ABC-123", "cantidad_picking": 8}, ...]}

INVESTIGACION QUE SUSTENTA ESTE DISEÑO (2026-07-25, ver conversacion con
Daniel / MEMORY.md "levantamiento_puro_descubrimiento" — no, ese es otro;
ver el resumen que trajo esta tarea):

  1) _cubicador_fetch(tido, nudo) SI trae una cantidad por linea de
     CUALQUIER documento del ERP (incluida una factura FCV), pero es un
     campo generico "cantidad" (= CAPRCO1 del ERP) cuyo SIGNIFICADO
     depende de que TIDO se consulto. No hay una llamada que traiga
     "cantidad facturada" y "cantidad de nota de venta" juntas: si
     algun dia se quiere cruzar FCV vs NVV automaticamente, hay que
     llamar _cubicador_fetch DOS VECES (una por documento) y cruzar por
     SKU aparte. Este modulo NO hace eso: compara la cantidad del
     documento que el usuario elija (tipicamente la factura) contra el
     conteo REAL de picking. Si Daniel quiere tambien comparar contra
     una nota de venta, ese es el MISMO patron: pedirle un segundo
     documento y cruzar — no esta implementado aca por honestidad (no
     estaba pedido explicitamente y agregarlo a ciegas seria inventar
     un cruce que hoy no se validaba con nadie).

  2) NO existe en Random un vinculo estructurado factura->nota de venta
     (ni en erp_engine.py ni en los diccionarios de campos ya usados en
     el proyecto). Solo hay texto libre en observaciones/glosa
     ("CAMBIO DE DCTO FCV-10201") que NO es un FK confiable. Por eso
     este modulo no intenta resolver solo la nota de venta de una
     factura: si se necesita, el USUARIO elige el segundo documento a
     mano (mismo criterio que el selector de documentos del Cubicador).

  3) El checklist de picking de Retiros (pickup_picking_items) NO guarda
     una cantidad picoteada real: guarda una cantidad ESPERADA (copiada
     del ERP) + un booleano picked (0/1). No sirve como fuente de
     "cuanto se picoteo realmente" — por eso este modulo crea una tabla
     PROPIA (transport_cuadratura_picking) donde bodega ingresa el
     conteo real por SKU, en vez de inventar que ese dato ya existe en
     algun lado.

  4) Conclusion: la unica cuadratura HONESTA hoy es
     "cantidad del documento ERP elegido" vs "conteo manual de bodega".
     Este modulo hace exactamente eso, con el conteo manual quedando
     PERSISTIDO (no se pierde al recargar) y auditado (quien y cuando).

Autor: agente Claude — 2026-07-25. Reglas aplicadas: #1 (front sin
alert/confirm nativos), #2 (paleta), #3 (mobile), #4 (SQL parametrizado,
sin detalles internos al cliente), #4.1 (solo LEE el ERP via
_cubicador_fetch, jamas escribe en Random), #4.2 (solo agrega), #5 (tabla
nueva idempotente, se crea SIEMPRE al registrar porque prod corre con
ILUS_SKIP_MIGRATIONS=1), #6 (horas Chile via filtro Jinja `chile_fmt`
donde corresponda).
"""

import sys
import traceback
from functools import wraps

from flask import request, jsonify, g

# ──────────────────────────────────────────────────────────────────────
#  Acceso a los helpers de app.py — IMPORT DIFERIDO (mismo patron que
#  cubicador_plus.py: resolver por getattr en tiempo de request evita el
#  import circular con un app.py que todavia se esta ejecutando a si mismo).
# ──────────────────────────────────────────────────────────────────────

_APP_MODULE_NAME = "app"

_TABLA_PICKING = "transport_cuadratura_picking"


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


def _f(value, default=0.0):
    """float tolerante: acepta None, Decimal, '12,5' (coma decimal chilena)."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        pass
    try:
        txt = str(value).strip().replace("$", "").replace(" ", "")
        if "," in txt and "." in txt:
            txt = txt.replace(".", "").replace(",", ".")
        elif "," in txt:
            txt = txt.replace(",", ".")
        return float(txt)
    except (TypeError, ValueError):
        return default


def _r(value, dec=2):
    try:
        return round(float(value or 0), dec)
    except (TypeError, ValueError):
        return 0.0


# Tolerancia para considerar "cuadrado" (evita falsos rojos por redondeo de
# decimales que arrastra el ERP en algunos TIDOs).
_TOLERANCIA = 0.001


# ──────────────────────────────────────────────────────────────────────
#  Gate de permiso + blindaje de errores — mismo criterio que cubicador_plus.py
# ──────────────────────────────────────────────────────────────────────

def _cuad_api(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        try:
            permisos = getattr(g, "permissions", None) or {}
            if not permisos.get("cubicador"):
                return jsonify({
                    "ok": False,
                    "error": "No tienes acceso al modulo Cubicador.",
                    "error_codigo": "SIN_PERMISO",
                    "permiso_requerido": "cubicador",
                }), 403
        except Exception as e_perm:
            print(f"[logistica_cuadratura] fallo leyendo permisos: {e_perm}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo validar tu sesion."}), 403

        try:
            return fn(*args, **kwargs)
        except Exception as e:
            print(f"[logistica_cuadratura][ERROR] {fn.__name__}: {e}", flush=True)
            try:
                print(traceback.format_exc(), flush=True)
            except Exception:
                pass
            return jsonify({
                "ok": False,
                "error": "Ocurrio un problema procesando la solicitud. "
                         "Intenta nuevamente; si persiste avisa a soporte.",
            }), 500

    login_required = _h("login_required")
    return login_required(wrapped) if callable(login_required) else wrapped


# ══════════════════════════════════════════════════════════════════════
#  SCHEMA — tabla NUEVA, se crea SIEMPRE al registrar (Regla #5:
#  ILUS_SKIP_MIGRATIONS=1 en prod, este modulo no puede depender de
#  ninguna migracion centralizada).
# ══════════════════════════════════════════════════════════════════════

def _ensure_cuadratura_schema():
    get_mysql = _h("get_mysql")
    if not callable(get_mysql):
        print("[logistica_cuadratura][WARN] get_mysql no disponible: "
              "no se pudo verificar/crear la tabla de picking.", flush=True)
        return
    conn = get_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS `{_TABLA_PICKING}` (
                    id                INT AUTO_INCREMENT PRIMARY KEY,
                    tido              VARCHAR(5)    NOT NULL,
                    nudo              VARCHAR(20)   NOT NULL,
                    sku               VARCHAR(80)   NOT NULL,
                    descripcion       VARCHAR(300),
                    cantidad_picking  DECIMAL(12,2) NOT NULL DEFAULT 0,
                    capturado_por     VARCHAR(190),
                    capturado_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP
                                      ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_doc_sku (tido, nudo, sku)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
        conn.commit()
    except Exception as e:
        print(f"[logistica_cuadratura][WARN] no se pudo crear/verificar "
              f"`{_TABLA_PICKING}`: {e}", flush=True)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════
#  NUCLEO: arma la cuadratura de un documento
# ══════════════════════════════════════════════════════════════════════

def _picking_guardado(tido, nudo):
    """Filas ya capturadas por bodega para este documento, indexadas por SKU."""
    mysql_fetchall = _h("mysql_fetchall")
    rows = mysql_fetchall(
        f"SELECT sku, cantidad_picking, capturado_por, capturado_at, updated_at "
        f"FROM `{_TABLA_PICKING}` WHERE tido=%s AND nudo=%s",
        (tido, nudo),
    ) or []
    out = {}
    for r in rows:
        out[(r["sku"] or "").strip().upper()] = r
    return out


def _armar_cuadratura(tido, nudo):
    """Devuelve (resultado_dict, status_http). No lanza."""
    _cubicador_fetch = _h("_cubicador_fetch")
    if not callable(_cubicador_fetch):
        return {"ok": False,
                "error": "El lector de documentos del ERP no esta disponible."}, 503

    try:
        header, lineas = _cubicador_fetch(tido, nudo)
    except ConnectionError as ce:
        print(f"[logistica_cuadratura] ERP no disponible {tido} {nudo}: {ce}", flush=True)
        return {"ok": False,
                "error": "El ERP no esta respondiendo en este momento. "
                         "Intenta nuevamente en unos minutos.",
                "error_codigo": "ERP_DOWN"}, 503

    if header is None:
        return {"ok": False,
                "error": f"No se encontro {tido} N° {nudo} en el ERP.",
                "error_codigo": "DOC_NO_ENCONTRADO"}, 404

    picking_map = _picking_guardado(tido, nudo)
    skus_documento = set()

    filas = []
    for l in (lineas or []):
        sku = (l.get("sku") or "").strip().upper()
        if not sku:
            continue
        # Las lineas de servicio del ERP (ZZ Envio, etc.) no son productos
        # que bodega pueda "picotear" — se excluyen de la cuadratura, igual
        # que el packing list. `es_zz` ya lo calcula _cubicador_fetch.
        if l.get("es_zz"):
            continue

        skus_documento.add(sku)
        facturada = _f(l.get("cantidad"))
        guardado = picking_map.get(sku)

        if guardado is None:
            estado = "pendiente"
            picking = None
            diferencia = None
        else:
            picking = _f(guardado.get("cantidad_picking"))
            diferencia = _r(facturada - picking, 2)
            estado = "ok" if abs(diferencia) <= _TOLERANCIA else "diferencia"

        filas.append({
            "sku": sku,
            "descripcion": (l.get("descripcion_erp") or l.get("nombre_app") or "").strip() or sku,
            "cantidad_facturada": _r(facturada, 2),
            "cantidad_picking": (_r(picking, 2) if picking is not None else None),
            "diferencia": diferencia,
            "estado": estado,  # 'pendiente' | 'ok' | 'diferencia'
            "capturado_por": (guardado or {}).get("capturado_por"),
            "capturado_at": str((guardado or {}).get("capturado_at") or "") or None,
        })

    # SKUs con conteo de picking guardado que YA NO estan en el documento
    # (p.ej. el operador se equivoco de SKU, o el documento cambio de
    # lineas). Se reportan aparte: NUNCA se descartan en silencio.
    huerfanos = []
    for sku, g_row in picking_map.items():
        if sku in skus_documento:
            continue
        huerfanos.append({
            "sku": sku,
            "descripcion": g_row.get("descripcion") or "",
            "cantidad_facturada": None,
            "cantidad_picking": _r(g_row.get("cantidad_picking"), 2),
            "diferencia": None,
            "estado": "sin_documento",
            "capturado_por": g_row.get("capturado_por"),
            "capturado_at": str(g_row.get("capturado_at") or "") or None,
        })

    n_pendientes = sum(1 for f in filas if f["estado"] == "pendiente")
    n_diferencia = sum(1 for f in filas if f["estado"] == "diferencia")
    n_ok = sum(1 for f in filas if f["estado"] == "ok")

    if n_diferencia > 0 or huerfanos:
        semaforo = "rojo"
    elif n_pendientes > 0:
        semaforo = "amarillo"
    elif n_ok > 0:
        semaforo = "verde"
    else:
        semaforo = "gris"  # documento sin lineas fisicas (todo ZZ)

    resultado = {
        "ok": True,
        "documento": {
            "tido": header.get("tido") or tido,
            "nudo": header.get("nudo") or nudo,
            "nudo_display": header.get("nudo_display") or nudo,
            "fecha": header.get("fecha") or "",
            "cliente_nombre": header.get("cliente_nombre") or "",
        },
        "filas": filas,
        "huerfanos": huerfanos,
        "resumen": {
            "total_skus": len(filas),
            "ok": n_ok,
            "pendientes": n_pendientes,
            "con_diferencia": n_diferencia,
            "huerfanos": len(huerfanos),
        },
        "semaforo": semaforo,
        "nota": ("La 'cantidad facturada' viene del documento del ERP consultado "
                 "(tal como Random la registra). La 'cantidad de picking' es un conteo "
                 "MANUAL que ingresa bodega — hoy no existe en ningun lado del sistema "
                 "un conteo real de picking, por eso se captura y guarda aca."),
    }
    return resultado, 200


# ══════════════════════════════════════════════════════════════════════
#  REGISTRO DE RUTAS
# ══════════════════════════════════════════════════════════════════════

def register_logistica_cuadratura(app, ctx=None):
    global _APP_MODULE_NAME
    try:
        nombre_mod = getattr(app, "import_name", None)
        if nombre_mod and nombre_mod in sys.modules:
            _APP_MODULE_NAME = nombre_mod
    except Exception:
        pass

    # ──────────────────────────────────────────────────────────────
    #  GET /cubicador/api/cuadratura/<tido>/<nudo>
    # ──────────────────────────────────────────────────────────────
    @app.route("/cubicador/api/cuadratura/<string:tido>/<path:nudo>", methods=["GET"])
    @_cuad_api
    def logcuad_get(tido, nudo):
        """Cuadratura factura (o el documento elegido) vs picking real, por SKU.

        Ver docstring del modulo: 'cantidad_facturada' es la cantidad del
        documento ERP consultado (campo generico segun el TIDO pedido);
        'cantidad_picking' es null si bodega todavia no cargo el conteo
        para ese SKU (estado 'pendiente').
        """
        tido = (tido or "").strip().upper()
        nudo = (nudo or "").strip()
        if not tido or not nudo:
            return jsonify({"ok": False,
                            "error": "Falta el tipo o el numero de documento."}), 400

        tipos = _h("TIPOS_DOC_CUBICADOR") or []
        codigos = {str(t[0]).upper() for t in tipos if isinstance(t, (list, tuple)) and t}
        if codigos and tido not in codigos:
            return jsonify({
                "ok": False,
                "error": f"Tipo de documento no soportado: {tido}.",
                "tipos_validos": sorted(codigos),
            }), 400

        data, status = _armar_cuadratura(tido, nudo)
        return jsonify(data), status

    # ──────────────────────────────────────────────────────────────
    #  POST /cubicador/api/cuadratura/<tido>/<nudo>/picking
    # ──────────────────────────────────────────────────────────────
    @app.route("/cubicador/api/cuadratura/<string:tido>/<path:nudo>/picking", methods=["POST"])
    @_cuad_api
    def logcuad_post_picking(tido, nudo):
        """Guarda (upsert) el conteo REAL de picking por SKU, ingresado por bodega.

        Body JSON:
            {"items": [{"sku": "ABC-123", "cantidad_picking": 8}, ...]}

        `cantidad_picking` acepta 0 (SKU contado y no habia ninguno) — lo
        unico que NO se guarda es un item sin SKU. Idempotente: reenviar el
        mismo SKU actualiza el conteo (ON DUPLICATE KEY UPDATE), no duplica
        filas (UNIQUE (tido,nudo,sku)).
        """
        tido = (tido or "").strip().upper()
        nudo = (nudo or "").strip()
        if not tido or not nudo:
            return jsonify({"ok": False,
                            "error": "Falta el tipo o el numero de documento."}), 400

        payload = request.get_json(silent=True) or {}
        items_raw = payload.get("items")
        if not isinstance(items_raw, (list, tuple)) or not items_raw:
            return jsonify({"ok": False,
                            "error": "No se recibio ningun item para guardar.",
                            "error_codigo": "SIN_ITEMS"}), 400

        current_username = _h("current_username")
        try:
            usuario = current_username() if callable(current_username) else None
        except Exception:
            usuario = None

        validos, descartados = [], []
        for idx, it in enumerate(items_raw, start=1):
            if not isinstance(it, dict):
                descartados.append({"posicion": idx, "motivo": "Formato invalido."})
                continue
            sku = (it.get("sku") or "").strip().upper()
            if not sku:
                descartados.append({"posicion": idx, "motivo": "Falta el SKU."})
                continue
            cant = _f(it.get("cantidad_picking"), None)
            if cant is None or cant < 0:
                descartados.append({"posicion": idx, "sku": sku,
                                    "motivo": "cantidad_picking invalida (debe ser >= 0)."})
                continue
            validos.append({
                "sku": sku,
                "descripcion": (it.get("descripcion") or "").strip()[:300],
                "cantidad_picking": _r(cant, 2),
            })

        if not validos:
            return jsonify({
                "ok": False,
                "error": "Ningun item valido para guardar.",
                "error_codigo": "SIN_ITEMS_VALIDOS",
                "descartados": descartados,
            }), 400

        get_db = _h("get_db")
        conn = get_db()
        try:
            with conn.cursor() as cur:
                for it in validos:
                    cur.execute(
                        f"""INSERT INTO `{_TABLA_PICKING}`
                                (tido, nudo, sku, descripcion, cantidad_picking, capturado_por)
                            VALUES (%s,%s,%s,%s,%s,%s)
                            ON DUPLICATE KEY UPDATE
                                descripcion=VALUES(descripcion),
                                cantidad_picking=VALUES(cantidad_picking),
                                capturado_por=VALUES(capturado_por)""",
                        (tido, nudo, it["sku"], it["descripcion"],
                         it["cantidad_picking"], usuario),
                    )
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[logistica_cuadratura] error guardando picking {tido}/{nudo}: {e}",
                  flush=True)
            return jsonify({"ok": False,
                            "error": "No se pudo guardar el conteo de picking."}), 500

        # Audit log (Regla #5): quien capturo cuantos SKUs para que documento.
        try:
            _audit = _h("_audit")
            if callable(_audit):
                _audit(
                    "cuadratura_picking_guardado",
                    target_type="documento_erp",
                    target_id=f"{tido}-{nudo}",
                    details={
                        "tido": tido, "nudo": nudo,
                        "items_guardados": len(validos),
                        "items_descartados": len(descartados),
                    },
                )
        except Exception as e_aud:
            print(f"[logistica_cuadratura] audit picking fallo (no critico): {e_aud}", flush=True)

        data, status = _armar_cuadratura(tido, nudo)
        if isinstance(data, dict):
            data["items_guardados"] = len(validos)
            data["descartados"] = descartados
        return jsonify(data), status

    # ── Schema: se crea SIEMPRE al registrar (Regla #5 — ILUS_SKIP_MIGRATIONS=1
    #    en prod). Nunca revienta el boot.
    try:
        with app.app_context():
            _ensure_cuadratura_schema()
    except Exception as e:
        print(f"[ILUS][WARN] _ensure_cuadratura_schema: {e}", flush=True)

    return app
