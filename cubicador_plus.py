"""
Modulo CUBICADOR PLUS — backend de las pestanas nuevas del Cubicador.

Vive en un archivo APARTE (no en app.py) por dos razones:
  1) app.py tiene ~88.000 lineas y hay otra sesion editandolo en paralelo.
  2) Todo lo que hay aca es AGREGADO: no toca ni redefine nada del Cubicador
     actual (Regla #4.2). En particular NO importa ni modifica las funciones
     blindadas `_cubicador_fetch`, `cubicador_export_excel` ni
     `_cubicador_pdf_response_ilus`; solo LEE datos llamandolas tal cual.

Se registra desde app.py con 3 lineas (las agrega OTRO agente):

    try:
        from cubicador_plus import register_cubicador_plus
        register_cubicador_plus(app)
        print("[ILUS] Modulo Cubicador Plus registrado.")
    except Exception as _cubplus_reg_err:
        print(f"[ILUS][WARN] register_cubicador_plus: {_cubplus_reg_err}")

RUTAS QUE EXPONE (todas con el gate del Cubicador: login + g.permissions['cubicador'])

    GET   /cubicador/api/producto/<pid>/medidas       -> bultos actuales (precarga del modal)
    POST  /cubicador/api/producto/<pid>/medidas       -> guarda bultos CON RED DE SEGURIDAD
    POST  /cubicador/api/producto/por-sku/medidas     -> idem, resolviendo/creando por SKU
    POST  /cubicador/api/cotizar                      -> envoltorio del comparador de couriers
    GET   /cubicador/api/packing-list/<tido>/<nudo>   -> lineas del documento (sku/descripcion/cantidad),
                                                          POR PALLET, sin campos de aduana
    GET   /cubicador/packing-list/<tido>/<nudo>       -> LA HOJA A4 imprimible (HTML, pestana nueva)

POR QUE EXISTE EL ENDPOINT DE MEDIDAS SI YA HAY /transporte/api/inline-bulto
---------------------------------------------------------------------------
`tr_inline_bulto` (app.py ~21415) hace `DELETE FROM app_bultos WHERE product_id=%s`
y reinserta lo que le llegue. Si el modal se abre vacio (o falla la precarga) y el
usuario guarda, un producto con 27 bultos queda con 1 — sin aviso y sin vuelta atras.
Este modulo NO reemplaza ni modifica ese endpoint (sigue vivo e intacto, Regla #4.2),
pero el Cubicador nuevo usa ESTE, que:
  - rechaza (400) cualquier guardado que quede en 0 bultos validos,
  - exige `confirmar_reduccion: true` (409) si el guardado deja MENOS bultos
    de los que el producto ya tenia,
  - reporta explicitamente los bultos DESCARTADOS por incompletos/invalidos
    (hoy `tr_inline_bulto` los descarta en silencio).

PACKING LIST POR PALLET — CORRECCION 2026-07-25 (spec real, no aduana)
-----------------------------------------------------------------------
La primera version de este modulo asumio una spec de ADUANA (bulto a bulto,
con marca/pais de origen/partida arancelaria/peso neto). Esa spec era
incorrecta: se verifico contra los 3 documentos reales de Daniel (PL_COLOMBIA.pdf
y los 2 Excel "PACKING LIST CHILE...") y el packing list real de ILUS es POR
PALLET, sin ninguno de esos 4 campos. Ver
`SPEC_PACKING_LIST.md` (scratchpad de la investigacion) para el detalle linea
por linea.

Lo que SI sale del sistema (una fila por LINEA del documento, no por bulto):
    sku, descripcion, cantidad          -> ERP, via `_cubicador_fetch`
Lo que NO existe en ninguna tabla y se arma 100% A MANO en bodega:
    Nro. Pallet    (que SKU/linea va en que pallet)
    Medidas        (largo x ancho x alto del PALLET armado, no del producto)
    Kilo           (peso del pallet completo)
    Tipo Embalaje  (Carton / Woden Box / Woden Box and Carton / otro)
    Volumen (Mts3) -> se CALCULA solo (largo*ancho*alto/1.000.000), nunca se
                      escribe a mano.

DECISION DE PERSISTENCIA (documentada, no hay tabla nueva por ahora): la
agrupacion de pallets se guarda en `localStorage` del navegador, en la MISMA
clave que usan tanto `_tab_packing.html` (el "armador de pallets" dentro del
Cubicador) como `packing_list.html` (la hoja imprimible) — asi lo armado en
una pestana aparece ya cargado en la otra, siempre que sea el mismo navegador.
Es el MISMO patron que esta pagina ya usaba para el resto de los campos
manuales (ver `LS_KEY` en packing_list.html) — no se inventa un patron nuevo.
Se eligio esto y no una tabla MySQL porque: (a) es un dato de una sola persona
armando UN embarque puntual, no algo que varios usuarios deban ver a la vez;
(b) evita otro `_ensure_*` con su propio riesgo de migracion en produccion
para un dato que hoy nadie pidio que sobreviva entre computadores. Si mas
adelante Daniel pide que la asignacion de pallets se comparta entre usuarios
o alimente la hoja de "Cuadratura", ahi si conviene una tabla
(`transport_pallets` / `transport_pallet_items`, con su `_ensure_*` idempotente
— Regla #5). Por ahora: NO se agrega tabla nueva.

Autor: agente Claude — 2026-07-24. Reglas aplicadas: #1 (sin alert/confirm en el
front que consuma esto), #2 (paleta), #3 (mobile), #4 (SQL parametrizado, sin
detalles internos al cliente), #4.2 (solo agrega), #5 (schema idempotente /
columnas verificadas contra el CREATE TABLE), #6 (horas Chile via filtros Jinja).
"""

import sys
import json
import traceback
from functools import wraps

from flask import request, jsonify, g, flash, redirect, url_for, render_template

# ──────────────────────────────────────────────────────────────────────
#  Acceso a los helpers de app.py — IMPORT DIFERIDO
#  ---------------------------------------------------------------
#  No podemos hacer `from app import mysql_fetchall` arriba: cuando app.py
#  llama a register_cubicador_plus() todavia se esta ejecutando a si mismo,
#  y un import directo crearia una dependencia circular (o traeria un modulo
#  a medio construir). Por eso resolvemos cada helper con getattr() EN TIEMPO
#  DE REQUEST, cuando app.py ya termino de cargar. Mismo espiritu que
#  tickets_module.py (que lo resuelve via ctx = globals() de app.py).
# ──────────────────────────────────────────────────────────────────────

# Nombre del modulo donde vive la Flask app. Se fija en register_cubicador_plus()
# a partir de app.import_name: asi funciona tanto con gunicorn (`app:app`, el
# modulo se llama "app") como ejecutando `python app.py` (se llama "__main__").
_APP_MODULE_NAME = "app"


def _appmod():
    """Modulo Python donde esta definida la Flask app (normalmente `app`)."""
    mod = sys.modules.get(_APP_MODULE_NAME)
    if mod is None:
        mod = sys.modules.get("app") or sys.modules.get("__main__")
    return mod


def _h(nombre, default=None):
    """Devuelve el helper `nombre` de app.py, o `default` si no existe."""
    mod = _appmod()
    if mod is None:
        return default
    return getattr(mod, nombre, default)


def _tabla(nombre_const, fallback):
    """Nombre real de una tabla (viene de MYSQL_CONFIG en app.py)."""
    val = _h(nombre_const)
    return val if isinstance(val, str) and val.strip() else fallback


# ──────────────────────────────────────────────────────────────────────
#  Utilidades numericas
# ──────────────────────────────────────────────────────────────────────

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
            # 1.234,56 -> 1234.56
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


# Peso volumetrico: el Cubicador usa el divisor 4000 (cm3 -> kg). Mismo valor
# que app.calc_pv() y que la query de _cubicador_fetch. NO cambiar sin avisar:
# cambia todas las cotizaciones de courier.
_DIVISOR_VOLUMETRICO = 4000.0

# Topes de cordura para las medidas. No son limites del ERP ni de la BD
# (DECIMAL(12,2) aguanta mucho mas): son para atajar el error tipico de tipear
# milimetros donde van centimetros (1200 en vez de 120) o gramos donde van kilos.
_MAX_CM = 2000.0     # 20 metros
_MAX_KG = 5000.0     # 5 toneladas por bulto


# ──────────────────────────────────────────────────────────────────────
#  Gate de permiso + blindaje de errores
# ──────────────────────────────────────────────────────────────────────

def _cub_api(fn):
    """Decorador para los endpoints JSON del Cubicador.

    Replica EXACTAMENTE el patron ya usado en app.py (no hay decorador
    dedicado para el Cubicador): @login_required + check inline sobre
    g.permissions['cubicador'] devolviendo 403 JSON.

    Ademas envuelve todo en try/except: ningun endpoint puede propagar una
    excepcion al cliente (Regla #4). El detalle va al log del servidor; al
    navegador solo llega un mensaje amigable.
    """
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
            print(f"[cubicador_plus] fallo leyendo permisos: {e_perm}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo validar tu sesion."}), 403

        try:
            return fn(*args, **kwargs)
        except Exception as e:
            # Log detallado al servidor, mensaje generico al cliente (Regla #4).
            print(f"[cubicador_plus][ERROR] {fn.__name__}: {e}", flush=True)
            try:
                print(traceback.format_exc(), flush=True)
            except Exception:
                pass
            return jsonify({
                "ok": False,
                "error": "Ocurrio un problema procesando la solicitud. "
                         "Intenta nuevamente; si persiste avisa a soporte.",
            }), 500

    # login_required se aplica POR FUERA (igual que en tickets_module.py), asi
    # un usuario sin sesion nunca llega al check de permisos.
    login_required = _h("login_required")
    return login_required(wrapped) if callable(login_required) else wrapped


def _cub_page(fn):
    """Gate para las PAGINAS HTML del Cubicador Plus.

    Mismo criterio que la vista `cubicador()` de app.py (15366-15371): si el
    usuario no tiene el permiso, flash + redirect al home — NO un JSON, que en
    una pestana nueva se veria como texto crudo.
    """
    @wraps(fn)
    def wrapped(*args, **kwargs):
        permisos = getattr(g, "permissions", None) or {}
        if not permisos.get("cubicador"):
            flash("No tienes acceso al modulo Cubicador.", "danger")
            return redirect(url_for("index"))
        return fn(*args, **kwargs)

    login_required = _h("login_required")
    return login_required(wrapped) if callable(login_required) else wrapped


# ══════════════════════════════════════════════════════════════════════
#  1) MEDIDAS DE UN PRODUCTO (bultos)
# ══════════════════════════════════════════════════════════════════════

def _leer_bultos(pid):
    """Bultos actuales del producto, con peso volumetrico calculado.

    Columnas verificadas contra el CREATE TABLE de app_bultos (app.py:2210):
        id, product_id, bulto_num, largo, ancho, alto, peso
    (NO hay columna `peso_vol` en la tabla — se calcula aqui, igual que
    app.enrich()/app.calc_pv()).
    """
    mysql_fetchall = _h("mysql_fetchall")
    BULTOS_TABLE = _tabla("BULTOS_TABLE", "app_bultos")

    rows = mysql_fetchall(
        f"SELECT id, bulto_num, largo, ancho, alto, peso "
        f"FROM `{BULTOS_TABLE}` WHERE product_id=%s ORDER BY bulto_num",
        (pid,),
    ) or []

    out = []
    for r in rows:
        largo, ancho, alto = _f(r["largo"]), _f(r["ancho"]), _f(r["alto"])
        peso = _f(r["peso"])
        vol_cm3 = largo * ancho * alto
        out.append({
            "id":        r["id"],
            "num":       int(r["bulto_num"]),
            "bulto_num": int(r["bulto_num"]),   # alias: el front del Cubicador usa 'num'
            "largo":     _r(largo),
            "ancho":     _r(ancho),
            "alto":      _r(alto),
            "peso":      _r(peso),
            "peso_vol":  _r(vol_cm3 / _DIVISOR_VOLUMETRICO),
            "volumen_cm3": _r(vol_cm3),
            "volumen_m3":  _r(vol_cm3 / 1_000_000.0, 6),
        })
    return out


def _totales_bultos(bultos, cantidad=None):
    """Totales unitarios (por 1 unidad del producto) y, si se pasa `cantidad`,
    los totales de la LINEA del documento — que es lo que muestra la tabla del
    Cubicador en data-kg-tot / data-pv-tot / data-vol-tot / data-pred-tot."""
    peso_kg = sum(_f(b.get("peso")) for b in bultos)
    vol_cm3 = sum(_f(b.get("largo")) * _f(b.get("ancho")) * _f(b.get("alto")) for b in bultos)
    peso_vol = vol_cm3 / _DIVISOR_VOLUMETRICO
    pred = max(peso_kg, peso_vol)

    out = {
        "total_bultos": len(bultos),
        "peso_kg":      _r(peso_kg, 4),
        "peso_vol":     _r(peso_vol, 4),
        "pred":         _r(pred, 4),
        # Alias explicitos "por unidad" (mismos nombres que usa _cubicador_fetch)
        "peso_kg_u":    _r(peso_kg, 4),
        "peso_vol_u":   _r(peso_vol, 4),
        "vol_u":        _r(vol_cm3, 2),
        "pred_u":       _r(pred, 4),
        "volumen_cm3":  _r(vol_cm3, 2),
        "volumen_m3":   _r(vol_cm3 / 1_000_000.0, 6),
    }

    if cantidad is not None:
        qty = _f(cantidad, 0.0)
        if qty > 0:
            out.update({
                "cantidad":     qty,
                "peso_kg_tot":  _r(peso_kg * qty, 4),
                "peso_vol_tot": _r(peso_vol * qty, 4),
                "vol_tot":      _r(vol_cm3 * qty, 2),
                "pred_tot":     _r(pred * qty, 4),
            })
    return out


def _normalizar_bultos_entrantes(bultos_raw):
    """Valida los bultos recibidos.

    Devuelve (validos, descartados). A DIFERENCIA de tr_inline_bulto, los
    descartes se REPORTAN (con el motivo) en vez de desaparecer en silencio:
    ese silencio es justamente lo que hace peligroso guardar desde un modal.
    """
    validos, descartados = [], []

    if not isinstance(bultos_raw, (list, tuple)):
        return validos, [{
            "posicion": None,
            "motivo": "El campo 'bultos' debe ser una lista.",
            "datos": None,
        }]

    for idx, b in enumerate(bultos_raw, start=1):
        if not isinstance(b, dict):
            descartados.append({
                "posicion": idx,
                "motivo": "Formato invalido (se esperaba un objeto con largo/ancho/alto/peso).",
                "datos": None,
            })
            continue

        largo = _f(b.get("largo"))
        ancho = _f(b.get("ancho"))
        alto  = _f(b.get("alto"))
        peso  = _f(b.get("peso"))
        datos = {"largo": largo, "ancho": ancho, "alto": alto, "peso": peso}

        faltantes = [nombre for nombre, val in
                     (("largo", largo), ("ancho", ancho), ("alto", alto), ("peso", peso))
                     if val <= 0]
        if faltantes:
            descartados.append({
                "posicion": idx,
                "motivo": "Faltan o van en cero: " + ", ".join(faltantes) + ".",
                "datos": datos,
            })
            continue

        fuera_rango = []
        for nombre, val in (("largo", largo), ("ancho", ancho), ("alto", alto)):
            if val > _MAX_CM:
                fuera_rango.append(f"{nombre}={val:g} cm (maximo {_MAX_CM:g} cm)")
        if peso > _MAX_KG:
            fuera_rango.append(f"peso={peso:g} kg (maximo {_MAX_KG:g} kg)")
        if fuera_rango:
            descartados.append({
                "posicion": idx,
                "motivo": "Valor fuera de rango — revisa las unidades (cm y kg): "
                          + "; ".join(fuera_rango) + ".",
                "datos": datos,
            })
            continue

        validos.append({
            "largo": _r(largo), "ancho": _r(ancho),
            "alto":  _r(alto),  "peso":  _r(peso),
        })

    return validos, descartados


def _guardar_medidas(pid, payload):
    """Nucleo compartido por los dos endpoints de guardado (por pid y por SKU).

    Devuelve (dict_respuesta, status_http). Nunca lanza: el decorador de arriba
    igual la protege, pero aca ya devolvemos errores de negocio bien tipados.
    """
    mysql_fetchone = _h("mysql_fetchone")
    get_db         = _h("get_db")
    PRODUCTS_TABLE = _tabla("PRODUCTS_TABLE", "app_products")
    BULTOS_TABLE   = _tabla("BULTOS_TABLE", "app_bultos")

    # Columnas verificadas contra CREATE TABLE app_products (app.py:2196):
    # id, sku, nombre, estado, codigo, erp_sync, created_by, updated_by, created_at, updated_at
    prod = mysql_fetchone(
        f"SELECT id, sku, nombre FROM `{PRODUCTS_TABLE}` WHERE id=%s LIMIT 1", (pid,)
    )
    if not prod:
        return {"ok": False, "error": "El producto no existe en la base local.",
                "error_codigo": "NO_EXISTE"}, 404

    bultos_raw = payload.get("bultos")
    if bultos_raw is None:
        # Compat con el formato "campos sueltos" de tr_inline_bulto (1 bulto).
        if payload.get("largo") is not None:
            bultos_raw = [{
                "largo": payload.get("largo"), "ancho": payload.get("ancho"),
                "alto":  payload.get("alto"),  "peso":  payload.get("peso"),
            }]
        else:
            bultos_raw = []

    validos, descartados = _normalizar_bultos_entrantes(bultos_raw)

    # ── RED DE SEGURIDAD 1: nunca dejar el producto en 0 bultos ──────────
    # Guardar "vacio" es exactamente el accidente que este modulo evita: borra
    # la ficha completa sin que nadie se entere. Si de verdad hay que dejarlo
    # sin medidas, eso se hace desde la ficha del producto, no desde aca.
    if not validos:
        detalle = ""
        if descartados:
            detalle = (" Se descartaron " + str(len(descartados)) +
                       " fila(s) incompleta(s) o invalida(s).")
        return {
            "ok": False,
            "error": "No se guardo nada: debes ingresar al menos un bulto con "
                     "largo, ancho, alto y peso." + detalle,
            "error_codigo": "SIN_BULTOS_VALIDOS",
            "descartados": descartados,
            "bultos_guardados": 0,
        }, 400

    # ── RED DE SEGURIDAD 2: reduccion de bultos requiere confirmacion ────
    fila_cnt = mysql_fetchone(
        f"SELECT COUNT(*) AS n FROM `{BULTOS_TABLE}` WHERE product_id=%s", (pid,)
    ) or {}
    actuales = int(fila_cnt.get("n") or 0)
    nuevos = len(validos)
    confirmar = bool(payload.get("confirmar_reduccion"))

    if actuales > nuevos and not confirmar:
        return {
            "ok": False,
            "error": (f"El producto {prod['sku']} tiene {actuales} bulto(s) y estas "
                      f"guardando {nuevos}. Confirma que quieres reducirlo."),
            "error_codigo": "REDUCCION_BULTOS",
            "requiere_confirmacion": True,
            "bultos_actuales": actuales,
            "bultos_nuevos": nuevos,
            "descartados": descartados,
            "sku": prod["sku"],
            # El front debe reenviar el MISMO payload agregando esta clave.
            "campo_confirmacion": "confirmar_reduccion",
        }, 409

    # ── Escritura: DELETE + INSERT dentro de una sola transaccion ────────
    # app_bultos tiene UNIQUE (product_id, bulto_num), asi que la renumeracion
    # 1..N obliga a borrar antes. El commit unico evita dejar el producto a
    # medio escribir si el INSERT falla.
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM `{BULTOS_TABLE}` WHERE product_id=%s", (pid,))
            for idx, b in enumerate(validos, start=1):
                cur.execute(
                    f"INSERT INTO `{BULTOS_TABLE}` "
                    f"(product_id, bulto_num, largo, ancho, alto, peso) "
                    f"VALUES (%s,%s,%s,%s,%s,%s)",
                    (pid, idx, b["largo"], b["ancho"], b["alto"], b["peso"]),
                )
            # Dejar rastro de quien toco la ficha (columna verificada en el
            # CREATE TABLE de app_products).
            current_username = _h("current_username")
            usuario = None
            try:
                usuario = current_username() if callable(current_username) else None
            except Exception:
                usuario = None
            if usuario:
                cur.execute(
                    f"UPDATE `{PRODUCTS_TABLE}` SET updated_by=%s WHERE id=%s",
                    (usuario, pid),
                )
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[cubicador_plus] error guardando bultos pid={pid}: {e}", flush=True)
        return {"ok": False,
                "error": "No se pudieron guardar las medidas. No se modifico nada."}, 500

    # Audit log (Regla #5: toda accion destructiva deja rastro). Best-effort.
    try:
        _audit = _h("_audit")
        if callable(_audit):
            _audit(
                "cubicador_medidas_guardadas",
                target_type="producto",
                target_id=pid,
                details={
                    "sku": prod["sku"],
                    "bultos_antes": actuales,
                    "bultos_despues": nuevos,
                    "reduccion_confirmada": bool(confirmar and actuales > nuevos),
                    "descartados": len(descartados),
                },
            )
    except Exception as e_aud:
        print(f"[cubicador_plus] audit medidas falló (no critico): {e_aud}", flush=True)

    # Invalidar el cache del listado de productos para que la grilla no muestre
    # los pesos viejos. Best-effort: si el helper no existe, seguimos igual.
    try:
        inval = _h("_invalidate_listing_cache")
        if callable(inval):
            inval()
    except Exception:
        pass

    bultos_final = _leer_bultos(pid)
    resp = {
        "ok": True,
        "producto_id": pid,
        "sku": prod["sku"],
        "nombre": prod["nombre"],
        "bultos": bultos_final,
        "bultos_antes": actuales,
        "bultos_guardados": nuevos,
        "descartados": descartados,
        "reduccion_aplicada": bool(actuales > nuevos),
    }
    resp.update(_totales_bultos(bultos_final, payload.get("cantidad")))

    # Aviso legible para mostrar con ilusToast/ilusAlert (Regla #1: el front
    # NUNCA usa alert() nativo — este texto va dentro del helper ILUS).
    avisos = []
    if descartados:
        avisos.append(f"Se descartaron {len(descartados)} fila(s) incompleta(s): "
                      "revisa que largo, ancho, alto y peso esten completos.")
    if actuales > nuevos:
        avisos.append(f"El producto paso de {actuales} a {nuevos} bulto(s).")
    resp["avisos"] = avisos
    return resp, 200


# ══════════════════════════════════════════════════════════════════════
#  2) COTIZADOR — envoltorio del comparador existente
# ══════════════════════════════════════════════════════════════════════

# Nombre del endpoint Flask del comparador ya existente (app.py:17750).
# Al ser `@app.route(...)` sin `endpoint=`, Flask usa el nombre de la funcion.
_ENDPOINT_COMPARADOR = "api_asignar_cotizar_couriers"

# Valores de `fuente` que significan "esto NO es una tarifa contractual".
_FUENTES_ESTIMADAS = {"estimado", "estimada", "estimacion", "estimación", "estimate"}

# NOTA 2026-07-25 (confidencial, no borrar sin indicación de Daniel): Felca y
# Milling tienen una política comercial propia (derivan su precio de FedEx
# Directo con un descuento por comuna — ver transporte_tarifas.py). De cara a
# app.py/cubicador_plus.py/el frontend esos precios llegan con fuente='tabla',
# indistinguibles de una tarifa negociada real — NUNCA se debe reintroducir un
# `fuente=='regla'` (ni similares) que los distinga en el JSON de salida.


def _invocar_comparador(flask_app, payload):
    """Llama a la vista /api/asignar/cotizar-couriers SIN duplicar su logica.

    Por que asi y no un requests.post contra nosotros mismos:
      - El comparador son ~350 lineas de tarifas, brackets, seguro, margen e
        IVA. Copiarlas seria garantizar que en 3 meses den precios distintos.
      - Un HTTP interno tendria que re-autenticarse (cookies de sesion) y
        agregaria latencia y un punto de falla mas.

    La vista lee su body con `request.get_json(silent=True)`. Werkzeug cachea
    ese parseo en `request._cached_json` (tupla (no_silent, silent)); dejando
    ahi el payload YA NORMALIZADO, la vista lo consume como si el navegador lo
    hubiera mandado. Se restaura el valor previo al terminar.

    Devuelve (data_dict, status_code).
    """
    view = None
    try:
        view = flask_app.view_functions.get(_ENDPOINT_COMPARADOR)
    except Exception:
        view = None
    if view is None:
        return {"ok": False,
                "error": "El comparador de couriers no esta disponible en este momento."}, 503

    previo = getattr(request, "_cached_json", None)
    try:
        try:
            request._cached_json = (payload, payload)
        except Exception as e_cache:
            print(f"[cubicador_plus] no se pudo inyectar el payload al comparador: {e_cache}",
                  flush=True)
            return {"ok": False,
                    "error": "No se pudo preparar la consulta de tarifas."}, 500
        rv = view()
    finally:
        try:
            if previo is not None:
                request._cached_json = previo
        except Exception:
            pass

    # Flask permite devolver Response o (Response, status). Normalizamos.
    status = 200
    if isinstance(rv, tuple):
        cuerpo = rv[0]
        if len(rv) > 1:
            try:
                status = int(rv[1])
            except (TypeError, ValueError):
                status = 200
    else:
        cuerpo = rv

    data = None
    if hasattr(cuerpo, "get_json"):
        try:
            data = cuerpo.get_json(silent=True)
        except Exception:
            data = None
        if status == 200:
            try:
                status = int(getattr(cuerpo, "status_code", 200))
            except (TypeError, ValueError):
                status = 200
    elif isinstance(cuerpo, dict):
        data = cuerpo
    elif isinstance(cuerpo, str):
        try:
            data = json.loads(cuerpo)
        except Exception:
            data = None

    if data is None:
        return {"ok": False,
                "error": "El comparador de couriers devolvio una respuesta inesperada."}, 502
    return data, status


def _marcar_estimados(cotizaciones):
    """Agrega `es_estimado` y su motivo a cada cotizacion.

    ESTIMADO (fuente='estimado') = proyeccion sin tabla contractual detras.
    Sirve para orientar, jamas para recomendar automaticamente: si el sistema
    recomienda un estimado y despues el courier cobra otra cosa, el margen se
    lo come ILUS.

    2026-07-25 (confidencial): la politica comercial de Felca/Milling (ver
    transporte_tarifas.py) llega desde app.py con fuente='tabla' — a proposito
    indistinguible de una tarifa negociada real. Esta funcion NO debe volver a
    tratarla como un caso aparte (nada de 'es_regla_comercial'/'motivo_regla'):
    eso es justamente la seña que Daniel pidio eliminar.
    """
    for c in cotizaciones or []:
        if not isinstance(c, dict):
            continue
        fuente = (c.get("fuente") or "").strip().lower()
        trace = c.get("trace") if isinstance(c.get("trace"), dict) else {}
        fuente_trace = (trace.get("fuente") or "").strip().lower()

        es_est = (fuente in _FUENTES_ESTIMADAS) or fuente_trace.startswith("estim")
        c["es_estimado"] = bool(es_est)

        advert = trace.get("advertencias") or []
        detalle = ""
        if isinstance(advert, (list, tuple)) and advert:
            detalle = " " + " ".join(str(a) for a in advert if a)

        if es_est:
            motivo = ("Precio ESTIMADO: no hay tarifa contractual cargada para esta "
                      "comuna/tramo de peso, el valor es una proyeccion." + detalle)
        else:
            motivo = ""
        c["motivo_estimado"] = motivo.strip()

        # Transparencia extra: si la tarifa fue validada contra la macro/Excel.
        c["tarifa_validada"] = bool(trace.get("validado"))
    return cotizaciones


def _precio_comparable(cot):
    """Precio para ordenar. None cuando el courier no entrego un numero usable
    (sin cobertura, error o timeout) — esos NUNCA pueden ser recomendados."""
    for campo in ("subtotal", "precio"):
        val = cot.get(campo)
        if val is None:
            continue
        try:
            num = float(val)
        except (TypeError, ValueError):
            continue
        if num > 0:
            return num
    return None


def _clp(valor):
    """Formatea un monto en pesos chilenos: 126933 -> '$126.933'."""
    try:
        return "$" + f"{int(round(float(valor))):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "$0"


def _recomendar_no_estimado(cotizaciones):
    """Devuelve (recomendado_id, aviso). Solo recomienda tarifas FIRMES.

    FIRME = tarifa de tabla O precio derivado por la regla comercial de ILUS
    (ver _marcar_estimados). Lo unico que queda fuera del recomendado es la
    ESTIMACION sin respaldo.
    """
    con_cobertura = [c for c in (cotizaciones or [])
                     if isinstance(c, dict) and c.get("tiene_cobertura")
                     and _precio_comparable(c) is not None]
    if not con_cobertura:
        return None, "Ningun courier entrego un precio utilizable para esta comuna."

    mas_barato = min(con_cobertura, key=_precio_comparable)
    firmes = [c for c in con_cobertura if not c.get("es_estimado")]

    if not firmes:
        return None, ("Todas las cotizaciones con cobertura son ESTIMADAS (no hay tarifa "
                      "contractual cargada). No se recomienda ninguna automaticamente: "
                      "revisa y confirma el precio con el courier antes de despachar.")

    mejor_firme = min(firmes, key=_precio_comparable)

    if mas_barato is mejor_firme:
        return mejor_firme.get("courier_id"), None

    aviso = (
        f"El precio mas bajo es {mas_barato.get('courier_nombre') or 'un courier'} "
        f"({_clp(_precio_comparable(mas_barato))}) pero es ESTIMADO, no una tarifa "
        f"contractual. Se recomienda {mejor_firme.get('courier_nombre') or 'la alternativa'} "
        f"({_clp(_precio_comparable(mejor_firme))}), que si tiene tarifa cargada."
    )
    return mejor_firme.get("courier_id"), aviso


# ══════════════════════════════════════════════════════════════════════
#  3) PACKING LIST DE EXPORTACION
# ══════════════════════════════════════════════════════════════════════

# Lineas que NO son cajas a embalar. `es_zz` ya lo marca _cubicador_fetch
# (sku.startswith("ZZ")), pero algunos documentos traen descuentos/fletes con
# SKUs propios. Se filtra conservador: prefijos de SKU inequivocos + un set de
# descripciones EXACTAS. Todo lo filtrado se REPORTA en `lineas_excluidas`
# para que nadie descubra a ultima hora que le falta una caja.
_SKU_SERVICIO_PREFIJOS = ("ZZ", "DESC", "DCTO")
_DESC_SERVICIO_EXACTAS = {
    "DESCUENTO", "DESCUENTOS", "DESCUENTO COMERCIAL",
    "FLETE", "ENVIO", "DESPACHO",
    "REDONDEO", "AJUSTE", "ABONO",
    "SERVICIO TECNICO", "MANO DE OBRA", "INSTALACION",
}


def _sin_acentos(txt):
    """MAYUSCULAS sin tildes: 'Envío' -> 'ENVIO'. El ERP mezcla ambas grafias,
    asi que comparar sin normalizar dejaria pasar lineas de servicio."""
    import unicodedata
    try:
        base = unicodedata.normalize("NFKD", str(txt or ""))
        return "".join(ch for ch in base if not unicodedata.combining(ch)).strip().upper()
    except Exception:
        return str(txt or "").strip().upper()

def _es_linea_servicio(sku, descripcion, es_zz):
    """(bool, motivo). Conservador a proposito: ante la duda, la linea SE
    EMBALA (mejor una fila de mas que una caja olvidada en el embarque)."""
    s = _sin_acentos(sku)
    d = _sin_acentos(descripcion)

    if es_zz:
        return True, "Linea de servicio del ERP (SKU con prefijo ZZ)."
    for pref in _SKU_SERVICIO_PREFIJOS:
        if s.startswith(pref):
            return True, f"SKU de servicio/descuento (prefijo {pref})."
    if d in _DESC_SERVICIO_EXACTAS:
        return True, "Descripcion corresponde a un servicio/descuento, no a un producto."
    return False, ""


def _packing_list_filas(lineas):
    """Convierte las lineas del documento en filas del Packing List.

    Contrato correcto (spec por PALLET, ver docstring del modulo): UNA fila
    por LINEA del documento — sin explotar por bulto y sin campos de aduana.
    Las medidas/volumen/kilo/tipo de embalaje YA NO se calculan aqui: son del
    PALLET armado en bodega (varias lineas pueden compartir un pallet), un
    dato 100% manual que el front arma y persiste (ver `_tab_packing.html` /
    `packing_list.html`). Esta funcion NO toca `app_bultos`: esa tabla mide
    CAJAS de producto, no pallets de embarque, y mezclar ambos conceptos fue
    justamente el error de la spec de aduana anterior.
    """
    filas, excluidas = [], []
    n_linea = 0

    for l in lineas:
        sku = (l.get("sku") or "").strip().upper()
        desc = (l.get("descripcion_erp") or l.get("nombre_app") or "").strip()
        qty = _f(l.get("cantidad"), 0.0)

        if not sku:
            continue

        servicio, motivo = _es_linea_servicio(sku, desc, bool(l.get("es_zz")))
        if servicio:
            excluidas.append({"sku": sku, "descripcion": desc,
                              "cantidad": qty, "motivo": motivo})
            continue

        n_linea += 1
        filas.append({
            "linea":       n_linea,
            "sku":         sku,
            "descripcion": desc or sku,
            "nombre_app":  (l.get("nombre_app") or "").strip(),
            "cantidad":    qty,
            "producto_id": l.get("app_id"),
        })

    return filas, excluidas


# ══════════════════════════════════════════════════════════════════════
#  SCHEMA — verificacion idempotente al boot (Regla #5)
# ══════════════════════════════════════════════════════════════════════

def _ensure_cubicador_plus_schema():
    """Verifica (NO altera) que existan las tablas/columnas que este modulo lee.

    Produccion corre con ILUS_SKIP_MIGRATIONS=1, asi que cualquier schema nuevo
    tiene que garantizarse en un _ensure_* que corra SIEMPRE al boot. Este
    modulo NO crea tablas propias: reusa app_products y app_bultos, que son de
    las mas antiguas del sistema y estan blindadas. Por eso aca solo VERIFICAMOS
    y avisamos por log — un ALTER sobre esas tablas seria mas riesgoso que util.

    Si algun dia se agregan los campos de aduana (marca / pais_origen /
    peso_neto_kg / partida_arancelaria), el CREATE TABLE idempotente va aca.
    """
    mysql_fetchone = _h("mysql_fetchone")
    if not callable(mysql_fetchone):
        print("[cubicador_plus][WARN] mysql_fetchone no disponible: se omite la verificacion.",
              flush=True)
        return

    requeridas = {
        _tabla("PRODUCTS_TABLE", "app_products"): ("id", "sku", "nombre", "updated_by"),
        _tabla("BULTOS_TABLE", "app_bultos"): ("id", "product_id", "bulto_num",
                                               "largo", "ancho", "alto", "peso"),
    }
    for tabla, columnas in requeridas.items():
        for col in columnas:
            row = mysql_fetchone(
                "SELECT COUNT(*) AS n FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME=%s AND COLUMN_NAME=%s",
                (tabla, col),
            ) or {}
            if not int(row.get("n") or 0):
                print(f"[cubicador_plus][WARN] falta {tabla}.{col} — "
                      f"las medidas del Cubicador pueden fallar.", flush=True)


# ══════════════════════════════════════════════════════════════════════
#  REGISTRO DE RUTAS
# ══════════════════════════════════════════════════════════════════════

def register_cubicador_plus(app, ctx=None):
    """Engancha las rutas del Cubicador Plus a la Flask app de app.py.

    `ctx` (opcional) acepta el globals() de app.py por compatibilidad con el
    patron de tickets_module.py, pero no es necesario: los helpers se resuelven
    por getattr sobre el modulo, en tiempo de request.
    """
    global _APP_MODULE_NAME
    try:
        nombre_mod = getattr(app, "import_name", None)
        if nombre_mod and nombre_mod in sys.modules:
            _APP_MODULE_NAME = nombre_mod
    except Exception:
        pass

    # ──────────────────────────────────────────────────────────────
    #  GET /cubicador/api/producto/<pid>/medidas — precarga del modal
    # ──────────────────────────────────────────────────────────────
    @app.route("/cubicador/api/producto/<int:pid>/medidas", methods=["GET"])
    @_cub_api
    def cubicador_plus_get_medidas(pid):
        """Bultos ACTUALES del producto, para precargar el modal de medidas.

        NO reusa /products/<pid>/quick a proposito: ese endpoint exige el
        permiso 'view', que NO esta implicado por 'cubicador' (app.py:3690-3695).
        Un rol de transporte con solo la accion 'cubicador' recibiria 403 justo
        al abrir el modal — es decir, el modal se abriria VACIO, que es
        exactamente el escenario que hace peligroso guardar. Por eso leemos
        directo de la BD con el gate del Cubicador.
        """
        mysql_fetchone = _h("mysql_fetchone")
        PRODUCTS_TABLE = _tabla("PRODUCTS_TABLE", "app_products")

        prod = mysql_fetchone(
            f"SELECT id, sku, nombre, estado, codigo FROM `{PRODUCTS_TABLE}` "
            f"WHERE id=%s LIMIT 1", (pid,)
        )
        if not prod:
            return jsonify({"ok": False, "error": "El producto no existe en la base local.",
                            "error_codigo": "NO_EXISTE"}), 404

        bultos = _leer_bultos(pid)
        cantidad = request.args.get("cantidad")
        resp = {
            "ok": True,
            "producto_id": prod["id"],
            "sku": prod["sku"],
            "nombre": prod["nombre"],
            "estado": prod.get("estado") or "",
            "codigo": prod.get("codigo") or "",
            "bultos": bultos,
            "max_bultos": int(_h("MAX_BULTOS", 27) or 27),
        }
        resp.update(_totales_bultos(bultos, cantidad))
        return jsonify(resp)

    # ──────────────────────────────────────────────────────────────
    #  POST /cubicador/api/producto/<pid>/medidas — guardar con red
    # ──────────────────────────────────────────────────────────────
    @app.route("/cubicador/api/producto/<int:pid>/medidas", methods=["POST"])
    @_cub_api
    def cubicador_plus_post_medidas(pid):
        """Guarda los bultos de un producto desde la tabla del Cubicador.

        Body JSON:
            {
              "bultos": [{"largo":120,"ancho":60,"alto":40,"peso":25.5}, ...],
              "cantidad": 3,                  # opcional: para devolver los totales de la linea
              "confirmar_reduccion": false    # obligatorio en true si se bajan bultos
            }

        Respuestas:
            200 -> {ok:true, bultos:[...], peso_kg, peso_vol, pred, descartados:[...], avisos:[...]}
            400 -> SIN_BULTOS_VALIDOS (no se toco nada)
            404 -> NO_EXISTE
            409 -> REDUCCION_BULTOS (reenviar con confirmar_reduccion:true)
        """
        payload = request.get_json(silent=True) or {}
        data, status = _guardar_medidas(pid, payload)
        return jsonify(data), status

    # ──────────────────────────────────────────────────────────────
    #  POST /cubicador/api/producto/por-sku/medidas
    # ──────────────────────────────────────────────────────────────
    @app.route("/cubicador/api/producto/por-sku/medidas", methods=["POST"])
    @_cub_api
    def cubicador_plus_post_medidas_por_sku():
        """Igual que el anterior pero resolviendo el producto por SKU.

        Existe porque las lineas del Cubicador SIN ficha local traen
        `app_id = None` (app.py:15166) — justamente las que mas necesitan que
        les carguen medidas. Si el SKU no existe en app_products se crea (mismo
        comportamiento que tr_inline_bulto), y desde ahi aplican las MISMAS
        redes de seguridad que el endpoint por pid.

        Body JSON: {"sku":"ABC-123", "nombre":"opcional", "bultos":[...], ...}
        """
        payload = request.get_json(silent=True) or {}
        sku = (payload.get("sku") or "").strip().upper()
        if not sku:
            return jsonify({"ok": False, "error": "Falta el SKU del producto.",
                            "error_codigo": "SIN_SKU"}), 400

        mysql_fetchone = _h("mysql_fetchone")
        get_db = _h("get_db")
        PRODUCTS_TABLE = _tabla("PRODUCTS_TABLE", "app_products")

        prod = mysql_fetchone(
            f"SELECT id FROM `{PRODUCTS_TABLE}` WHERE UPPER(TRIM(sku))=%s LIMIT 1", (sku,)
        )
        if prod:
            pid = prod["id"]
            creado = False
        else:
            # Solo creamos si de verdad vienen medidas validas: asi un POST
            # vacio nunca deja productos fantasma en el catalogo.
            validos, _desc = _normalizar_bultos_entrantes(payload.get("bultos") or [])
            if not validos:
                return jsonify({
                    "ok": False,
                    "error": "No se guardo nada: debes ingresar al menos un bulto con "
                             "largo, ancho, alto y peso.",
                    "error_codigo": "SIN_BULTOS_VALIDOS",
                    "bultos_guardados": 0,
                }), 400
            nombre = (payload.get("nombre") or "").strip() or sku
            current_username = _h("current_username")
            try:
                usuario = current_username() if callable(current_username) else None
            except Exception:
                usuario = None
            conn = get_db()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"INSERT INTO `{PRODUCTS_TABLE}` "
                        f"(sku, nombre, estado, created_by, updated_by) "
                        f"VALUES (%s,%s,'activo',%s,%s)",
                        (sku, nombre, usuario, usuario),
                    )
                    pid = cur.lastrowid
                conn.commit()
                creado = True
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                print(f"[cubicador_plus] error creando producto {sku}: {e}", flush=True)
                return jsonify({"ok": False,
                                "error": "No se pudo crear la ficha del producto."}), 500

        data, status = _guardar_medidas(pid, payload)
        if isinstance(data, dict):
            data["producto_creado"] = bool(creado)
        return jsonify(data), status

    def _cub_api_cotizar(fn):
        """Variante de _cub_api SOLO para /cubicador/api/cotizar.

        BUG reportado por Daniel 2026-07-26 ("el cotizar envío no sale sin
        tarifa"): este endpoint también lo llama el wizard de Cotizaciones
        de Transporte y Distribución (_modal_cotizacion_logistica.html,
        botón "Cotizar flete" -> epCotizarFlete), pero _cub_api exige
        ESTRICTAMENTE g.permissions['cubicador']. Un usuario con permiso
        'transporte'/'logistica_cotizaciones' (que SÍ puede abrir ese
        wizard, gateado por _lc_tiene_permiso en logistica_cotizaciones.py)
        pero SIN el permiso 'cubicador' recibía un 403 "Sin permiso"
        silencioso — el wizard solo mostraba un hint pequeño de advertencia,
        que en la práctica se veía como "no cotiza nada".
        Fix: para ESTE endpoint puntual, se acepta cubicador O transporte O
        logistica_cotizaciones O superadmin (mismo OR que ya usa
        logistica_cotizaciones._lc_tiene_permiso). Los otros 4 endpoints del
        Cubicador siguen exigiendo SOLO 'cubicador' vía @_cub_api — no se
        tocó su gate.
        """
        @wraps(fn)
        def wrapped(*args, **kwargs):
            try:
                permisos = getattr(g, "permissions", None) or {}
                if not (permisos.get("cubicador") or permisos.get("transporte")
                        or permisos.get("logistica_cotizaciones") or permisos.get("superadmin")):
                    return jsonify({
                        "ok": False,
                        "error": "No tienes acceso para cotizar flete.",
                        "error_codigo": "SIN_PERMISO",
                    }), 403
            except Exception as e_perm:
                print(f"[cubicador_plus] fallo leyendo permisos (cotizar): {e_perm}", flush=True)
                return jsonify({"ok": False, "error": "No se pudo validar tu sesion."}), 403
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                print(f"[cubicador_plus][ERROR] {fn.__name__}: {e}", flush=True)
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

    # ──────────────────────────────────────────────────────────────
    #  POST /cubicador/api/cotizar — envoltorio del comparador
    # ──────────────────────────────────────────────────────────────
    @app.route("/cubicador/api/cotizar", methods=["POST"])
    @_cub_api_cotizar
    def cubicador_plus_cotizar():
        """Cotiza couriers desde la pestana del Cubicador.

        Body JSON:
            {
              "cliente": "Sportlife SpA",          # informativo, se devuelve tal cual
              "comuna": "Lo Barnechea",            # obligatoria (o deducible de documentos)
              "peso_kg": 173.89,
              "peso_pred_kg": 180.0,               # opcional (peso predominante)
              "valor_neto": 1571566,
              "es_residencial": false,
              "documentos": [{"tido":"FCV","nudo":"8598"}, ...]   # opcional
            }

        Devuelve lo MISMO que /api/asignar/cotizar-couriers mas, por courier:
            es_estimado        (bool) — proyeccion sin tabla NI regla detras
            motivo_estimado    (str)  — por que, en castellano
            es_regla_comercial (bool) — precio derivado por la politica de ILUS
                                        (Felca −10% / Milling −5% sobre FedEx).
                                        Es FIRME: cuenta como recomendable.
            motivo_regla       (str)  — explicacion del precio derivado
            tarifa_validada    (bool) — si la tarifa fue validada contra la macro
        y a nivel raiz:
            recomendado_id                — SIEMPRE una tarifa NO estimada (o null)
            recomendado_id_comparador     — lo que habria recomendado el comparador
            aviso_recomendacion           — texto para mostrar con ilusAlert/ilusToast
            hay_estimados                 — bool
        """
        payload = request.get_json(silent=True) or {}

        cliente = (payload.get("cliente") or "").strip()
        comuna = (payload.get("comuna") or "").strip()
        peso_kg = _f(payload.get("peso_kg"))
        peso_pred = _f(payload.get("peso_pred_kg") or payload.get("peso_pred") or peso_kg)
        valor_neto = _f(payload.get("valor_neto"))
        es_residencial = bool(payload.get("es_residencial", False))

        # ── Documentos (opcional) ────────────────────────────────
        # Solo se usan para (a) el audit trail del comparador y (b) rellenar
        # comuna/valor_neto si el front no los mando. NO se re-cotiza por doc.
        documentos = []
        for d in (payload.get("documentos") or []):
            if isinstance(d, dict):
                tido = (d.get("tido") or "").strip().upper()
                nudo = str(d.get("nudo") or "").strip()
                if nudo:
                    documentos.append({"tido": tido or "FCV", "nudo": nudo})

        aviso_documento = None
        if documentos and (not comuna or valor_neto <= 0):
            # Best-effort: leemos el PRIMER documento para completar los huecos.
            # _cubicador_fetch esta blindada; aca solo se LLAMA, no se toca.
            try:
                _cubicador_fetch = _h("_cubicador_fetch")
                if callable(_cubicador_fetch):
                    hdr, _lin = _cubicador_fetch(documentos[0]["tido"],
                                                 documentos[0]["nudo"], fast=True)
                    if hdr:
                        if not comuna:
                            comuna = (hdr.get("comuna") or "").strip()
                        if valor_neto <= 0:
                            valor_neto = _f(hdr.get("valor_neto"))
                        if not cliente:
                            cliente = (hdr.get("cliente_nombre") or "").strip()
            except Exception as e_doc:
                print(f"[cubicador_plus] no se pudo leer el documento para completar "
                      f"la cotizacion: {e_doc}", flush=True)
                aviso_documento = ("No se pudo leer el documento en el ERP para completar "
                                   "la comuna: verifica los datos manualmente.")

        if not comuna:
            return jsonify({
                "ok": False,
                "error": "Falta la comuna de destino: sin ella no se puede cotizar.",
                "error_codigo": "SIN_COMUNA",
            }), 400

        if peso_kg <= 0 and peso_pred <= 0:
            return jsonify({
                "ok": True,
                "cotizaciones": [],
                "recomendado_id": None,
                "recomendado_id_comparador": None,
                "hay_estimados": False,
                "peso_facturable": 0,
                "comuna_resuelta": comuna,
                "cliente": cliente,
                "aviso_recomendacion": None,
                "mensaje": "Sin datos de peso ni cubicaje: carga las medidas de los "
                           "productos antes de cotizar.",
            }), 200

        payload_comparador = {
            "peso_kg":        peso_kg,
            "peso_pred_kg":   peso_pred,
            "comuna":         comuna,
            "es_residencial": es_residencial,
            "valor_neto":     valor_neto,
        }
        if documentos:
            payload_comparador["tido"] = documentos[0]["tido"]
            payload_comparador["nudo"] = documentos[0]["nudo"]

        data, status = _invocar_comparador(app, payload_comparador)

        if status != 200 or not isinstance(data, dict) or not data.get("ok"):
            mensaje = "No se pudieron obtener las tarifas de los couriers."
            if isinstance(data, dict):
                mensaje = data.get("error") or data.get("mensaje") or mensaje
            return jsonify({"ok": False, "error": mensaje}), (status if status >= 400 else 502)

        cotizaciones = data.get("cotizaciones") or []
        _marcar_estimados(cotizaciones)
        recomendado_id, aviso = _recomendar_no_estimado(cotizaciones)

        data["recomendado_id_comparador"] = data.get("recomendado_id")
        data["recomendado_id"] = recomendado_id
        data["aviso_recomendacion"] = aviso
        data["hay_estimados"] = any(c.get("es_estimado") for c in cotizaciones
                                    if isinstance(c, dict))
        data["cliente"] = cliente
        data["documentos"] = documentos
        if aviso_documento:
            data["aviso_documento"] = aviso_documento

        return jsonify(data), 200

    # ──────────────────────────────────────────────────────────────
    #  GET /cubicador/packing-list/<tido>/<nudo>  — LA HOJA (HTML)
    # ──────────────────────────────────────────────────────────────
    @app.route("/cubicador/packing-list/<string:tido>/<path:nudo>", methods=["GET"])
    @_cub_page
    def cubicador_plus_packing_list_page(tido, nudo):
        """Hoja A4 imprimible del packing list.

        Es la URL que abre el boton "Generar Packing List" de la pestana
        (templates/cubicador/_tab_packing.html:49). Sin esta ruta el boton daba
        404: existia el endpoint JSON y existia la plantilla, pero nadie
        renderizaba la pagina.

        La hoja pide sus propios datos a
        GET /cubicador/api/packing-list/<tido>/<nudo> (mismo gate de permiso),
        por eso aca NO se consulta el ERP: abrir la pestana es instantaneo y un
        ERP lento no deja la pagina en blanco, sino con su propio mensaje.
        """
        tido = (tido or "").strip().upper()
        nudo = (nudo or "").strip()

        logo_url = ""
        try:
            _logo = _h("_logo_data_url")
            if callable(_logo):
                logo_url = _logo() or ""
        except Exception as e_logo:
            print(f"[cubicador_plus] logo del packing list no disponible: {e_logo}", flush=True)

        exportador = _h("ILUS_REMITENTE") or None

        try:
            api_url = url_for("cubicador_plus_packing_list", tido=tido, nudo=nudo)
        except Exception:
            api_url = f"/cubicador/api/packing-list/{tido}/{nudo}"
        try:
            volver_url = url_for("cubicador")
        except Exception:
            volver_url = "/cubicador"

        return render_template(
            "cubicador/packing_list.html",
            tido=tido,
            nudo=nudo,
            api_url=api_url,
            volver_url=volver_url,
            logo_url=logo_url,
            exportador=exportador,
        )

    # ──────────────────────────────────────────────────────────────
    #  GET /cubicador/api/packing-list/<tido>/<nudo>
    # ──────────────────────────────────────────────────────────────
    @app.route("/cubicador/api/packing-list/<string:tido>/<path:nudo>", methods=["GET"])
    @_cub_api
    def cubicador_plus_packing_list(tido, nudo):
        """Packing list POR PALLET: una fila por LINEA del documento.

        Devuelve sku/descripcion/cantidad de cada linea, tal como salen del
        ERP. Las lineas de servicio (ZZ ENVIO, descuentos, fletes) se
        excluyen — no son cajas — pero se listan en `lineas_excluidas` con el
        motivo. El Nro. de pallet, las medidas, el kilo y el tipo de embalaje
        NO vienen en esta respuesta: son 100% manuales, se arman en
        `_tab_packing.html` y se guardan en localStorage del navegador (ver
        docstring del modulo — DECISION DE PERSISTENCIA).
        """
        tido = (tido or "").strip().upper()
        nudo = (nudo or "").strip()
        if not tido or not nudo:
            return jsonify({"ok": False,
                            "error": "Falta el tipo o el numero de documento."}), 400

        # Validar el TIDO contra la lista oficial del Cubicador (si esta
        # disponible): evita pegarle al ERP con codigos inventados.
        tipos = _h("TIPOS_DOC_CUBICADOR") or []
        codigos = {str(t[0]).upper() for t in tipos if isinstance(t, (list, tuple)) and t}
        if codigos and tido not in codigos:
            return jsonify({
                "ok": False,
                "error": f"Tipo de documento no soportado: {tido}.",
                "tipos_validos": sorted(codigos),
            }), 400

        _cubicador_fetch = _h("_cubicador_fetch")
        if not callable(_cubicador_fetch):
            return jsonify({"ok": False,
                            "error": "El lector de documentos del ERP no esta disponible."}), 503

        try:
            header, lineas = _cubicador_fetch(tido, nudo)
        except ConnectionError as ce:
            # El ERP esta caido o con el breaker abierto: mensaje amigable.
            print(f"[cubicador_plus] packing-list ERP no disponible {tido} {nudo}: {ce}",
                  flush=True)
            return jsonify({"ok": False,
                            "error": "El ERP no esta respondiendo en este momento. "
                                     "Intenta nuevamente en unos minutos.",
                            "error_codigo": "ERP_DOWN"}), 503

        if header is None:
            return jsonify({"ok": False,
                            "error": f"No se encontro {tido} N° {nudo} en el ERP.",
                            "error_codigo": "DOC_NO_ENCONTRADO"}), 404

        filas, excluidas = _packing_list_filas(lineas or [])

        total_unidades = sum(_f(f.get("cantidad")) for f in filas)

        return jsonify({
            "ok": True,
            "documento": {
                "tido":         header.get("tido") or tido,
                "nudo":         header.get("nudo") or nudo,
                "nudo_display": header.get("nudo_display") or nudo,
                "fecha":        header.get("fecha") or "",
                "valor_neto":   _f(header.get("valor_neto")),
                "valor_bruto":  _f(header.get("valor_bruto")),
            },
            "cliente": {
                "nombre":    header.get("cliente_nombre") or "",
                "rut":       header.get("cliente_rut") or "",
                "direccion": header.get("direccion") or "",
                "comuna":    header.get("comuna") or "",
                "email":     header.get("email") or "",
                "telefono":  header.get("telefono") or "",
            },
            "filas":            filas,
            "lineas_excluidas": excluidas,
            "totales": {
                "productos":      len({f["sku"] for f in filas}) if filas else 0,
                "filas":          len(filas),
                "total_unidades": _r(total_unidades, 2),
            },
            "nota_pallets": ("El Nro. de pallet, las medidas, el kilo y el tipo de embalaje "
                             "no estan registrados en ningun sistema: se arman a mano por "
                             "pallet (varias lineas pueden compartir un pallet) y se guardan "
                             "en este navegador."),
        }), 200

    # ── Schema: se verifica SIEMPRE al registrar (Regla #5 —
    #    produccion corre con ILUS_SKIP_MIGRATIONS=1). Nunca revienta el boot.
    try:
        with app.app_context():
            _ensure_cubicador_plus_schema()
    except Exception as e:
        print(f"[ILUS][WARN] _ensure_cubicador_plus_schema: {e}", flush=True)

    return app
