"""
Modulo COTIZACIONES DE TRANSPORTE Y DISTRIBUCION (2026-07-25).

Vive en un archivo APARTE (no en app.py) por el mismo criterio que
cubicador_plus.py:
  1) app.py tiene ~75.500 lineas y hay otra sesion editandolo en paralelo.
  2) Todo lo de aca es AGREGADO: tablas propias con prefijo `transport_cotiz*`
     / `transport_cotizacion*`, numeracion propia (CTR-, nunca COT- que es de
     Servicio Tecnico / tk_cotizaciones), settings propios
     (transport_cotiz_settings, NO se reutiliza tk_settings -- acoplaria dos
     modulos que deben quedar independientes). No toca ni redefine nada de
     Tickets/Cotizaciones de Servicio Tecnico ni del Cubicador (Regla #4.2).

Se registra desde app.py con 2-3 lineas (patron ya usado con cubicador_plus):

    try:
        from logistica_cotizaciones import register_logistica_cotizaciones as _register_logistica_cotizaciones
        _register_logistica_cotizaciones(app, globals())
        print("[ILUS] Modulo Cotizaciones de Transporte registrado.")
    except Exception as _lc_reg_err:
        print(f"[ILUS][WARN] register_logistica_cotizaciones: {_lc_reg_err}")

ALCANCE DE ESTE ARCHIVO
------------------------
Fase 1 (andamiaje, ya construida) dejó el esquema idempotente de las 4
tablas, el PDF (reusa templates/tickets/cotizacion_pdf.html TAL CUAL,
Playwright, header/footer nativos) y la plantilla de correo editable
(comm_templates, modulo='transporte_cotizaciones').

Fase 2 (este agente, mismo día) agrega el CRUD completo sobre ese
andamiaje, sin tocarlo:
  - Correlativo propio CTR-NNNNNN sin condicion de carrera (SELECT...FOR
    UPDATE sobre transport_cotiz_settings.ultimo_correlativo -- mismo
    patron ya probado por tk_api_cotizacion_desde_erp en
    tickets_module.py).
  - Motor de calculo (_lc_calcular_totales / _lc_recalcular): prorratea el
    costo_courier entre los items segun peso predominante, aplica margen
    editable (global, con snapshot por linea) y descuento de linea +
    descuento global independiente, y termina en IVA/total -- mismo orden
    que _tk_cotiz_recalcular (tickets_module.py): subtotal -> descuento ->
    IVA -> total.
  - Rutas: settings, cotizar-flete (preview sin persistir, invoca
    api_asignar_cotizar_couriers SIN duplicar su logica), crear, listar,
    detalle, (re)asignar courier, agregar/editar/quitar item, recalcular,
    estado (aprobar/rechazar/reabrir), eliminar (soft-delete con las
    guardas de aprobada/reutilizada), clonar (via oficial de "editar" una
    cotizacion ya no-draft), log, calculo (trazabilidad de formula), y el
    endpoint que efectivamente ENVIA el correo (usa
    `_render_comm_template("cotizacion_envio", "email", {...},
    modulo="transporte_cotizaciones")`, con fallback a HTML plano si la
    plantilla no está disponible).
  - Permiso propio "logistica_cotizaciones" con fallback seguro a
    "transporte"/"superadmin" mientras esa clave no se agregue a la
    matriz de roles de app.py (ver _lc_tiene_permiso) -- las rutas nuevas
    de este archivo usan _lc_api (login_required + ese gate), NO
    _tr_required; /pdf y /ver de la fase 1 se dejan intactas con
    _tr_required (ya eran un gate real, cambiar el decorador de una ruta
    que ya funciona sin necesidad no es gratis, Regla #4.2).

INVESTIGACION comm_templates -- plantillas de transporte YA sembradas
-----------------------------------------------------------------------
Se revisaron TODOS los `modulo=` usados hoy en comm_templates (app.py):
'tickets', 'catalogo', 'retiros', 'transporte', 'mantenciones', 'general',
'comunicacion_interna'. La UNICA relacionada con "transporte" es
`_ensure_comm_template_transporte` / `_transporte_tpl_seed` (app.py
~L72784), con modulo='transporte' y estados
programado/en_ruta/en_camino/entregado/fallido -- esas son notificaciones
de ESTADO DE ENVIO de un paquete/documento individual (courier en ruta,
entregado, etc.), un concepto TOTALMENTE distinto a "aqui tienes tu
cotizacion, apruebala". No hay ninguna plantilla existente para
"cotizacion" ni "presupuesto" bajo modulo='transporte' ni en ningun otro
modulo de transporte -- CERO duplicados u obsoletos que documentar; no se
borro ni se toco nada de _transporte_tpl_seed (Regla #4.2). Por eso este
archivo siembra un modulo PROPIO ('transporte_cotizaciones') en vez de
sumar un estado nuevo dentro de modulo='transporte': mezclar "estado de
despacho" con "estado de cotizacion" bajo el mismo modulo confundiria el
editor de /comunicaciones (Daniel viendo "cotizacion_envio" en la misma
lista que "en_camino" no tiene sentido para quien edita) y arriesga que un
futuro estado de cotizacion choque de nombre con un estado de tracking.

Autor: agente Claude -- 2026-07-25. Reglas aplicadas: #1 (el HTML/JS que
consuma esto en fases futuras debe usar ilus*, no alert/confirm), #2
(paleta, ver header/footer del PDF), #4 (SQL parametrizado, sin detalles
internos al cliente), #4.2 (solo agrega, no toca tickets_module.py ni
cotizacion_pdf.html), #5 (schema idempotente), #6 (fechas via helpers de
hora Chile), #11 (branding generico ILUS via _get_brand_cfg, plantilla
editable en comm_templates con patron _ensure_comm_template_*).
"""

import os
import re
import sys
import json
import html as _html_mod
import traceback
from decimal import Decimal, ROUND_HALF_UP
from functools import wraps

from flask import request, render_template, render_template_string, Response, jsonify, g

try:
    from zoneinfo import ZoneInfo
    _CL_TZ = ZoneInfo("America/Santiago")
except Exception:
    _CL_TZ = None

from datetime import datetime, timezone

# ──────────────────────────────────────────────────────────────────────
#  Acceso a los helpers de app.py — IMPORT DIFERIDO (mismo mecanismo que
#  cubicador_plus.py): resolvemos cada helper con getattr() EN TIEMPO DE
#  REQUEST, cuando app.py ya termino de cargar. Evita import circular.
# ──────────────────────────────────────────────────────────────────────
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


def _flask_app():
    """La instancia Flask real (app.py define `app = Flask(__name__)` a
    nivel de modulo) -- necesaria para static_folder (logo del PDF)."""
    return _h("app")


# ──────────────────────────────────────────────────────────────────────
#  Utilidades chicas (copias auto-contenidas de las de tickets_module.py
#  -- NO se importan de alli a proposito: son funciones puras sin ningun
#  estado ni dependencia de closures, y evitar el import cruzado mantiene
#  este modulo desacoplado, mismo criterio que cubicador_plus.py aplica
#  con sus propios helpers numericos _f/_r).
# ──────────────────────────────────────────────────────────────────────

def _lc_chile_hoy():
    """Fecha de HOY en hora Chile (Regla #6)."""
    try:
        if _CL_TZ is not None:
            return datetime.now(timezone.utc).astimezone(_CL_TZ).date()
    except Exception:
        pass
    return datetime.utcnow().date()


def _lc_money_round(x):
    """Redondeo comercial 'half up' (no bancario) -- mismo criterio que
    _tk_money_round de tickets_module.py."""
    try:
        return int(Decimal(str(x)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except Exception:
        try:
            return int(round(float(x or 0)))
        except Exception:
            return 0


_LC_UNIDADES = ("", "un", "dos", "tres", "cuatro", "cinco", "seis", "siete", "ocho", "nueve",
                "diez", "once", "doce", "trece", "catorce", "quince", "dieciséis",
                "diecisiete", "dieciocho", "diecinueve", "veinte")
_LC_DECENAS = ("", "", "veinti", "treinta", "cuarenta", "cincuenta", "sesenta",
               "setenta", "ochenta", "noventa")
_LC_CENTENAS = ("", "ciento", "doscientos", "trescientos", "cuatrocientos", "quinientos",
                "seiscientos", "setecientos", "ochocientos", "novecientos")


def _lc_num_a_palabras_999(n):
    if n == 0:
        return ""
    if n == 100:
        return "cien"
    c, resto = divmod(n, 100)
    partes = []
    if c:
        partes.append(_LC_CENTENAS[c])
    if resto:
        if resto <= 20:
            partes.append(_LC_UNIDADES[resto])
        else:
            d, u = divmod(resto, 10)
            if d == 2:
                _veinti = {1: "veintiún", 2: "veintidós", 3: "veintitrés", 6: "veintiséis"}
                partes.append(_veinti.get(u, "veinti" + _LC_UNIDADES[u]) if u else "veinte")
            else:
                partes.append(_LC_DECENAS[d] + (" y " + _LC_UNIDADES[u] if u else ""))
    return " ".join(partes)


def _lc_monto_en_palabras_miles(n):
    miles, unidades = divmod(n, 1000)
    partes = []
    if miles:
        partes.append("mil" if miles == 1 else _lc_num_a_palabras_999(miles) + " mil")
    if unidades:
        partes.append(_lc_num_a_palabras_999(unidades))
    return " ".join(partes)


def _lc_monto_en_palabras(n):
    """Monto CLP entero a palabras (linea "SON: ..." del PDF) -- copia
    adaptada de _tk_monto_en_palabras (tickets_module.py). Devuelve None si
    n no es un entero positivo razonable."""
    try:
        n = int(n)
    except (TypeError, ValueError):
        return None
    if n < 0 or n >= 1_000_000_000_000:
        return None
    if n == 0:
        return "cero pesos"
    millones, resto_millon = divmod(n, 1_000_000)
    miles, unidades = divmod(resto_millon, 1000)
    partes = []
    if millones:
        partes.append("un millón" if millones == 1
                       else _lc_monto_en_palabras_miles(millones) + " millones")
    if miles:
        partes.append("mil" if miles == 1 else _lc_num_a_palabras_999(miles) + " mil")
    if unidades:
        partes.append(_lc_num_a_palabras_999(unidades))
    sufijo = " de pesos" if (millones and not miles and not unidades) else " pesos"
    return " ".join(partes).strip() + sufijo


def _lc_cotiz_logo_b64():
    """Logo pre-encoded (mismo archivo static/logo_pdf.txt que usan los PDF
    de OT y de Cotizaciones de Servicio Tecnico -- asset compartido, no se
    duplica el PNG)."""
    try:
        _app = _flask_app()
        _static_folder = getattr(_app, "static_folder", None) if _app else None
        if not _static_folder:
            return ""
        _logo_path = os.path.join(_static_folder, "logo_pdf.txt")
        if os.path.exists(_logo_path):
            with open(_logo_path, "r", encoding="utf-8") as _f_logo:
                return _f_logo.read().strip()
    except Exception:
        pass
    return ""


def _lc_cotiz_logo_shs_b64():
    """PNG real del logo SPORTS HEALTH SOLUTIONS (static/logo_shs.png),
    pre-codificado en static/logo_shs_pdf.txt -- mismo patrón que
    _lc_cotiz_logo_b64() para el logo ILUS."""
    try:
        _app = _flask_app()
        _static_folder = getattr(_app, "static_folder", None) if _app else None
        if not _static_folder:
            return ""
        _logo_path = os.path.join(_static_folder, "logo_shs_pdf.txt")
        if os.path.exists(_logo_path):
            with open(_logo_path, "r", encoding="utf-8") as _f_logo:
                return _f_logo.read().strip()
    except Exception:
        pass
    return ""


def _lc_logo_shs_html(alto_mm=17.6):
    """Logo real SPORTS HEALTH SOLUTIONS (PNG, static/logo_shs.png) en el
    header del PDF de cotización. FIX 2026-07-27 (Daniel compartió el PNG
    exacto): antes era una reconstrucción en HTML/CSS con texto azul navy
    que no calzaba con la marca real (blanco y negro) -- se reemplaza por
    la imagen real, del mismo alto que el logo ILUS para que ambos queden
    a la par (antes 12mm, chico y desproporcionado frente al de ILUS)."""
    _b64 = _lc_cotiz_logo_shs_b64()
    if not _b64:
        return ""
    # El PNG real tiene fondo transparente y el texto "HEALTH SOLUTIONS" en
    # negro -- sobre el header oscuro del PDF quedaría ilegible. Se mantiene
    # el mismo tratamiento que la versión anterior (caja blanca redondeada
    # detrás), ahora con la imagen real adentro en vez del texto CSS.
    _caja_alto = alto_mm + 2.4
    return (
        f'<div style="height:{_caja_alto}mm;background:#fff;border-radius:1mm;'
        'box-sizing:border-box;display:flex;align-items:center;justify-content:center;'
        'padding:1.2mm 2.4mm;-webkit-print-color-adjust:exact;print-color-adjust:exact;">'
        f'<img src="data:image/png;base64,{_b64}" '
        f'style="height:{alto_mm}mm;width:auto;object-fit:contain;display:block;">'
        '</div>'
    )


def _lc_fecha_str(v):
    if v is None:
        return None
    if isinstance(v, str):
        return v
    try:
        return v.strftime("%d-%m-%Y")
    except Exception:
        return str(v)


def _lc_fecha_date(v):
    """Fecha (date, sin hora) -- None si no se puede interpretar."""
    if v is None:
        return None
    if hasattr(v, "date") and callable(getattr(v, "date")):
        try:
            return v.date()
        except Exception:
            pass
    if hasattr(v, "year"):
        return v
    if isinstance(v, str):
        for _fmt in ("%Y-%m-%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(v, _fmt).date()
            except ValueError:
                continue
    return None


# ══════════════════════════════════════════════════════════════════════
#  FASE 2 — utilidades numericas + permiso del modulo (ver docstring)
# ══════════════════════════════════════════════════════════════════════

def _f(value, default=0.0):
    """float tolerante: acepta None, Decimal, '12,5' (coma decimal chilena).
    Copia auto-contenida de la de cubicador_plus.py (mismo criterio de
    desacoplar modulos: sin import cruzado)."""
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


def _pct(value, default=0.0):
    """Porcentaje acotado a [0, 100]."""
    return max(0.0, min(100.0, _f(value, default)))


# Alias corto: _lc_money_round ya existe arriba (fase 1) con el mismo
# criterio 'half up' -- se reusa tal cual, sin duplicar la logica.
_money = _lc_money_round


def _clp(valor):
    try:
        return "$" + f"{int(round(_f(valor))):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "$0"


# Peso volumetrico: mismo divisor que usa TODA la app (app.calc_pv,
# cubicador_plus._DIVISOR_VOLUMETRICO). NO cambiar sin avisar.
_DIVISOR_VOLUMETRICO = 4000.0

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")

_ESTADOS_VALIDOS = ("draft", "sent", "approved", "rejected", "expired")
_ESTADO_ACCION = {"approved": "aprobar", "rejected": "rechazar", "draft": "reabrir"}
# FIX 2026-07-25 (revisión adversarial, hallazgo ALTA): lc_estado (endpoint
# POST /api/<id>/estado) solo acepta estos dos destinos. 'draft' se deja
# en _ESTADO_ACCION arriba solo para que el historial (_TRC_HIST_LABEL en
# cotizaciones.html) siga sabiendo mostrar la etiqueta "Reabierta" de
# registros viejos, pero YA NO es un destino válido para este endpoint: la
# regla de negocio de Daniel es que la ÚNICA salida de una cotización
# 'approved'/'sent' es 'rejected' (cancelar) o clonarla (/clonar) -- jamás
# volver a 'draft', porque eso reabre la edición libre de ítems
# (_lc_items_editables) sobre una cotización que ya se aprobó/envió.
_ESTADO_TRANSICIONES_LC_ESTADO = ("approved", "rejected")


def _lc_tiene_permiso(permisos):
    """Permiso propio "logistica_cotizaciones" con fallback seguro a
    "transporte"/"superadmin" -- ver nota del docstring del modulo sobre
    por que este archivo no edita la matriz de roles de app.py."""
    permisos = permisos or {}
    return bool(
        permisos.get("logistica_cotizaciones")
        or permisos.get("transporte")
        or permisos.get("superadmin")
    )


def _lc_solo_superadmin():
    permisos = getattr(g, "permissions", None) or {}
    return bool(permisos.get("superadmin"))


def _lc_es_admin():
    permisos = getattr(g, "permissions", None) or {}
    return bool(permisos.get("admin") or permisos.get("superadmin"))


def _lc_api(fn):
    """Decorador de los endpoints JSON nuevos (fase 2): login_required +
    gate de permiso + blindaje de excepciones (Regla #4). Mismo patron que
    cubicador_plus._cub_api. Las rutas /pdf y /ver de la fase 1 NO usan
    este decorador (siguen con _tr_required, ver docstring del modulo)."""
    @wraps(fn)
    def wrapped(*args, **kwargs):
        try:
            permisos = getattr(g, "permissions", None) or {}
            if not _lc_tiene_permiso(permisos):
                return jsonify({
                    "ok": False,
                    "error": "No tienes acceso al módulo de Cotizaciones de Transporte.",
                    "error_codigo": "SIN_PERMISO",
                }), 403
        except Exception as e_perm:
            print(f"[logistica_cotizaciones] fallo leyendo permisos: {e_perm}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo validar tu sesión."}), 403

        try:
            return fn(*args, **kwargs)
        except Exception as e:
            print(f"[logistica_cotizaciones][ERROR] {fn.__name__}: {e}", flush=True)
            try:
                print(traceback.format_exc(), flush=True)
            except Exception:
                pass
            return jsonify({
                "ok": False,
                "error": "Ocurrió un problema procesando la solicitud. "
                         "Intenta nuevamente; si persiste avisa a soporte.",
            }), 500

    login_required = _h("login_required")
    return login_required(wrapped) if callable(login_required) else wrapped


# ══════════════════════════════════════════════════════════════════════
#  ESQUEMA — tablas nuevas (Regla #5: patron _ensure_* idempotente,
#  corre siempre al boot aunque ILUS_SKIP_MIGRATIONS=1). Prefijo
#  `transport_` para mantener consistencia con lo que ya existe
#  (transport_manifests, transport_logs, transport_couriers).
#  SQL transcrito TAL CUAL del diseno tecnico (secciones 1.1-1.4).
# ══════════════════════════════════════════════════════════════════════

def _ensure_transport_cotiz_tables():
    """CREATE TABLE IF NOT EXISTS de las 4 tablas del modulo + semillas de
    transport_cotiz_settings. Cada sentencia en su propio try/except: un
    fallo en una tabla no debe impedir que las demas se creen."""
    mysql_execute = _h("mysql_execute")
    mysql_fetchone = _h("mysql_fetchone")
    if not mysql_execute:
        print("[logistica_cotizaciones] mysql_execute no disponible, "
              "esquema NO verificado", flush=True)
        return

    try:
        mysql_execute("""
            CREATE TABLE IF NOT EXISTS transport_cotizaciones (
              id                    INT AUTO_INCREMENT PRIMARY KEY,
              numero                VARCHAR(30)  NOT NULL UNIQUE,
              estado                ENUM('draft','sent','approved','rejected','expired')
                                     NOT NULL DEFAULT 'draft',
              eliminada             TINYINT(1)   NOT NULL DEFAULT 0,
              eliminada_por         VARCHAR(190) NULL,
              eliminada_at          DATETIME     NULL,
              editada_post_aprobacion TINYINT(1) NOT NULL DEFAULT 0,
              reutilizada_en_id     INT NULL,

              cliente_id            INT NULL,
              erp_idmaeen           INT NULL,
              erp_koen               VARCHAR(50)  NULL,
              rut                    VARCHAR(12)  NULL,
              empresa                VARCHAR(150) NULL,
              email                  VARCHAR(190) NULL,
              telefono                VARCHAR(50)  NULL,
              direccion               VARCHAR(300) NULL,
              direccion_lat            DECIMAL(10,7) NULL,
              direccion_lng            DECIMAL(10,7) NULL,
              direccion_place_id       VARCHAR(200) NULL,
              comuna                   VARCHAR(100) NULL,
              region                   VARCHAR(100) NULL,
              contacto_nombre          VARCHAR(200) NULL,
              contacto_cargo           VARCHAR(120) NULL,
              contacto_tel             VARCHAR(50)  NULL,
              contacto_email           VARCHAR(190) NULL,
              ejecutivo                VARCHAR(190) NULL,

              courier_id               INT NULL,
              courier_nombre            VARCHAR(120) NULL,
              peso_kg                   DECIMAL(10,3) NULL,
              peso_vol_kg                DECIMAL(10,3) NULL,
              peso_facturable_kg          DECIMAL(10,3) NULL,
              comuna_resuelta              VARCHAR(100) NULL,
              costo_courier                INT NOT NULL DEFAULT 0,

              margen_pct                    DECIMAL(5,2) NOT NULL DEFAULT 30.00,
              margen_monto                  INT NOT NULL DEFAULT 0,
              subtotal_items                 INT NOT NULL DEFAULT 0,
              descuento_pct                  DECIMAL(5,2) NOT NULL DEFAULT 0,
              descuento_monto                 INT NOT NULL DEFAULT 0,
              subtotal                        INT NOT NULL DEFAULT 0,
              iva_pct                         DECIMAL(5,2) NOT NULL DEFAULT 19,
              iva_monto                        INT NOT NULL DEFAULT 0,
              total                            INT NOT NULL DEFAULT 0,

              valida_hasta               DATE NULL,
              notas                       TEXT NULL,
              notas_internas               TEXT NULL,
              terminos                      TEXT NULL,
              ticket_id                     INT NULL,

              created_by                    VARCHAR(190) NULL,
              updated_by                    VARCHAR(190) NULL,
              created_at                    DATETIME DEFAULT CURRENT_TIMESTAMP,
              updated_at                    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

              KEY idx_estado (estado),
              KEY idx_cliente (cliente_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
    except Exception as e:
        print(f"[logistica_cotizaciones] transport_cotizaciones: {e}", flush=True)

    # Defensivo (regla de negocio agregada DESPUÉS del diseño original: una
    # cotización "reutilizada" -- clonada -- en otra no se puede eliminar
    # mientras ese vínculo exista). Si la tabla ya existía de un deploy
    # anterior sin esta columna, se agrega aquí -- mismo patrón que
    # _ensure_tk_cotizaciones_columns en tickets_module.py.
    try:
        mysql_fetchall = _h("mysql_fetchall")
        cols = {r["COLUMN_NAME"] for r in (mysql_fetchall(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='transport_cotizaciones'") or [])}
        if "reutilizada_en_id" not in cols:
            mysql_execute("ALTER TABLE transport_cotizaciones ADD COLUMN reutilizada_en_id INT NULL")
    except Exception as e:
        print(f"[logistica_cotizaciones] ALTER reutilizada_en_id: {e}", flush=True)

    try:
        mysql_execute("""
            CREATE TABLE IF NOT EXISTS transport_cotizacion_items (
              id                       INT AUTO_INCREMENT PRIMARY KEY,
              cotizacion_id             INT NOT NULL,

              erp_tido                   VARCHAR(10)  NULL,
              erp_nudo                    VARCHAR(30)  NULL,
              erp_kopr                     VARCHAR(100) NULL,
              descripcion                   VARCHAR(300) NULL,
              app_id                        INT NULL,

              cantidad                      INT NOT NULL DEFAULT 1,
              total_bultos                   INT NOT NULL DEFAULT 0,
              peso_kg                         DECIMAL(10,3) NOT NULL DEFAULT 0,
              volumen_cm3                      DECIMAL(14,2) NOT NULL DEFAULT 0,

              costo_courier_prorrateo           INT NOT NULL DEFAULT 0,
              margen_pct_aplicado                DECIMAL(5,2) NOT NULL DEFAULT 30.00,
              margen_monto                        INT NOT NULL DEFAULT 0,
              descuento_pct                        DECIMAL(5,2) NOT NULL DEFAULT 0,
              subtotal                              INT NOT NULL DEFAULT 0,
              total                                  INT NOT NULL DEFAULT 0,

              notas                                  TEXT NULL,

              KEY idx_cotizacion (cotizacion_id),
              CONSTRAINT fk_trcotit_cot FOREIGN KEY (cotizacion_id)
                REFERENCES transport_cotizaciones(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
    except Exception as e:
        print(f"[logistica_cotizaciones] transport_cotizacion_items: {e}", flush=True)

    try:
        mysql_execute("""
            CREATE TABLE IF NOT EXISTS transport_cotizacion_log (
              id             INT AUTO_INCREMENT PRIMARY KEY,
              cotizacion_id  INT NOT NULL,
              accion         VARCHAR(40)  NOT NULL,
              usuario        VARCHAR(190) NULL,
              detalle        TEXT NULL,
              created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
              KEY idx_cotiz_fecha (cotizacion_id, created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
    except Exception as e:
        print(f"[logistica_cotizaciones] transport_cotizacion_log: {e}", flush=True)

    try:
        mysql_execute("""
            CREATE TABLE IF NOT EXISTS transport_cotiz_settings (
              clave       VARCHAR(60)  NOT NULL PRIMARY KEY,
              valor       VARCHAR(190) NOT NULL,
              updated_by  VARCHAR(190) NULL,
              updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
    except Exception as e:
        print(f"[logistica_cotizaciones] transport_cotiz_settings: {e}", flush=True)

    # Semillas de settings (INSERT IGNORE -- no pisa si Daniel ya las tocó
    # desde /transporte/cotizaciones/api/settings). "ultimo_correlativo" es
    # el contador de la serie CTR-NNNNNN (fase 2) -- arranca en 0, nunca se
    # comparte con tk_settings.cotiz_ultimo_correlativo (series independientes,
    # ver docstring del modulo).
    if mysql_fetchone:
        for _clave, _valor in (("margen_pct", "30"), ("descuento_pct_default", "0"),
                               ("iva_pct_default", "19"), ("ultimo_correlativo", "0")):
            try:
                mysql_execute(
                    "INSERT IGNORE INTO transport_cotiz_settings (clave, valor) VALUES (%s,%s)",
                    (_clave, _valor))
            except Exception as e:
                print(f"[logistica_cotizaciones] seed settings {_clave}: {e}", flush=True)


def _lc_log(cid, accion, detalle=None, user=None):
    """Trazabilidad de una cotizacion de transporte -- clon del patron
    _tk_cotiz_log de tickets_module.py. Escribe en transport_cotizacion_log
    Y en el audit central (_audit de app.py), cada escritura en su propio
    try/except: un fallo de log NUNCA debe tumbar la accion real (crear,
    editar, aprobar, enviar, eliminar una cotizacion). `detalle` se guarda
    como JSON. Acciones esperadas: crear, editar, editar_aprobada, aprobar,
    rechazar, reabrir, enviar_correo, eliminar, cotizar_courier."""
    mysql_execute = _h("mysql_execute")
    _audit = _h("_audit")
    current_username = _h("current_username")
    _u = user or (current_username() if current_username else None) or "sistema"
    try:
        _det_json = json.dumps(detalle, ensure_ascii=False, default=str) if detalle is not None else None
    except Exception:
        _det_json = None
    if mysql_execute:
        try:
            mysql_execute(
                "INSERT INTO transport_cotizacion_log (cotizacion_id, accion, usuario, detalle) "
                "VALUES (%s,%s,%s,%s)", (cid, str(accion)[:40], str(_u)[:190], _det_json))
        except Exception as e:
            print(f"[logistica_cotizaciones] _lc_log INSERT {accion} cid={cid}: {e}", flush=True)
    if _audit:
        try:
            _audit(f"transport_cotizacion_{accion}", target_type="transport_cotizacion",
                   target_id=cid, details=(detalle or {}))
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════
#  SETTINGS (margen/descuento/IVA por defecto)
# ══════════════════════════════════════════════════════════════════════

def _lc_settings():
    mysql_fetchall = _h("mysql_fetchall")
    rows = (mysql_fetchall("SELECT clave, valor FROM transport_cotiz_settings") or []) \
        if callable(mysql_fetchall) else []
    m = {r["clave"]: r["valor"] for r in rows}
    return {
        "margen_pct": _pct(m.get("margen_pct"), 30.0),
        "descuento_pct_default": _pct(m.get("descuento_pct_default"), 0.0),
        "iva_pct_default": _pct(m.get("iva_pct_default"), 19.0),
    }


# ══════════════════════════════════════════════════════════════════════
#  CORRELATIVO — SIN condicion de carrera (SELECT...FOR UPDATE), mismo
#  patron exacto que tk_api_cotizacion_desde_erp en tickets_module.py
#  (~L3856-3891): conexion FRESCA (get_mysql(), NO la pooled de get_db()
#  con REPEATABLE READ), fila bloqueada, valor calculado en Python y usado
#  directo -- JAMAS releido con una conexion distinta despues del commit
#  (bug real documentado en la memoria del proyecto:
#  concurrencia_numero_pool_repeatable_read.md).
# ══════════════════════════════════════════════════════════════════════

def _lc_generar_numero(cur):
    """Debe llamarse con un cursor abierto sobre una conexion de
    get_mysql() (fresca, autocommit=False), DENTRO de la transaccion que
    va a insertar la cabecera. Devuelve "CTR-NNNNNN" ya reservado (el
    UPDATE del correlativo ya se ejecuto en esta misma transaccion -- el
    caller solo debe hacer commit)."""
    cur.execute(
        "SELECT valor FROM transport_cotiz_settings WHERE clave='ultimo_correlativo' FOR UPDATE")
    fila = cur.fetchone()
    if fila is None:
        cur.execute(
            "INSERT IGNORE INTO transport_cotiz_settings (clave, valor) VALUES ('ultimo_correlativo','0')")
        cur.execute(
            "SELECT valor FROM transport_cotiz_settings WHERE clave='ultimo_correlativo' FOR UPDATE")
        fila = cur.fetchone()
    try:
        ultimo = int((fila or {}).get("valor") or 0)
    except (TypeError, ValueError):
        ultimo = 0
    correlativo = ultimo + 1
    cur.execute(
        "UPDATE transport_cotiz_settings SET valor=%s WHERE clave='ultimo_correlativo'",
        (str(correlativo),))
    return f"CTR-{correlativo:06d}"


# ══════════════════════════════════════════════════════════════════════
#  PESO / VOLUMEN — reusa app_bultos (misma tabla que el Cubicador).
#  Columnas verificadas contra el CREATE TABLE real (citado también en
#  cubicador_plus.py): id, product_id, bulto_num, largo, ancho, alto,
#  peso. Solo LECTURA -- este modulo nunca escribe en app_bultos ni crea
#  productos nuevos (a diferencia de cubicador_plus, si el SKU no existe
#  en el catálogo local la línea queda sin medidas y se avisa).
# ══════════════════════════════════════════════════════════════════════

def _lc_bultos_unitarios_batch(app_ids):
    """{app_id: {"peso_kg_u", "volumen_cm3_u", "total_bultos"}} para TODOS
    los ids pedidos en UNA sola query (evita N+1 con documentos grandes)."""
    mysql_fetchall = _h("mysql_fetchall")
    ids = sorted({int(x) for x in (app_ids or []) if x})
    if not ids or not callable(mysql_fetchall):
        return {}
    placeholders = ",".join(["%s"] * len(ids))
    rows = mysql_fetchall(
        f"SELECT product_id, largo, ancho, alto, peso FROM `app_bultos` "
        f"WHERE product_id IN ({placeholders})", tuple(ids)) or []
    out = {}
    for r in rows:
        pid = r.get("product_id")
        d = out.setdefault(pid, {"peso_kg_u": 0.0, "volumen_cm3_u": 0.0, "total_bultos": 0})
        d["peso_kg_u"] += _f(r.get("peso"))
        d["volumen_cm3_u"] += _f(r.get("largo")) * _f(r.get("ancho")) * _f(r.get("alto"))
        d["total_bultos"] += 1
    return out


def _lc_resolver_app_ids_por_sku(skus):
    """Resuelve SKU -> app_id via app_products (SOLO LECTURA)."""
    mysql_fetchall = _h("mysql_fetchall")
    skus = sorted({(s or "").strip().upper() for s in (skus or []) if (s or "").strip()})
    if not skus or not callable(mysql_fetchall):
        return {}
    placeholders = ",".join(["%s"] * len(skus))
    rows = mysql_fetchall(
        f"SELECT id, UPPER(TRIM(sku)) AS sku_up FROM `app_products` "
        f"WHERE UPPER(TRIM(sku)) IN ({placeholders})", tuple(skus)) or []
    return {r["sku_up"]: r["id"] for r in rows}


def _lc_enriquecer_items_con_medidas(items_in):
    """A partir de items crudos [{app_id|erp_kopr/sku, cantidad, ...}],
    devuelve una lista nueva con total_bultos/peso_kg(TOTAL de línea)/
    volumen_cm3(TOTAL de línea) resueltos, más `sin_medidas` (bool) por
    línea. NO muta la entrada."""
    items_in = items_in or []

    skus_a_resolver = [it.get("erp_kopr") or it.get("sku") for it in items_in
                       if not it.get("app_id") and (it.get("erp_kopr") or it.get("sku"))]
    mapa_sku = _lc_resolver_app_ids_por_sku(skus_a_resolver) if skus_a_resolver else {}

    app_ids = []
    for it in items_in:
        aid = it.get("app_id")
        if not aid:
            sku_up = (it.get("erp_kopr") or it.get("sku") or "").strip().upper()
            aid = mapa_sku.get(sku_up)
        if aid:
            app_ids.append(aid)

    bultos_map = _lc_bultos_unitarios_batch(app_ids)

    out = []
    for it in items_in:
        aid = it.get("app_id")
        if not aid:
            sku_up = (it.get("erp_kopr") or it.get("sku") or "").strip().upper()
            aid = mapa_sku.get(sku_up)
        try:
            cantidad = max(int(_f(it.get("cantidad"), 1)), 0)
        except (TypeError, ValueError):
            cantidad = 1

        b = bultos_map.get(aid) if aid else None
        if b and cantidad > 0:
            peso_kg = round(b["peso_kg_u"] * cantidad, 3)
            volumen_cm3 = round(b["volumen_cm3_u"] * cantidad, 2)
            total_bultos = int(b["total_bultos"])
            sin_medidas = False
        else:
            peso_kg = 0.0
            volumen_cm3 = 0.0
            total_bultos = 0
            sin_medidas = True

        out.append({
            "app_id": aid,
            "erp_tido": (it.get("erp_tido") or it.get("tido") or "").strip().upper()[:10] or None,
            "erp_nudo": str(it.get("erp_nudo") or it.get("nudo") or "").strip()[:30] or None,
            "erp_kopr": (it.get("erp_kopr") or it.get("sku") or "").strip()[:100] or None,
            "descripcion": (it.get("descripcion") or it.get("nombre") or "").strip()[:300] or None,
            "cantidad": cantidad,
            "total_bultos": total_bultos,
            "peso_kg": peso_kg,
            "volumen_cm3": volumen_cm3,
            "descuento_pct": _pct(it.get("descuento_pct"), 0.0),
            "margen_pct_aplicado": (None if it.get("margen_pct_aplicado") in (None, "")
                                    else _pct(it.get("margen_pct_aplicado"))),
            "notas": (it.get("notas") or "").strip()[:2000] or None,
            "sin_medidas": sin_medidas,
        })
    return out


# ══════════════════════════════════════════════════════════════════════
#  COMPARADOR DE COURIERS — invoca la vista YA existente
#  (api_asignar_cotizar_couriers) sin duplicar su logica (tarifas,
#  brackets, seguro, regla comercial Felca/Milling...). Mismo mecanismo
#  exacto que cubicador_plus._invocar_comparador.
# ══════════════════════════════════════════════════════════════════════

_ENDPOINT_COMPARADOR = "api_asignar_cotizar_couriers"


def _lc_invocar_comparador(flask_app, payload):
    view = None
    try:
        view = flask_app.view_functions.get(_ENDPOINT_COMPARADOR)
    except Exception:
        view = None
    if view is None:
        return {"ok": False,
                "error": "El comparador de couriers no está disponible en este momento."}, 503

    previo = getattr(request, "_cached_json", None)
    try:
        try:
            request._cached_json = (payload, payload)
        except Exception as e_cache:
            print(f"[logistica_cotizaciones] no se pudo inyectar el payload al comparador: {e_cache}",
                  flush=True)
            return {"ok": False, "error": "No se pudo preparar la consulta de tarifas."}, 500
        rv = view()
    finally:
        try:
            if previo is not None:
                request._cached_json = previo
        except Exception:
            pass

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
                "error": "El comparador de couriers devolvió una respuesta inesperada."}, 502
    return data, status


# ══════════════════════════════════════════════════════════════════════
#  MOTOR DE CALCULO — funcion PURA (sin BD), reusada tanto al crear como
#  al recalcular tras cualquier edicion (item, margen, descuento, courier).
# ══════════════════════════════════════════════════════════════════════

def _lc_calcular_totales(items, costo_courier, margen_pct_global, descuento_pct_global, iva_pct):
    """
    items: lista de dicts con al menos: id (opcional), peso_kg (TOTAL de
    línea), volumen_cm3 (TOTAL de línea), descuento_pct,
    margen_pct_aplicado (o None -> usa el global).

    Devuelve (items_out, header) — NO escribe en BD.

    Prorrateo: cada línea se lleva del costo_courier una fracción
    proporcional a su "peso predominante" (max(peso, peso_volumétrico),
    mismo criterio que usa el comparador para el envío completo). Si
    ninguna línea tiene peso/volumen cargado, se reparte en partes
    iguales. El redondeo se ajusta en la ÚLTIMA línea para que la suma
    cuadre EXACTO con costo_courier (nunca se pierden ni sobran pesos)."""
    costo_courier = _money(costo_courier)
    margen_pct_global = _pct(margen_pct_global, 30.0)
    descuento_pct_global = _pct(descuento_pct_global, 0.0)
    iva_pct = _pct(iva_pct, 19.0)

    n = len(items or [])
    predominantes = []
    for it in (items or []):
        peso = _f(it.get("peso_kg"))
        vol_kg = _f(it.get("volumen_cm3")) / _DIVISOR_VOLUMETRICO
        predominantes.append(max(peso, vol_kg))
    total_pred = sum(predominantes)

    items_out = []
    acumulado_courier = 0
    acumulado_margen = 0
    subtotal_items = 0

    for idx, it in enumerate(items or []):
        es_ultimo = (idx == n - 1)
        if es_ultimo:
            costo_prorrateo = costo_courier - acumulado_courier
        else:
            share = (predominantes[idx] / total_pred) if total_pred > 0 else ((1.0 / n) if n else 0.0)
            costo_prorrateo = _money(costo_courier * share)
        acumulado_courier += costo_prorrateo

        margen_pct_linea = it.get("margen_pct_aplicado")
        margen_pct_linea = margen_pct_global if margen_pct_linea in (None, "") else _pct(margen_pct_linea)
        margen_monto = _money(costo_prorrateo * margen_pct_linea / 100.0)
        acumulado_margen += margen_monto

        subtotal_linea = costo_prorrateo + margen_monto
        desc_pct_linea = _pct(it.get("descuento_pct"), 0.0)
        total_linea = _money(subtotal_linea * (1 - desc_pct_linea / 100.0))
        subtotal_items += total_linea

        items_out.append({
            "id": it.get("id"),
            "peso_predominante_kg": round(predominantes[idx], 3),
            "share_pct": round((predominantes[idx] / total_pred * 100.0) if total_pred > 0
                               else ((100.0 / n) if n else 0.0), 2),
            "costo_courier_prorrateo": costo_prorrateo,
            "margen_pct_aplicado": margen_pct_linea,
            "margen_monto": margen_monto,
            "descuento_pct": desc_pct_linea,
            "subtotal": subtotal_linea,
            "total": total_linea,
        })

    descuento_monto = _money(subtotal_items * descuento_pct_global / 100.0)
    subtotal = subtotal_items - descuento_monto
    iva_monto = _money(subtotal * iva_pct / 100.0)
    total = subtotal + iva_monto

    header = {
        "costo_courier": costo_courier,
        "margen_pct": margen_pct_global,
        "margen_monto": acumulado_margen,
        "subtotal_items": subtotal_items,
        "descuento_pct": descuento_pct_global,
        "descuento_monto": descuento_monto,
        "subtotal": subtotal,
        "iva_pct": iva_pct,
        "iva_monto": iva_monto,
        "total": total,
    }
    return items_out, header


def _lc_recalcular(cid, user=None):
    """Lee cabecera + items ACTUALES de la BD, corre _lc_calcular_totales y
    PERSISTE el resultado (items y cabecera). Devuelve el dict `header` o
    None si la cotización no existe. Se llama tras CUALQUIER cambio que
    afecte el precio: crear, agregar/editar/quitar ítem, cambiar
    margen/descuento global, o reasignar courier."""
    mysql_fetchone = _h("mysql_fetchone")
    mysql_fetchall = _h("mysql_fetchall")
    mysql_execute = _h("mysql_execute")

    cab = mysql_fetchone(
        "SELECT id, costo_courier, margen_pct, descuento_pct, iva_pct "
        "FROM transport_cotizaciones WHERE id=%s", (cid,))
    if not cab:
        return None

    filas = mysql_fetchall(
        "SELECT id, peso_kg, volumen_cm3, descuento_pct, margen_pct_aplicado "
        "FROM transport_cotizacion_items WHERE cotizacion_id=%s ORDER BY id", (cid,)) or []

    items_calc = [{
        "id": r["id"], "peso_kg": r.get("peso_kg"), "volumen_cm3": r.get("volumen_cm3"),
        "descuento_pct": r.get("descuento_pct"), "margen_pct_aplicado": r.get("margen_pct_aplicado"),
    } for r in filas]

    items_out, header = _lc_calcular_totales(
        items_calc, cab.get("costo_courier"), cab.get("margen_pct"),
        cab.get("descuento_pct"), cab.get("iva_pct"))

    for it in items_out:
        mysql_execute(
            "UPDATE transport_cotizacion_items SET "
            "costo_courier_prorrateo=%s, margen_pct_aplicado=%s, margen_monto=%s, "
            "subtotal=%s, total=%s WHERE id=%s",
            (it["costo_courier_prorrateo"], it["margen_pct_aplicado"], it["margen_monto"],
             it["subtotal"], it["total"], it["id"]))

    mysql_execute(
        "UPDATE transport_cotizaciones SET margen_monto=%s, subtotal_items=%s, "
        "descuento_monto=%s, subtotal=%s, iva_monto=%s, total=%s, updated_by=%s WHERE id=%s",
        (header["margen_monto"], header["subtotal_items"], header["descuento_monto"],
         header["subtotal"], header["iva_monto"], header["total"], user, cid))

    return header


# ══════════════════════════════════════════════════════════════════════
#  GUARDAS DE ESTADO (reglas de negocio agregadas por Daniel, ADEMÁS del
#  diseño original)
# ══════════════════════════════════════════════════════════════════════

def _lc_items_editables(cot):
    """Solo se pueden agregar/quitar/editar ítems mientras la cotización
    esté en 'draft'. Una vez 'sent' o 'approved' (o cualquier otro estado
    no-draft) los ítems quedan congelados -- la única vía para "editar" es
    clonarla como una cotización nueva (ver /clonar)."""
    return (cot or {}).get("estado") == "draft"


def _lc_puede_eliminar(cot):
    """(bool, motivo_o_None). 'approved' NUNCA se puede eliminar, ni con
    confirm_text ni por superadmin. Tampoco si fue REUTILIZADA (clonada)
    en otra cotización -- ese vínculo debe romperse a propósito primero."""
    if not cot:
        return False, "Cotización no encontrada."
    if cot.get("estado") == "approved":
        return False, "Esta cotización está aprobada: no se puede eliminar."
    if cot.get("reutilizada_en_id"):
        return False, (f"Esta cotización fue reutilizada en la cotización "
                        f"#{cot['reutilizada_en_id']}: no se puede eliminar mientras "
                        f"ese vínculo exista.")
    return True, None


# ══════════════════════════════════════════════════════════════════════
#  GUARDA DE MANIFIESTOS — helper de SOLO LECTURA, reusable. NO se engancha
#  a ningún endpoint de app.py desde este archivo (esa tarea no toca
#  app.py -- ver detalle preciso en la respuesta final del agente sobre
#  qué endpoints faltan enganchar: tr_agregar_item / app.py:23427 y
#  tr_quitar_item / app.py:23456). Replica EXACTAMENTE la misma consulta
#  que ya usa tr_manifiesto_eliminar (app.py ~22559-22568) para decidir si
#  un manifiesto "ya tiene actividad real".
# ══════════════════════════════════════════════════════════════════════

def _lc_manifiesto_tiene_actividad(mid):
    """(bool tiene_actividad, str motivo_o_None). Solo LECTURA."""
    mysql_fetchone = _h("mysql_fetchone")
    if not callable(mysql_fetchone):
        return False, None
    activo = mysql_fetchone(
        "SELECT COUNT(*) AS n FROM transport_manifest_items "
        "WHERE manifest_id=%s AND (tracking_number IS NOT NULL "
        "OR simpliroute_visit_id IS NOT NULL)", (mid,))
    tiene_courier = bool(activo and activo.get("n"))
    tiene_pod = mysql_fetchone(
        "SELECT COUNT(*) AS n FROM transport_delivery_proof dp "
        "JOIN transport_manifest_items mi ON mi.commitment_id = dp.commitment_id "
        "WHERE mi.manifest_id=%s", (mid,))
    tiene_pod = bool(tiene_pod and tiene_pod.get("n"))
    if tiene_pod:
        return True, "ya tiene prueba de entrega firmada"
    if tiene_courier:
        return True, "ya se subió a un courier"
    return False, None


# ══════════════════════════════════════════════════════════════════════
#  PDF de la cotizacion — reusa templates/tickets/cotizacion_pdf.html TAL
#  CUAL (mismo archivo, sin duplicar ni un byte). Solo se arma el
#  diccionario de contexto respetando el contrato documentado dentro de
#  ese template. Igual que tk_cotizacion_pdf, este modulo NO tiene bloque
#  "rico" (Propuesta de Mantencion/Visita/Instalacion) -- eso es propio de
#  Servicio Tecnico; una cotizacion de transporte siempre usa el formato
#  simple (modo_rico=False), que el template ya soporta sin cambios.
# ══════════════════════════════════════════════════════════════════════

def _lc_cotizacion_pdf_ctx(cid):
    """Arma el contexto del PDF de una cotizacion de transporte (cabecera +
    items multidocumento). Devuelve (ctx_pdf, cot) o None si no existe.
    Compartido por /pdf (Playwright) y /ver (visor HTML instantaneo) --
    una sola fuente de verdad, mismo criterio que _tk_cotizacion_pdf_ctx."""
    mysql_fetchone = _h("mysql_fetchone")
    mysql_fetchall = _h("mysql_fetchall")
    if not mysql_fetchone or not mysql_fetchall:
        return None

    cot = mysql_fetchone(
        "SELECT * FROM transport_cotizaciones WHERE id=%s AND COALESCE(eliminada,0)=0", (cid,))
    if not cot:
        return None
    items_rows = mysql_fetchall(
        "SELECT erp_kopr, erp_nudo, descripcion, cantidad, total "
        "FROM transport_cotizacion_items WHERE cotizacion_id=%s ORDER BY id", (cid,)) or []

    # UF EN VIVO (Regla: reusa el mismo cache/endpoint de indicadores
    # economicos ya existente en app.py -- _uf_valor_actual, TTL 1h,
    # /api/uf-actual -- en vez de inventar un segundo cache paralelo). A
    # diferencia de tk_cotizaciones, esta tabla no guarda un snapshot de
    # UF al emitir (no esta en el diseno de transport_cotizaciones), asi
    # que siempre se usa el valor vigente. Resiliente: si la API externa
    # esta caida, uf_info queda None y el template simplemente omite el
    # equivalente en UF (nunca rompe el PDF).
    uf_info = None
    _uf_helper = _h("_uf_valor_actual")
    if _uf_helper:
        try:
            _d = _uf_helper()
            if _d and _d.get("ok") and _d.get("uf"):
                uf_info = {"valor": float(_d["uf"]), "fecha": _d.get("fecha")}
        except Exception as e:
            print(f"[logistica_cotizaciones] UF no disponible: {e}", flush=True)
    uf_valor = uf_info["valor"] if uf_info else None

    def _precio_uf_str(monto_clp):
        if not uf_valor:
            return None
        return "{:,.1f}".format(monto_clp / uf_valor).replace(",", "X").replace(".", ",").replace("X", ".")

    # Multidocumento: la columna "Documento" del PDF solo se muestra si la
    # cotizacion mezcla mas de 1 documento ERP de origen (mismo criterio
    # que Servicio Tecnico).
    _docs_presentes = {(it.get("erp_nudo") or "").strip()
                        for it in items_rows if (it.get("erp_nudo") or "").strip()}
    mostrar_col_documento = len(_docs_presentes) > 1

    items = []
    for it in items_rows:
        cant = int(float(it.get("cantidad") or 0))
        tot = int(it.get("total") or 0)
        # El costo del courier YA viene prorrateado dentro de este total
        # (transport_cotizacion_items.costo_courier_prorrateo se suma en el
        # motor de calculo que arma la fase de creacion/edicion) -- aca solo
        # se lee, no se prorratea de nuevo. precio unitario es SOLO derivado
        # para mostrar "Precio UF" (no se guarda en la BD).
        pu = _lc_money_round(tot / cant) if cant > 0 else tot
        items.append({
            "sku": it.get("erp_kopr") or "",
            "descripcion": it.get("descripcion") or "",
            "cantidad": cant,
            "total": tot,
            "precio_uf": _precio_uf_str(pu),
            "documento": (it.get("erp_nudo") or "").strip() or "—",
        })

    _rut_fmt = _h("rut_fmt_filter")
    rut_mostrar = cot.get("rut") or ""
    if _rut_fmt and rut_mostrar:
        try:
            rut_mostrar = _rut_fmt(rut_mostrar)
        except Exception:
            pass

    _fecha_emision_date = _lc_fecha_date(cot.get("created_at")) or _lc_chile_hoy()
    _valida_hasta_date = _lc_fecha_date(cot.get("valida_hasta"))
    validez_dias = (_valida_hasta_date - _fecha_emision_date).days if _valida_hasta_date else None

    numero_mostrar = cot.get("numero") or f"#{cid}"

    # Etiqueta de "Tipo de Solicitud" del bloque Despacho -- no hay
    # tipo_servicio en este modulo (es siempre transporte/distribucion);
    # se agrega el courier elegido cuando existe, para que el PDF sea mas
    # informativo sin inventar un campo que no esta en el diseno.
    tipo_label = "Transporte y Distribución"
    if cot.get("courier_nombre"):
        tipo_label = f"Transporte y Distribución · {cot.get('courier_nombre')}"

    terminos_pares = []
    _terminos_raw = [l.strip() for l in (cot.get("terminos") or "").split("\n") if l.strip()]
    for _linea in _terminos_raw:
        if ":" in _linea:
            _clave, _valor = _linea.split(":", 1)
            terminos_pares.append((_clave.strip(), _valor.strip()))
        else:
            terminos_pares.append((_linea, ""))
    if not terminos_pares:
        terminos_pares = [
            ("Servicio", "transporte y distribución de la mercadería individualizada en el detalle adjunto."),
            ("Precio", "valores netos; incluye el flete cotizado con el courier más el margen de gestión ILUS."),
            ("Validez", "sujeta a la vigencia indicada; el valor del flete puede variar según el courier a la fecha del despacho."),
        ]

    ctx_pdf = {
        "cot": {
            "id": cid,
            "numero": numero_mostrar,
            "fecha_emision": _lc_fecha_str(cot.get("created_at")) or _lc_fecha_str(_lc_chile_hoy()),
            "valida_hasta": _lc_fecha_str(cot.get("valida_hasta")),
            "validez_dias": validez_dias,
            "empresa": cot.get("empresa") or "",
            "rut": rut_mostrar,
            "direccion": cot.get("direccion") or "",
            "comuna": cot.get("comuna") or "",
            "region": cot.get("region") or "",
            "email": cot.get("email") or "",
            "telefono": cot.get("telefono") or "",
            "ejecutivo": cot.get("ejecutivo") or "",
            "contacto_nombre": cot.get("contacto_nombre") or "",
            "tipo_servicio_label": tipo_label,
            "notas": cot.get("notas"),
            "subtotal": int(cot.get("subtotal") or 0),
            "descuento_pct": float(cot.get("descuento_pct") or 0),
            "descuento_monto": int(cot.get("descuento_monto") or 0),
            # El costo del flete NO se muestra como linea aparte -- ya viene
            # PRORRATEADO dentro del Total de cada item (ver arriba), igual
            # que "Costo de Ruta" en el PDF de Servicio Tecnico.
            "costo_ruta": 0,
            "iva_pct": float(cot.get("iva_pct") or 19),
            "iva_monto": int(cot.get("iva_monto") or 0),
            "total": int(cot.get("total") or 0),
        },
        "items": items,
        "total_en_palabras": _lc_monto_en_palabras(cot.get("total")),
        "uf_info": uf_info,
        "modo_rico": False,
        "kpis": None,
        "rico_textos": None,
        "alcance_lineas": [],
        "recomendacion_lineas": [],
        "terminos_pares": terminos_pares,
        "mostrar_col_documento": mostrar_col_documento,
    }
    ctx_pdf["logo_b64"] = _lc_cotiz_logo_b64()
    return (ctx_pdf, cot)


def _lc_cotizacion_pdf_bytes(cid):
    """Genera los bytes del PDF (Playwright, header/footer nativos --
    calcado de _tk_cotizacion_pdf_bytes). Devuelve (pdf_bytes, filename) o
    None si la cotizacion no existe. Lanza si el motor no esta disponible
    (el caller lo convierte a 503)."""
    _res = _lc_cotizacion_pdf_ctx(cid)
    if not _res:
        return None
    ctx_pdf, cot = _res
    numero_mostrar = ctx_pdf["cot"]["numero"]
    html_doc = render_template("tickets/cotizacion_pdf.html", **ctx_pdf)

    _pw_pdf = _h("_pw_pdf")
    if not _pw_pdf:
        raise RuntimeError("pdf_engine_unavailable")

    _get_brand_cfg = _h("_get_brand_cfg")
    _support_email = "servicio.tecnico@ilusfitness.com"
    if _get_brand_cfg:
        try:
            _support_email = _get_brand_cfg().get("support_email") or _support_email
        except Exception:
            pass

    _footer_numero = _html_mod.escape(numero_mostrar)
    _valida_str = ctx_pdf["cot"].get("valida_hasta") or ""
    _footer_vigencia = (
        f' · válida hasta el {_html_mod.escape(_valida_str)}' if _valida_str else "")
    footer_tpl = (
        '<div style="width:100%;font-size:7px;font-family:Arial,Helvetica,sans-serif;'
        'color:#6b7280;padding:3px 12mm 0;box-sizing:border-box;display:flex;'
        'justify-content:space-between;align-items:center;border-top:1.5px solid #dc2626;">'
        f'<span><b style="color:#0a0a0a">ILUS Sport &amp; Health</b> · www.ilusfitness.com · '
        f'{_html_mod.escape(_support_email)}</span>'
        f'<span>Cotización <b style="color:#0a0a0a">{_footer_numero}</b>{_footer_vigencia} · '
        'Página <span class="pageNumber"></span> de <span class="totalPages"></span></span>'
        '</div>'
    )
    _logo_b64_val = ctx_pdf.get("logo_b64")
    _header_ilus = (
        f'<img src="data:image/png;base64,{_logo_b64_val}" style="height:17.6mm;max-width:67.2mm;object-fit:contain;">'
        if _logo_b64_val else
        '<span style="font-weight:900;font-size:25.6px;color:#ffffff;letter-spacing:.02em;">'
        '<span style="color:#dc2626;">&#9650;</span> ILUS<span style="color:#dc2626;">.</span></span>'
    )
    _header_fecha = _html_mod.escape(ctx_pdf["cot"].get("fecha_emision") or "")
    header_tpl = (
        '<div style="width:100%;font-family:Arial,Helvetica,sans-serif;">'
        '<div style="background:#0a0a0a;padding:5mm 12mm;box-sizing:border-box;'
        'display:flex;justify-content:space-between;align-items:center;'
        '-webkit-print-color-adjust:exact;print-color-adjust:exact;">'
        f'<div style="display:flex;align-items:center;gap:5mm;">{_lc_logo_shs_html()}{_header_ilus}</div>'
        '<div style="text-align:right;">'
        '<div style="font-size:15px;font-weight:800;color:#ffffff;letter-spacing:-.01em;">COTIZACIÓN</div>'
        f'<div style="font-size:19px;font-weight:900;color:#dc2626;line-height:1.15;">N&#176; {_footer_numero}</div>'
        f'<div style="font-size:7.5px;color:#9ca3af;margin-top:1px;">FECHA EMISIÓN: {_header_fecha}</div>'
        '</div></div>'
        '<div style="height:2mm;background:linear-gradient(90deg,#dc2626 0 25%,#0a0a0a 25% 100%);'
        '-webkit-print-color-adjust:exact;print-color-adjust:exact;"></div>'
        '</div>'
    )

    pdf_bytes = _pw_pdf(
        html_doc, page_format="A4",
        margin={"top": "36mm", "right": "0mm", "bottom": "16mm", "left": "0mm"},
        header_template=header_tpl, footer_template=footer_tpl,
    )
    _emp = re.sub(r"[^A-Za-z0-9 _-]", "", (cot.get("empresa") or "cliente"))[:60].strip() or "cliente"
    _fname = f"Cotizacion_{numero_mostrar}_{_emp}.pdf".replace(" ", "_")
    return (pdf_bytes, _fname)


# ══════════════════════════════════════════════════════════════════════
#  PLANTILLA DE CORREO — envio de la cotizacion al cliente (Regla #11:
#  marca generica ILUS, editable desde /comunicaciones). Sembrada con
#  modulo PROPIO 'transporte_cotizaciones' -- ver nota de investigacion
#  en el docstring del archivo sobre por que no se reutiliza
#  modulo='transporte'.
# ══════════════════════════════════════════════════════════════════════

def _lc_tpl_seed():
    """Source of truth de las plantillas EDITABLES de
    transporte_cotizaciones (email). {slug: (asunto, cuerpo_html)}.
    Placeholders: {{numero_cotizacion}}, {{empresa}}, {{total}},
    {{valida_hasta}} (ya viene con ", válida hasta el DD-MM-AAAA" o vacio),
    {{mensaje}} (ya viene envuelto en <p> o vacio) -- mismos nombres que
    usa tk_api_cotizacion_enviar (tickets_module.py) para el modulo
    'tickets', asi el futuro endpoint de envio de este modulo puede
    armar las variables con el mismo criterio."""
    return {
        'cotizacion_envio': (
            "Cotización {{numero_cotizacion}} · {{empresa}}",
            "<p>Estimados,</p>"
            "<p>Adjuntamos la cotización de transporte y distribución "
            "<strong>{{numero_cotizacion}}</strong> para <strong>{{empresa}}</strong> "
            "por un total de <strong>{{total}}</strong>{{valida_hasta}}.</p>"
            "{{mensaje}}"
            "<p>Quedamos atentos a sus comentarios.</p>"
        ),
    }


def _ensure_comm_template_cotizacion_transporte():
    """Siembra idempotente de la plantilla de correo del modulo
    'transporte_cotizaciones' AUNQUE ILUS_SKIP_MIGRATIONS=1. INSERT IGNORE
    -> no pisa ediciones de Daniel (UNIQUE modulo+estado+canal). Mismo
    patron que _ensure_comm_template_catalogo / _ensure_comm_template_retiros
    (app.py)."""
    mysql_fetchone = _h("mysql_fetchone")
    mysql_execute = _h("mysql_execute")
    if not mysql_fetchone or not mysql_execute:
        return 0
    try:
        sembradas = 0
        for slug, (asunto, cuerpo) in _lc_tpl_seed().items():
            existe = mysql_fetchone(
                "SELECT id FROM comm_templates WHERE modulo='transporte_cotizaciones' "
                "  AND estado=%s AND canal='email' LIMIT 1", (slug,))
            if existe:
                continue
            mysql_execute(
                "INSERT IGNORE INTO comm_templates (modulo, estado, canal, asunto, cuerpo) "
                "VALUES ('transporte_cotizaciones',%s,'email',%s,%s)", (slug, asunto, cuerpo))
            sembradas += 1
        if sembradas:
            print(f"[logistica_cotizaciones] {sembradas} plantilla(s) de "
                  "transporte_cotizaciones sembradas", flush=True)
        return sembradas
    except Exception as e:
        print(f"[logistica_cotizaciones] no se pudo sembrar transporte_cotizaciones: {e}", flush=True)
        return 0


# ══════════════════════════════════════════════════════════════════════
#  RUTAS
# ══════════════════════════════════════════════════════════════════════

def register_logistica_cotizaciones(app, ctx=None):
    """Engancha las rutas de Cotizaciones de Transporte a la Flask app de
    app.py. `ctx` (opcional) acepta el globals() de app.py por
    compatibilidad con el patron de tickets_module.py, pero no es
    necesario: los helpers se resuelven por getattr sobre el modulo, en
    tiempo de request (ver _h arriba)."""
    global _APP_MODULE_NAME
    try:
        nombre_mod = getattr(app, "import_name", None)
        if nombre_mod and nombre_mod in sys.modules:
            _APP_MODULE_NAME = nombre_mod
    except Exception:
        pass

    # Esquema + plantilla de correo: idempotentes, corren siempre al boot
    # (Regla #5 / #11), best-effort -- un fallo aca no debe tumbar el
    # registro de rutas ni la app completa.
    try:
        _ensure_transport_cotiz_tables()
    except Exception as e:
        print(f"[logistica_cotizaciones] _ensure_transport_cotiz_tables: {e}", flush=True)
    try:
        _ensure_comm_template_cotizacion_transporte()
    except Exception as e:
        print(f"[logistica_cotizaciones] _ensure_comm_template_cotizacion_transporte: {e}", flush=True)

    _tr_required = _h("_tr_required")
    _login_required = _h("login_required")
    if _login_required is None:
        # Sin gate de sesion disponible: mejor NO registrar NINGUNA ruta
        # (fallar cerrado) que dejarlas abiertas sin control de acceso.
        print("[logistica_cotizaciones] login_required no disponible en app.py, "
              "NINGUNA ruta de cotizaciones de transporte fue registrada", flush=True)
        return

    # ════════════════════════════════════════════════════════════════
    #  FASE 2 — CRUD completo (settings, cotizar-flete, crear, listar,
    #  detalle, courier, items, recalcular, estado, eliminar, clonar,
    #  log, calculo, enviar). Todas gateadas con _lc_api (login_required +
    #  permiso propio con fallback -- ver _lc_tiene_permiso).
    # ════════════════════════════════════════════════════════════════

    @app.route("/transporte/cotizaciones/api/settings", methods=["GET"])
    @_lc_api
    def lc_settings_get():
        return jsonify({"ok": True, "settings": _lc_settings()})

    @app.route("/transporte/cotizaciones/api/settings", methods=["POST"])
    @_lc_api
    def lc_settings_post():
        if not _lc_es_admin():
            return jsonify({"ok": False, "error": "Solo un administrador puede cambiar "
                            "la configuración del módulo."}), 403
        mysql_execute = _h("mysql_execute")
        current_username = _h("current_username")
        body = request.get_json(silent=True) or {}
        user = (current_username() if callable(current_username) else None) or "sistema"
        cambios = {}
        for clave, tope in (("margen_pct", 100000.0), ("descuento_pct_default", 100.0),
                            ("iva_pct_default", 100.0)):
            if clave in body:
                valor = max(0.0, min(tope, _f(body.get(clave))))
                mysql_execute(
                    "INSERT INTO transport_cotiz_settings (clave, valor, updated_by) "
                    "VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE valor=VALUES(valor), "
                    "updated_by=VALUES(updated_by)", (clave, str(valor), user))
                cambios[clave] = valor
        try:
            _audit = _h("_audit")
            if callable(_audit) and cambios:
                _audit("transport_cotiz_settings_editar", target_type="transport_cotiz_settings",
                       details=cambios)
        except Exception:
            pass
        return jsonify({"ok": True, "settings": _lc_settings()})

    @app.route("/transporte/cotizaciones/api/cotizar-flete", methods=["POST"])
    @_lc_api
    def lc_cotizar_flete():
        """Body: { "comuna": "...", "es_residencial": false, "valor_neto": N,
          "items": [{"app_id":123,"cantidad":2}, {"sku":"ABC","cantidad":1}, ...] }

        PREVIEW: no persiste nada. Devuelve lo mismo que
        /api/asignar/cotizar-couriers (courier_id, courier_nombre, precio,
        fuente, etc. por courier) más el desglose de peso agregado de ESTE
        conjunto de ítems. El courier elegido se manda recién en /crear o
        en /<id>/courier."""
        body = request.get_json(silent=True) or {}
        comuna = (body.get("comuna") or "").strip()
        if not comuna:
            return jsonify({"ok": False, "error": "Falta la comuna de destino.",
                            "error_codigo": "SIN_COMUNA"}), 400

        items_in = body.get("items") or []
        if not isinstance(items_in, list) or not items_in:
            return jsonify({"ok": False, "error": "Agrega al menos un ítem para cotizar el flete.",
                            "error_codigo": "SIN_ITEMS"}), 400

        items_calc = _lc_enriquecer_items_con_medidas(items_in)
        peso_kg_total = round(sum(_f(it["peso_kg"]) for it in items_calc), 3)
        vol_cm3_total = round(sum(_f(it["volumen_cm3"]) for it in items_calc), 2)
        peso_vol_kg_total = round(vol_cm3_total / _DIVISOR_VOLUMETRICO, 3)
        peso_pred = max(peso_kg_total, peso_vol_kg_total)
        sin_medidas = [it for it in items_calc if it.get("sin_medidas")]

        if peso_kg_total <= 0 and peso_vol_kg_total <= 0:
            return jsonify({
                "ok": True, "cotizaciones": [], "recomendado_id": None,
                "peso_kg": 0, "peso_vol_kg": 0, "peso_facturable_kg": 0,
                "comuna_resuelta": comuna, "items_calculados": items_calc,
                "items_sin_medidas": len(sin_medidas),
                "mensaje": "Ninguno de los ítems tiene medidas registradas (bultos): "
                           "carga las medidas en el Cubicador antes de cotizar el flete.",
            })

        payload_comparador = {
            "peso_kg": peso_kg_total, "peso_pred_kg": peso_pred, "comuna": comuna,
            "es_residencial": bool(body.get("es_residencial", False)),
            "valor_neto": _f(body.get("valor_neto")),
        }
        data, status = _lc_invocar_comparador(app, payload_comparador)
        if status != 200 or not isinstance(data, dict) or not data.get("ok"):
            mensaje = "No se pudieron obtener las tarifas de los couriers."
            if isinstance(data, dict):
                mensaje = data.get("error") or mensaje
            return jsonify({"ok": False, "error": mensaje}), (status if status >= 400 else 502)

        data["peso_kg"] = peso_kg_total
        data["peso_vol_kg"] = peso_vol_kg_total
        data["items_calculados"] = items_calc
        data["items_sin_medidas"] = len(sin_medidas)
        if sin_medidas:
            data["aviso_medidas"] = (
                f"{len(sin_medidas)} ítem(s) no tienen medidas (bultos) registradas y "
                f"se cotizaron con peso 0 — el flete real puede ser mayor.")
        return jsonify(data), 200

    @app.route("/transporte/cotizaciones/api/crear", methods=["POST"])
    @_lc_api
    def lc_crear():
        get_mysql = _h("get_mysql")
        current_username = _h("current_username")
        body = request.get_json(silent=True) or {}

        items_in = body.get("items") or []
        if not isinstance(items_in, list) or not items_in:
            return jsonify({"ok": False, "error": "La cotización necesita al menos un ítem.",
                            "error_codigo": "SIN_ITEMS"}), 400

        courier_nombre = (body.get("courier_nombre") or "").strip() or None
        costo_courier = _money(body.get("costo_courier"))
        if not courier_nombre or costo_courier <= 0:
            return jsonify({
                "ok": False,
                "error": "Falta cotizar el flete con un courier antes de crear la cotización "
                         "(usa /cotizar-flete y elige una opción).",
                "error_codigo": "SIN_COURIER",
            }), 400

        settings = _lc_settings()
        margen_pct = _pct(body.get("margen_pct"), settings["margen_pct"])
        descuento_pct = _pct(body.get("descuento_pct"), settings["descuento_pct_default"])
        iva_pct = _pct(body.get("iva_pct"), settings["iva_pct_default"])
        items_calc = _lc_enriquecer_items_con_medidas(items_in)
        user = (current_username() if callable(current_username) else None) or "sistema"

        # FIX 2026-07-25 (revisión adversarial, hallazgo BLOQUEANTE): los
        # totales se calculan en Python (función PURA, sin BD) ANTES del
        # INSERT y se graban ya resueltos dentro de la MISMA transacción de
        # get_mysql() -- ya NO se llama _lc_recalcular() en esta misma
        # request. _lc_recalcular relee por mysql_fetchone/mysql_execute,
        # que usan la conexión POOLED per-request (g._db, autocommit=False,
        # REPEATABLE READ); esa conexión ya fijó su snapshot con los SELECT
        # previos de esta misma request (_lc_settings/
        # _lc_enriquecer_items_con_medidas usan mysql_fetchall) -- así que,
        # tras el commit hecho por la conexión FRESCA de get_mysql(), una
        # relectura por la conexión pooled NO ve la fila recién insertada
        # (cab=None -> _lc_recalcular salía sin persistir nada y la fila
        # quedaba con total=0). Mismo patrón ya documentado en la memoria
        # del proyecto (concurrencia_numero_pool_repeatable_read.md) y ya
        # evitado a propósito en tk_api_cotizacion_desde_erp
        # (tickets_module.py).
        items_out, header = _lc_calcular_totales(
            items_calc, costo_courier, margen_pct, descuento_pct, iva_pct)

        conn = get_mysql()
        try:
            with conn.cursor() as cur:
                numero = _lc_generar_numero(cur)
                cur.execute(
                    "INSERT INTO transport_cotizaciones "
                    "(numero, estado, cliente_id, erp_idmaeen, erp_koen, rut, empresa, email, "
                    " telefono, direccion, direccion_lat, direccion_lng, direccion_place_id, "
                    " comuna, region, contacto_nombre, contacto_cargo, contacto_tel, contacto_email, "
                    " ejecutivo, courier_id, courier_nombre, peso_kg, peso_vol_kg, "
                    " peso_facturable_kg, comuna_resuelta, costo_courier, margen_pct, margen_monto, "
                    " subtotal_items, descuento_pct, descuento_monto, subtotal, iva_pct, iva_monto, "
                    " total, valida_hasta, notas, notas_internas, terminos, "
                    " created_by, updated_by) "
                    "VALUES (%s,'draft',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                    "        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (numero,
                     body.get("cliente_id"), body.get("erp_idmaeen"),
                     (body.get("erp_koen") or "")[:50] or None, (body.get("rut") or "")[:12] or None,
                     (body.get("empresa") or "")[:150] or None, (body.get("email") or "")[:190] or None,
                     (body.get("telefono") or "")[:50] or None, (body.get("direccion") or "")[:300] or None,
                     body.get("direccion_lat"), body.get("direccion_lng"),
                     (body.get("direccion_place_id") or "")[:200] or None,
                     (body.get("comuna") or "")[:100] or None, (body.get("region") or "")[:100] or None,
                     (body.get("contacto_nombre") or "")[:200] or None,
                     (body.get("contacto_cargo") or "")[:120] or None,
                     (body.get("contacto_tel") or "")[:50] or None,
                     (body.get("contacto_email") or "")[:190] or None,
                     (body.get("ejecutivo") or "")[:190] or None,
                     body.get("courier_id"), courier_nombre[:120],
                     _f(body.get("peso_kg")) or None, _f(body.get("peso_vol_kg")) or None,
                     _f(body.get("peso_facturable_kg")) or None,
                     (body.get("comuna_resuelta") or body.get("comuna") or "")[:100] or None,
                     costo_courier, margen_pct, header["margen_monto"],
                     header["subtotal_items"], descuento_pct, header["descuento_monto"],
                     header["subtotal"], iva_pct, header["iva_monto"], header["total"],
                     (body.get("valida_hasta") or "").strip() or None,
                     (body.get("notas") or "").strip() or None,
                     (body.get("notas_internas") or "").strip() or None,
                     (body.get("terminos") or "").strip() or None,
                     user, user),
                )
                # (columnas=42, 'estado' es literal 'draft' -> 41 placeholders;
                # verificado programáticamente tras el fix de arriba: 20 en la
                # primera línea del VALUES + 21 en la segunda = 41, igual a los
                # 41 valores de la tupla.)
                cot_id = cur.lastrowid
                for it, it_calc in zip(items_calc, items_out):
                    cur.execute(
                        "INSERT INTO transport_cotizacion_items "
                        "(cotizacion_id, erp_tido, erp_nudo, erp_kopr, descripcion, app_id, "
                        " cantidad, total_bultos, peso_kg, volumen_cm3, costo_courier_prorrateo, "
                        " margen_pct_aplicado, margen_monto, descuento_pct, subtotal, total, notas) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (cot_id, it["erp_tido"], it["erp_nudo"], it["erp_kopr"], it["descripcion"],
                         it["app_id"], it["cantidad"], it["total_bultos"], it["peso_kg"],
                         it["volumen_cm3"], it_calc["costo_courier_prorrateo"],
                         it_calc["margen_pct_aplicado"], it_calc["margen_monto"],
                         it_calc["descuento_pct"], it_calc["subtotal"], it_calc["total"],
                         it["notas"]),
                    )
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[logistica_cotizaciones] crear CRASH: {e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo crear la cotización."}), 500
        finally:
            conn.close()

        # numero/cot_id/header ya se conocen desde DENTRO de la transacción
        # (calculados en Python antes del INSERT y grabados en la misma
        # transacción de get_mysql()) -- NO releer con la conexión pooled
        # distinta tras el commit (REPEATABLE READ puede no ver la fila
        # recién commiteada). Ver memoria del proyecto:
        # concurrencia_numero_pool_repeatable_read.md.
        _lc_log(cot_id, "crear", {"numero": numero, "items": len(items_calc),
                                  "courier": courier_nombre, "costo_courier": costo_courier}, user)
        sin_medidas = sum(1 for it in items_calc if it.get("sin_medidas"))
        return jsonify({"ok": True, "id": cot_id, "numero": numero,
                        "items_sin_medidas": sin_medidas, "totales": header})

    @app.route("/transporte/cotizaciones/api/<int:cid>/actualizar", methods=["POST"])
    @_lc_api
    def lc_actualizar(cid):
        """Guardado completo del modal de edición (templates/transporte/
        _modal_cotizacion_logistica.html, lcGuardar() en modo edición).

        Los campos de "cabecera segura" (cliente/contacto/direccion/notas/
        ejecutivo/valida_hasta) SIEMPRE se pueden editar, sin importar el
        estado -- son datos administrativos, no afectan ningún monto ya
        cobrado/aprobado.

        courier/margen/descuento/items (todo lo que afecta el TOTAL) solo
        se reemplaza si la cotización sigue en 'draft' -- mismo criterio
        que ya aplican /courier, /items y /recalcular (_lc_items_editables).
        Si NO está en draft (ej. 'approved'), esos campos del payload se
        ignoran en silencio (el modal ya avisó al usuario con un
        ilusConfirm antes de llegar acá) y se deja registro con
        editada_post_aprobacion=1 + accion 'editar_aprobada' -- acción ya
        reservada en el docstring de _lc_log."""
        mysql_fetchone = _h("mysql_fetchone")
        mysql_execute = _h("mysql_execute")
        current_username = _h("current_username")

        cot = mysql_fetchone(
            "SELECT id, estado FROM transport_cotizaciones WHERE id=%s AND COALESCE(eliminada,0)=0", (cid,))
        if not cot:
            return jsonify({"ok": False, "error": "Cotización no encontrada."}), 404

        body = request.get_json(silent=True) or {}
        user = (current_username() if callable(current_username) else None) or "sistema"
        editable = _lc_items_editables(cot)

        mysql_execute(
            "UPDATE transport_cotizaciones SET "
            "cliente_id=%s, erp_idmaeen=%s, erp_koen=%s, rut=%s, empresa=%s, email=%s, telefono=%s, "
            "direccion=%s, direccion_lat=%s, direccion_lng=%s, direccion_place_id=%s, comuna=%s, "
            "region=%s, contacto_nombre=%s, contacto_cargo=%s, contacto_tel=%s, contacto_email=%s, "
            "ejecutivo=%s, valida_hasta=%s, notas=%s, notas_internas=%s, terminos=%s, updated_by=%s "
            "WHERE id=%s",
            (body.get("cliente_id"), body.get("erp_idmaeen"),
             (body.get("erp_koen") or "")[:50] or None, (body.get("rut") or "")[:12] or None,
             (body.get("empresa") or "")[:150] or None, (body.get("email") or "")[:190] or None,
             (body.get("telefono") or "")[:50] or None, (body.get("direccion") or "")[:300] or None,
             body.get("direccion_lat"), body.get("direccion_lng"),
             (body.get("direccion_place_id") or "")[:200] or None,
             (body.get("comuna") or "")[:100] or None, (body.get("region") or "")[:100] or None,
             (body.get("contacto_nombre") or "")[:200] or None,
             (body.get("contacto_cargo") or "")[:120] or None,
             (body.get("contacto_tel") or "")[:50] or None,
             (body.get("contacto_email") or "")[:190] or None,
             (body.get("ejecutivo") or "")[:190] or None,
             (body.get("valida_hasta") or "").strip() or None,
             (body.get("notas") or "").strip() or None,
             (body.get("notas_internas") or "").strip() or None,
             (body.get("terminos") or "").strip() or None,
             user, cid))

        if editable:
            courier_nombre = (body.get("courier_nombre") or "").strip() or None
            costo_courier = _money(body.get("costo_courier"))
            settings = _lc_settings()
            margen_pct = _pct(body.get("margen_pct"), settings["margen_pct"])
            descuento_pct = _pct(body.get("descuento_pct"), settings["descuento_pct_default"])
            iva_pct = _pct(body.get("iva_pct"), settings["iva_pct_default"])

            mysql_execute(
                "UPDATE transport_cotizaciones SET courier_id=%s, courier_nombre=%s, costo_courier=%s, "
                "peso_kg=%s, peso_vol_kg=%s, peso_facturable_kg=%s, comuna_resuelta=%s, "
                "margen_pct=%s, descuento_pct=%s, iva_pct=%s, updated_by=%s WHERE id=%s",
                (body.get("courier_id"), courier_nombre[:120] if courier_nombre else None, costo_courier,
                 _f(body.get("peso_kg")) or None, _f(body.get("peso_vol_kg")) or None,
                 _f(body.get("peso_facturable_kg")) or None,
                 (body.get("comuna_resuelta") or body.get("comuna") or "")[:100] or None,
                 margen_pct, descuento_pct, iva_pct, user, cid))

            items_in = body.get("items")
            if isinstance(items_in, list):
                items_calc = _lc_enriquecer_items_con_medidas(items_in)
                mysql_execute("DELETE FROM transport_cotizacion_items WHERE cotizacion_id=%s", (cid,))
                for it in items_calc:
                    mysql_execute(
                        "INSERT INTO transport_cotizacion_items "
                        "(cotizacion_id, erp_tido, erp_nudo, erp_kopr, descripcion, app_id, "
                        " cantidad, total_bultos, peso_kg, volumen_cm3, margen_pct_aplicado, "
                        " descuento_pct, notas) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (cid, it["erp_tido"], it["erp_nudo"], it["erp_kopr"], it["descripcion"],
                         it["app_id"], it["cantidad"], it["total_bultos"], it["peso_kg"],
                         it["volumen_cm3"], (it["margen_pct_aplicado"] or margen_pct),
                         it["descuento_pct"], it["notas"]))
            accion, detalle = "editar", {"campos": list(body.keys())}
        else:
            # No-draft: el total/courier/items quedan intactos, solo cambió
            # la cabecera administrativa. Se marca editada_post_aprobacion
            # para que la lista muestre el ícono de aviso (mismo criterio
            # visual que ya usa cotizaciones.html con ese campo).
            mysql_execute(
                "UPDATE transport_cotizaciones SET editada_post_aprobacion=1 WHERE id=%s", (cid,))
            accion = "editar_aprobada" if cot.get("estado") == "approved" else "editar"
            detalle = {"campos": list(body.keys()), "estado": cot.get("estado"), "items_bloqueados": True}

        header = _lc_recalcular(cid, user)
        _lc_log(cid, accion, detalle, user)
        return jsonify({"ok": True, "id": cid, "totales": header,
                        "items_bloqueados": (not editable)})

    @app.route("/transporte/cotizaciones/api/listar", methods=["GET"])
    @_lc_api
    def lc_listar():
        mysql_fetchall = _h("mysql_fetchall")
        mysql_fetchone = _h("mysql_fetchone")
        chile_fmt = _h("chile_fmt_filter")

        estado = (request.args.get("estado") or "").strip().lower()
        q = (request.args.get("q") or "").strip()
        incluir_eliminadas = request.args.get("incluir_eliminadas") == "1" and _lc_solo_superadmin()

        where = ["1=1"]
        params = []
        if not incluir_eliminadas:
            where.append("COALESCE(eliminada,0)=0")
        if estado in _ESTADOS_VALIDOS:
            where.append("estado=%s")
            params.append(estado)
        if q:
            where.append("(numero LIKE %s OR empresa LIKE %s OR rut LIKE %s)")
            like = f"%{q}%"
            params.extend([like, like, like])

        try:
            page = max(1, int(request.args.get("page", 1)))
        except (TypeError, ValueError):
            page = 1
        try:
            limit = min(200, max(5, int(request.args.get("limit", 50))))
        except (TypeError, ValueError):
            limit = 50
        offset = (page - 1) * limit

        wsql = " AND ".join(where)
        total_row = mysql_fetchone(
            f"SELECT COUNT(*) AS n FROM transport_cotizaciones WHERE {wsql}", tuple(params))
        total = int(total_row["n"]) if total_row else 0

        rows = mysql_fetchall(
            f"SELECT id, numero, estado, empresa, rut, courier_nombre, comuna, "
            f"       peso_facturable_kg, costo_courier, total, reutilizada_en_id, "
            f"       created_by, created_at, updated_at "
            f"FROM transport_cotizaciones WHERE {wsql} "
            f"ORDER BY id DESC LIMIT %s OFFSET %s",
            tuple(params) + (limit, offset)) or []

        out = []
        for r in rows:
            out.append({
                "id": r["id"], "numero": r["numero"], "estado": r["estado"],
                "empresa": r.get("empresa"), "rut": r.get("rut"),
                "courier_nombre": r.get("courier_nombre"), "comuna": r.get("comuna"),
                "peso_facturable_kg": (float(r["peso_facturable_kg"])
                                       if r.get("peso_facturable_kg") is not None else None),
                "costo_courier": int(r.get("costo_courier") or 0),
                "total": int(r.get("total") or 0),
                "reutilizada_en_id": r.get("reutilizada_en_id"),
                "created_by": r.get("created_by"),
                "created_at": (chile_fmt(r.get("created_at")) if callable(chile_fmt) and r.get("created_at")
                               else str(r.get("created_at") or "")),
            })
        return jsonify({"ok": True, "total": total, "page": page, "limit": limit, "cotizaciones": out})

    @app.route("/transporte/cotizaciones/api/<int:cid>", methods=["GET"])
    @_lc_api
    def lc_detalle(cid):
        mysql_fetchone = _h("mysql_fetchone")
        mysql_fetchall = _h("mysql_fetchall")
        chile_fmt = _h("chile_fmt_filter")

        cot = mysql_fetchone(
            "SELECT * FROM transport_cotizaciones WHERE id=%s AND COALESCE(eliminada,0)=0", (cid,))
        if not cot:
            return jsonify({"ok": False, "error": "Cotización no encontrada."}), 404
        items = mysql_fetchall(
            "SELECT * FROM transport_cotizacion_items WHERE cotizacion_id=%s ORDER BY id", (cid,)) or []

        for k in ("created_at", "updated_at", "eliminada_at"):
            if cot.get(k) and callable(chile_fmt):
                cot[k] = chile_fmt(cot[k])
            elif cot.get(k) is not None:
                cot[k] = str(cot[k])
        if cot.get("valida_hasta") is not None:
            cot["valida_hasta"] = str(cot["valida_hasta"])

        return jsonify({"ok": True, "cotizacion": cot, "items": items,
                        "items_editables": _lc_items_editables(cot)})

    @app.route("/transporte/cotizaciones/api/<int:cid>/courier", methods=["POST"])
    @_lc_api
    def lc_set_courier(cid):
        mysql_fetchone = _h("mysql_fetchone")
        mysql_execute = _h("mysql_execute")
        current_username = _h("current_username")

        cot = mysql_fetchone(
            "SELECT id, estado FROM transport_cotizaciones WHERE id=%s AND COALESCE(eliminada,0)=0", (cid,))
        if not cot:
            return jsonify({"ok": False, "error": "Cotización no encontrada."}), 404
        if not _lc_items_editables(cot):
            return jsonify({"ok": False, "error": "Esta cotización ya no está en borrador: "
                            "no se puede cambiar el courier. Clónala si necesitas ajustarla."}), 409

        body = request.get_json(silent=True) or {}
        courier_nombre = (body.get("courier_nombre") or "").strip()
        costo_courier = _money(body.get("costo_courier"))
        if not courier_nombre or costo_courier <= 0:
            return jsonify({"ok": False, "error": "Falta courier_nombre o costo_courier válido."}), 400

        user = (current_username() if callable(current_username) else None) or "sistema"
        mysql_execute(
            "UPDATE transport_cotizaciones SET courier_id=%s, courier_nombre=%s, costo_courier=%s, "
            "peso_kg=%s, peso_vol_kg=%s, peso_facturable_kg=%s, comuna_resuelta=%s, updated_by=%s "
            "WHERE id=%s",
            (body.get("courier_id"), courier_nombre[:120], costo_courier,
             _f(body.get("peso_kg")) or None, _f(body.get("peso_vol_kg")) or None,
             _f(body.get("peso_facturable_kg")) or None,
             (body.get("comuna_resuelta") or "")[:100] or None, user, cid))

        header = _lc_recalcular(cid, user)
        _lc_log(cid, "cotizar_courier", {"courier_nombre": courier_nombre,
                                        "costo_courier": costo_courier}, user)
        return jsonify({"ok": True, "totales": header})

    @app.route("/transporte/cotizaciones/api/<int:cid>/items", methods=["POST"])
    @_lc_api
    def lc_items_agregar(cid):
        mysql_fetchone = _h("mysql_fetchone")
        mysql_execute = _h("mysql_execute")
        current_username = _h("current_username")

        cot = mysql_fetchone(
            "SELECT id, estado, margen_pct FROM transport_cotizaciones "
            "WHERE id=%s AND COALESCE(eliminada,0)=0", (cid,))
        if not cot:
            return jsonify({"ok": False, "error": "Cotización no encontrada."}), 404
        if not _lc_items_editables(cot):
            return jsonify({"ok": False, "error": "Esta cotización ya no está en borrador: "
                            "no se pueden agregar ítems. Clónala si necesitas ajustarla."}), 409

        body = request.get_json(silent=True) or {}
        items_in = body.get("items")
        if items_in is None and (body.get("app_id") or body.get("erp_kopr")):
            items_in = [body]
        if not isinstance(items_in, list) or not items_in:
            return jsonify({"ok": False, "error": "No se recibieron ítems para agregar."}), 400

        items_calc = _lc_enriquecer_items_con_medidas(items_in)
        user = (current_username() if callable(current_username) else None) or "sistema"
        for it in items_calc:
            mysql_execute(
                "INSERT INTO transport_cotizacion_items "
                "(cotizacion_id, erp_tido, erp_nudo, erp_kopr, descripcion, app_id, "
                " cantidad, total_bultos, peso_kg, volumen_cm3, margen_pct_aplicado, "
                " descuento_pct, notas) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (cid, it["erp_tido"], it["erp_nudo"], it["erp_kopr"], it["descripcion"],
                 it["app_id"], it["cantidad"], it["total_bultos"], it["peso_kg"],
                 it["volumen_cm3"], (it["margen_pct_aplicado"] or cot.get("margen_pct") or 30),
                 it["descuento_pct"], it["notas"]))

        header = _lc_recalcular(cid, user)
        _lc_log(cid, "editar", {"accion": "agregar_items", "cantidad": len(items_calc)}, user)
        return jsonify({"ok": True, "items_agregados": len(items_calc), "totales": header})

    @app.route("/transporte/cotizaciones/api/<int:cid>/items/<int:iid>", methods=["PATCH"])
    @_lc_api
    def lc_items_patch(cid, iid):
        mysql_fetchone = _h("mysql_fetchone")
        mysql_execute = _h("mysql_execute")
        current_username = _h("current_username")

        cot = mysql_fetchone(
            "SELECT id, estado FROM transport_cotizaciones WHERE id=%s AND COALESCE(eliminada,0)=0", (cid,))
        if not cot:
            return jsonify({"ok": False, "error": "Cotización no encontrada."}), 404
        if not _lc_items_editables(cot):
            return jsonify({"ok": False, "error": "Esta cotización ya no está en borrador: "
                            "no se pueden editar sus ítems (margen/descuento/cantidad). "
                            "Clónala si necesitas ajustarla."}), 409

        item = mysql_fetchone(
            "SELECT id, app_id, cantidad FROM transport_cotizacion_items "
            "WHERE id=%s AND cotizacion_id=%s", (iid, cid))
        if not item:
            return jsonify({"ok": False, "error": "Ítem no encontrado."}), 404

        body = request.get_json(silent=True) or {}
        sets, params = [], []

        if "descuento_pct" in body:
            sets.append("descuento_pct=%s")
            params.append(_pct(body.get("descuento_pct"), 0.0))
        if "margen_pct_aplicado" in body:
            valor = body.get("margen_pct_aplicado")
            sets.append("margen_pct_aplicado=%s")
            params.append(_pct(valor, 30.0) if valor not in (None, "") else None)
        if "notas" in body:
            sets.append("notas=%s")
            params.append((body.get("notas") or "").strip()[:2000] or None)
        if "cantidad" in body:
            try:
                nueva_cantidad = max(int(_f(body.get("cantidad"), item.get("cantidad") or 1)), 0)
            except (TypeError, ValueError):
                nueva_cantidad = item.get("cantidad") or 1
            recalc = (_lc_bultos_unitarios_batch([item["app_id"]]).get(item["app_id"])
                      if item.get("app_id") else None)
            if recalc:
                sets.extend(["cantidad=%s", "total_bultos=%s", "peso_kg=%s", "volumen_cm3=%s"])
                params.extend([
                    nueva_cantidad, int(recalc["total_bultos"]),
                    round(recalc["peso_kg_u"] * nueva_cantidad, 3),
                    round(recalc["volumen_cm3_u"] * nueva_cantidad, 2),
                ])
            else:
                sets.append("cantidad=%s")
                params.append(nueva_cantidad)

        if not sets:
            return jsonify({"ok": False, "error": "No se recibieron campos para actualizar."}), 400

        params.extend([iid, cid])
        mysql_execute(
            f"UPDATE transport_cotizacion_items SET {', '.join(sets)} "
            f"WHERE id=%s AND cotizacion_id=%s", tuple(params))

        user = (current_username() if callable(current_username) else None) or "sistema"
        header = _lc_recalcular(cid, user)
        _lc_log(cid, "editar", {"accion": "editar_item", "item_id": iid,
                                "campos": list(body.keys())}, user)
        return jsonify({"ok": True, "totales": header})

    @app.route("/transporte/cotizaciones/api/<int:cid>/items/<int:iid>", methods=["DELETE"])
    @_lc_api
    def lc_items_eliminar(cid, iid):
        mysql_fetchone = _h("mysql_fetchone")
        mysql_execute = _h("mysql_execute")
        current_username = _h("current_username")

        cot = mysql_fetchone(
            "SELECT id, estado FROM transport_cotizaciones WHERE id=%s AND COALESCE(eliminada,0)=0", (cid,))
        if not cot:
            return jsonify({"ok": False, "error": "Cotización no encontrada."}), 404
        if not _lc_items_editables(cot):
            return jsonify({"ok": False, "error": "Esta cotización ya no está en borrador: "
                            "no se pueden quitar ítems. Clónala si necesitas ajustarla."}), 409

        item = mysql_fetchone(
            "SELECT id, descripcion, erp_kopr FROM transport_cotizacion_items "
            "WHERE id=%s AND cotizacion_id=%s", (iid, cid))
        if not item:
            return jsonify({"ok": False, "error": "Ítem no encontrado."}), 404

        mysql_execute(
            "DELETE FROM transport_cotizacion_items WHERE id=%s AND cotizacion_id=%s", (iid, cid))

        user = (current_username() if callable(current_username) else None) or "sistema"
        header = _lc_recalcular(cid, user)
        _lc_log(cid, "editar", {"accion": "quitar_item", "item_id": iid,
                                "descripcion": item.get("descripcion") or item.get("erp_kopr")}, user)
        return jsonify({"ok": True, "totales": header or {}})

    @app.route("/transporte/cotizaciones/api/<int:cid>/recalcular", methods=["POST"])
    @_lc_api
    def lc_recalcular_endpoint(cid):
        mysql_fetchone = _h("mysql_fetchone")
        mysql_execute = _h("mysql_execute")
        current_username = _h("current_username")

        cot = mysql_fetchone(
            "SELECT id, estado FROM transport_cotizaciones WHERE id=%s AND COALESCE(eliminada,0)=0", (cid,))
        if not cot:
            return jsonify({"ok": False, "error": "Cotización no encontrada."}), 404

        body = request.get_json(silent=True) or {}
        if not _lc_items_editables(cot) and ("margen_pct" in body or "descuento_pct" in body or "iva_pct" in body):
            return jsonify({"ok": False, "error": "Esta cotización ya no está en borrador: "
                            "no se pueden cambiar margen/descuento/IVA globales."}), 409

        if _lc_items_editables(cot):
            sets, params = [], []
            if "margen_pct" in body:
                sets.append("margen_pct=%s"); params.append(_pct(body.get("margen_pct"), 30.0))
            if "descuento_pct" in body:
                sets.append("descuento_pct=%s"); params.append(_pct(body.get("descuento_pct"), 0.0))
            if "iva_pct" in body:
                sets.append("iva_pct=%s"); params.append(_pct(body.get("iva_pct"), 19.0))
            if sets:
                params.append(cid)
                mysql_execute(f"UPDATE transport_cotizaciones SET {', '.join(sets)} WHERE id=%s",
                              tuple(params))

        user = (current_username() if callable(current_username) else None) or "sistema"
        header = _lc_recalcular(cid, user)
        if header is None:
            return jsonify({"ok": False, "error": "Cotización no encontrada."}), 404
        return jsonify({"ok": True, "totales": header})

    @app.route("/transporte/cotizaciones/api/<int:cid>/estado", methods=["POST"])
    @_lc_api
    def lc_estado(cid):
        if not _lc_solo_superadmin():
            return jsonify({"ok": False, "error": "Solo un superadministrador puede aprobar, "
                            "rechazar o reabrir cotizaciones."}), 403

        mysql_fetchone = _h("mysql_fetchone")
        mysql_execute = _h("mysql_execute")
        current_username = _h("current_username")

        cot = mysql_fetchone(
            "SELECT id, estado FROM transport_cotizaciones WHERE id=%s AND COALESCE(eliminada,0)=0", (cid,))
        if not cot:
            return jsonify({"ok": False, "error": "Cotización no encontrada."}), 404

        body = request.get_json(silent=True) or {}
        nuevo = (body.get("estado") or "").strip().lower()
        # FIX 2026-07-25 (revisión adversarial, hallazgo ALTA): 'draft' YA
        # NO es un destino aceptado -- ver _ESTADO_TRANSICIONES_LC_ESTADO
        # arriba. Antes, un POST {"estado":"draft"} sobre una cotización
        # 'approved' la reabría a 'draft', lo que reactivaba
        # _lc_items_editables() y permitía agregar/quitar/editar ítems de
        # una cotización ya aprobada sin pasar por /clonar (la única vía
        # que exige la regla de negocio).
        if nuevo not in _ESTADO_TRANSICIONES_LC_ESTADO:
            return jsonify({"ok": False, "error": "Estado inválido. Usa 'approved' o "
                            "'rejected' (para reabrir la edición, clona la cotización)."}), 400

        if nuevo == "approved":
            n_items = mysql_fetchone(
                "SELECT COUNT(*) AS n FROM transport_cotizacion_items WHERE cotizacion_id=%s", (cid,))
            if not (n_items and n_items.get("n")):
                return jsonify({"ok": False, "error": "No se puede aprobar una cotización "
                                "sin ítems."}), 400

        user = (current_username() if callable(current_username) else None) or "sistema"
        mysql_execute(
            "UPDATE transport_cotizaciones SET estado=%s, updated_by=%s WHERE id=%s",
            (nuevo, user, cid))

        _lc_log(cid, _ESTADO_ACCION[nuevo], {"desde": cot.get("estado"), "a": nuevo}, user)
        return jsonify({"ok": True, "estado": nuevo})

    @app.route("/transporte/cotizaciones/api/<int:cid>/eliminar", methods=["POST"])
    @_lc_api
    def lc_eliminar(cid):
        if not _lc_solo_superadmin():
            return jsonify({"ok": False, "error": "Solo un superadministrador puede eliminar "
                            "cotizaciones."}), 403

        mysql_fetchone = _h("mysql_fetchone")
        mysql_execute = _h("mysql_execute")
        current_username = _h("current_username")

        cot = mysql_fetchone(
            "SELECT id, numero, estado, reutilizada_en_id FROM transport_cotizaciones "
            "WHERE id=%s AND COALESCE(eliminada,0)=0", (cid,))
        if not cot:
            return jsonify({"ok": False, "error": "Cotización no encontrada."}), 404

        puede, motivo = _lc_puede_eliminar(cot)
        if not puede:
            return jsonify({"ok": False, "error": motivo}), 409

        user = (current_username() if callable(current_username) else None) or "sistema"
        _lc_log(cid, "eliminar", {"numero": cot.get("numero")}, user)
        mysql_execute(
            "UPDATE transport_cotizaciones SET eliminada=1, eliminada_por=%s, eliminada_at=NOW() "
            "WHERE id=%s", (user, cid))
        return jsonify({"ok": True})

    @app.route("/transporte/cotizaciones/api/<int:cid>/clonar", methods=["POST"])
    @_lc_api
    def lc_clonar(cid):
        """Crea una cotización NUEVA en draft, copiando el snapshot de
        cliente/courier y los ítems de `cid`. Es la vía oficial para
        "editar" una cotización que ya no está en draft: en vez de tocar
        el original, se clona. El original queda marcado
        `reutilizada_en_id` = la nueva (y por regla de negocio ya no se
        puede eliminar mientras ese vínculo exista)."""
        get_mysql = _h("get_mysql")
        mysql_fetchone = _h("mysql_fetchone")
        mysql_fetchall = _h("mysql_fetchall")
        current_username = _h("current_username")

        original = mysql_fetchone(
            "SELECT * FROM transport_cotizaciones WHERE id=%s AND COALESCE(eliminada,0)=0", (cid,))
        if not original:
            return jsonify({"ok": False, "error": "Cotización no encontrada."}), 404
        items = mysql_fetchall(
            "SELECT * FROM transport_cotizacion_items WHERE cotizacion_id=%s ORDER BY id", (cid,)) or []

        user = (current_username() if callable(current_username) else None) or "sistema"

        # FIX 2026-07-25 (mismo hallazgo BLOQUEANTE que en lc_crear): se
        # recalculan los totales en Python ANTES del INSERT (a partir del
        # snapshot de items/courier/margen/descuento ya leído arriba) y se
        # graban ya resueltos dentro de la MISMA transacción de
        # get_mysql() -- ya NO se llama _lc_recalcular() en esta request
        # (esa relectura por la conexión pooled con REPEATABLE READ no ve
        # la fila recién insertada por la conexión fresca; ver el mismo
        # comentario extenso en lc_crear más arriba en este archivo). Como
        # beneficio adicional, si el original quedó con total=0 por este
        # mismo bug (ya corregido) antes de este fix, clonarlo ahora
        # recalcula bien la copia en vez de arrastrar el $0.
        items_out, header = _lc_calcular_totales(
            items, original.get("costo_courier"), original.get("margen_pct"),
            original.get("descuento_pct"), original.get("iva_pct"))

        conn = get_mysql()
        try:
            with conn.cursor() as cur:
                numero = _lc_generar_numero(cur)
                cur.execute(
                    "INSERT INTO transport_cotizaciones "
                    "(numero, estado, cliente_id, erp_idmaeen, erp_koen, rut, empresa, email, "
                    " telefono, direccion, direccion_lat, direccion_lng, direccion_place_id, "
                    " comuna, region, contacto_nombre, contacto_cargo, contacto_tel, contacto_email, "
                    " ejecutivo, courier_id, courier_nombre, peso_kg, peso_vol_kg, "
                    " peso_facturable_kg, comuna_resuelta, costo_courier, margen_pct, margen_monto, "
                    " subtotal_items, descuento_pct, descuento_monto, subtotal, iva_pct, iva_monto, "
                    " total, valida_hasta, notas, notas_internas, terminos, "
                    " created_by, updated_by) "
                    "VALUES (%s,'draft',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                    "        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (numero,
                     original.get("cliente_id"), original.get("erp_idmaeen"), original.get("erp_koen"),
                     original.get("rut"), original.get("empresa"), original.get("email"),
                     original.get("telefono"), original.get("direccion"), original.get("direccion_lat"),
                     original.get("direccion_lng"), original.get("direccion_place_id"),
                     original.get("comuna"), original.get("region"), original.get("contacto_nombre"),
                     original.get("contacto_cargo"), original.get("contacto_tel"),
                     original.get("contacto_email"), original.get("ejecutivo"),
                     original.get("courier_id"), original.get("courier_nombre"),
                     original.get("peso_kg"), original.get("peso_vol_kg"),
                     original.get("peso_facturable_kg"), original.get("comuna_resuelta"),
                     original.get("costo_courier"), original.get("margen_pct"), header["margen_monto"],
                     header["subtotal_items"], original.get("descuento_pct"), header["descuento_monto"],
                     header["subtotal"], original.get("iva_pct"), header["iva_monto"], header["total"],
                     original.get("valida_hasta"), original.get("notas"),
                     original.get("notas_internas"), original.get("terminos"),
                     user, user),
                )
                nuevo_id = cur.lastrowid
                for it, it_calc in zip(items, items_out):
                    cur.execute(
                        "INSERT INTO transport_cotizacion_items "
                        "(cotizacion_id, erp_tido, erp_nudo, erp_kopr, descripcion, app_id, "
                        " cantidad, total_bultos, peso_kg, volumen_cm3, costo_courier_prorrateo, "
                        " margen_pct_aplicado, margen_monto, descuento_pct, subtotal, total, notas) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (nuevo_id, it.get("erp_tido"), it.get("erp_nudo"), it.get("erp_kopr"),
                         it.get("descripcion"), it.get("app_id"), it.get("cantidad"),
                         it.get("total_bultos"), it.get("peso_kg"), it.get("volumen_cm3"),
                         it_calc["costo_courier_prorrateo"], it_calc["margen_pct_aplicado"],
                         it_calc["margen_monto"], it_calc["descuento_pct"], it_calc["subtotal"],
                         it_calc["total"], it.get("notas")),
                    )
                cur.execute(
                    "UPDATE transport_cotizaciones SET reutilizada_en_id=%s WHERE id=%s",
                    (nuevo_id, cid))
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[logistica_cotizaciones] clonar CRASH cid={cid}: {e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo clonar la cotización."}), 500
        finally:
            conn.close()

        # header ya calculado en Python antes del INSERT (ver comentario
        # arriba) -- NO releer con _lc_recalcular por la misma razón que en
        # lc_crear.
        _lc_log(nuevo_id, "crear", {"clonada_de": cid, "numero": numero}, user)
        _lc_log(cid, "clonada", {"nueva_id": nuevo_id, "nuevo_numero": numero}, user)
        return jsonify({"ok": True, "id": nuevo_id, "numero": numero, "clonada_de": cid,
                        "totales": header})

    @app.route("/transporte/cotizaciones/api/<int:cid>/log", methods=["GET"])
    @_lc_api
    def lc_log_get(cid):
        mysql_fetchall = _h("mysql_fetchall")
        chile_fmt = _h("chile_fmt_filter")
        rows = mysql_fetchall(
            "SELECT accion, usuario, detalle, created_at FROM transport_cotizacion_log "
            "WHERE cotizacion_id=%s ORDER BY created_at DESC, id DESC LIMIT 100", (cid,)) or []
        out = []
        for r in rows:
            out.append({
                "accion": r.get("accion"), "usuario": r.get("usuario"), "detalle": r.get("detalle"),
                "fecha": (chile_fmt(r.get("created_at")) if callable(chile_fmt) and r.get("created_at")
                         else str(r.get("created_at") or "")),
            })
        return jsonify({"ok": True, "eventos": out})

    @app.route("/transporte/cotizaciones/api/<int:cid>/calculo", methods=["GET"])
    @_lc_api
    def lc_calculo(cid):
        """Trazabilidad de la fórmula: cómo se llegó al total (prorrateo del
        flete por ítem, margen, descuentos, IVA)."""
        mysql_fetchone = _h("mysql_fetchone")
        mysql_fetchall = _h("mysql_fetchall")

        cot = mysql_fetchone(
            "SELECT * FROM transport_cotizaciones WHERE id=%s AND COALESCE(eliminada,0)=0", (cid,))
        if not cot:
            return jsonify({"ok": False, "error": "Cotización no encontrada."}), 404
        items = mysql_fetchall(
            "SELECT * FROM transport_cotizacion_items WHERE cotizacion_id=%s ORDER BY id", (cid,)) or []

        items_detalle = []
        for it in items:
            peso_pred = max(_f(it.get("peso_kg")), _f(it.get("volumen_cm3")) / _DIVISOR_VOLUMETRICO)
            share_pct = (round(it["costo_courier_prorrateo"] / cot["costo_courier"] * 100.0, 2)
                        if cot.get("costo_courier") else 0.0)
            items_detalle.append({
                "item_id": it["id"],
                "descripcion": it.get("descripcion") or it.get("erp_kopr"),
                "peso_predominante_kg": round(peso_pred, 3),
                "share_pct_del_flete": share_pct,
                "formula": (
                    f"Flete prorrateado {_clp(it['costo_courier_prorrateo'])} "
                    f"({share_pct:.1f}% de {_clp(cot.get('costo_courier') or 0)}) "
                    f"+ margen {float(it.get('margen_pct_aplicado') or 0):.1f}% "
                    f"({_clp(it['margen_monto'])}) = subtotal {_clp(it['subtotal'])}; "
                    f"descuento línea {float(it.get('descuento_pct') or 0):.1f}% "
                    f"→ total {_clp(it['total'])}."
                ),
                "costo_courier_prorrateo": int(it["costo_courier_prorrateo"]),
                "margen_pct_aplicado": float(it.get("margen_pct_aplicado") or 0),
                "margen_monto": int(it["margen_monto"]),
                "descuento_pct": float(it.get("descuento_pct") or 0),
                "subtotal": int(it["subtotal"]),
                "total": int(it["total"]),
            })

        header_formula = (
            f"Suma de ítems {_clp(cot.get('subtotal_items') or 0)} "
            f"− descuento global {float(cot.get('descuento_pct') or 0):.1f}% "
            f"({_clp(cot.get('descuento_monto') or 0)}) "
            f"= subtotal {_clp(cot.get('subtotal') or 0)}; "
            f"+ IVA {float(cot.get('iva_pct') or 0):.1f}% ({_clp(cot.get('iva_monto') or 0)}) "
            f"= total {_clp(cot.get('total') or 0)}."
        )

        return jsonify({
            "ok": True,
            "courier": {
                "courier_nombre": cot.get("courier_nombre"),
                "costo_courier": int(cot.get("costo_courier") or 0),
                "peso_kg": (float(cot.get("peso_kg")) if cot.get("peso_kg") is not None else None),
                "peso_vol_kg": (float(cot.get("peso_vol_kg")) if cot.get("peso_vol_kg") is not None else None),
                "peso_facturable_kg": (float(cot.get("peso_facturable_kg"))
                                       if cot.get("peso_facturable_kg") is not None else None),
                "comuna_resuelta": cot.get("comuna_resuelta"),
            },
            "items": items_detalle,
            "header": {
                "margen_pct": float(cot.get("margen_pct") or 0),
                "margen_monto": int(cot.get("margen_monto") or 0),
                "subtotal_items": int(cot.get("subtotal_items") or 0),
                "descuento_pct": float(cot.get("descuento_pct") or 0),
                "descuento_monto": int(cot.get("descuento_monto") or 0),
                "subtotal": int(cot.get("subtotal") or 0),
                "iva_pct": float(cot.get("iva_pct") or 0),
                "iva_monto": int(cot.get("iva_monto") or 0),
                "total": int(cot.get("total") or 0),
                "formula": header_formula,
            },
        })

    @app.route("/transporte/cotizaciones/api/<int:cid>/enviar", methods=["POST"])
    @_lc_api
    def lc_enviar(cid):
        """Envía el PDF de la cotización por correo (marca ILUS genérica,
        plantilla editable modulo='transporte_cotizaciones' -- Regla #11)."""
        mysql_fetchone = _h("mysql_fetchone")
        mysql_execute = _h("mysql_execute")
        current_username = _h("current_username")
        comm_is_enabled = _h("comm_is_enabled")
        _send_ilus_email = _h("_send_ilus_email")
        _brand_subject = _h("_brand_subject")
        _render_comm_template = _h("_render_comm_template")

        cot = mysql_fetchone(
            "SELECT * FROM transport_cotizaciones WHERE id=%s AND COALESCE(eliminada,0)=0", (cid,))
        if not cot:
            return jsonify({"ok": False, "error": "Cotización no encontrada."}), 404
        if callable(comm_is_enabled) and not comm_is_enabled("email"):
            return jsonify({"ok": False, "error": "El envío de correos está desactivado globalmente."})

        body = request.get_json(silent=True) or {}
        destinatarios_in = body.get("destinatarios") or []
        if not isinstance(destinatarios_in, list) or not destinatarios_in:
            return jsonify({"ok": False, "error": "Agrega al menos un destinatario."}), 400

        buenos, malos, vistos = [], [], set()
        for e in destinatarios_in[:15]:
            e = (str(e) or "").strip()
            if not e:
                continue
            if not _EMAIL_RE.match(e):
                malos.append(e)
            elif e.lower() not in vistos:
                vistos.add(e.lower())
                buenos.append(e)
        if malos:
            return jsonify({"ok": False, "error": "Hay correos inválidos: " + ", ".join(malos[:5])}), 400
        if not buenos:
            return jsonify({"ok": False, "error": "No hay destinatarios válidos."}), 400
        if not callable(_h("_pw_pdf")) or not callable(_send_ilus_email):
            return jsonify({"ok": False, "error": "El envío no está disponible en este entorno."}), 503

        try:
            res = _lc_cotizacion_pdf_bytes(cid)
        except Exception as e:
            print(f"[logistica_cotizaciones] enviar/pdf cid={cid}: {e}", flush=True)
            return jsonify({"ok": False, "error": "No se pudo generar el PDF para adjuntar."}), 503
        if not res:
            return jsonify({"ok": False, "error": "Cotización no encontrada."}), 404
        pdf_bytes, fname = res

        numero = cot.get("numero") or f"#{cid}"
        empresa = cot.get("empresa") or "cliente"
        mensaje = (body.get("mensaje") or "").strip()
        total_fmt = _clp(cot.get("total"))
        valida_str = _lc_fecha_str(cot.get("valida_hasta")) or ""

        subject_default = f"Cotización {numero} · {empresa}"
        subject, cuerpo_para_master = subject_default, None
        if callable(_render_comm_template):
            try:
                tpl = _render_comm_template(
                    "cotizacion_envio", "email",
                    {"numero_cotizacion": numero, "empresa": empresa, "total": total_fmt,
                     "valida_hasta": (f", válida hasta el {valida_str}" if valida_str else ""),
                     "mensaje": (f"<p>{_html_mod.escape(mensaje)}</p>" if mensaje else "")},
                    modulo="transporte_cotizaciones")
                if tpl:
                    asu, cue = tpl
                    if (asu or "").strip():
                        subject = asu.strip()
                    if (cue or "").strip():
                        cuerpo_para_master = cue
            except Exception as e:
                print(f"[logistica_cotizaciones] plantilla no usada cid={cid}: {e}", flush=True)
        if cuerpo_para_master is None:
            cuerpo_para_master = (
                f"<p>Estimados,</p>"
                f"<p>Adjuntamos la cotización de transporte y distribución "
                f"<strong>{_html_mod.escape(numero)}</strong> para "
                f"<strong>{_html_mod.escape(empresa)}</strong> por un total de "
                f"<strong>{_html_mod.escape(total_fmt)}</strong>"
                + (f", válida hasta el {_html_mod.escape(valida_str)}." if valida_str else ".") + "</p>"
                + (f"<p>{_html_mod.escape(mensaje)}</p>" if mensaje else "")
                + "<p>Quedamos atentos a sus comentarios.</p>"
            )
        if callable(_brand_subject):
            subject = _brand_subject(subject)

        # FIX 2026-07-27 (Daniel: "los correos llegan sin ningún diseño"):
        # antes `html_body` quedaba con el fragmento crudo de la plantilla
        # (un <p> + un div de color), sin el header/logo/footer de marca.
        # Se envuelve con _ilus_email_master, igual que hace el envío de
        # cotizaciones de Mantenciones (tk_api_cotizacion_enviar).
        _ilus_email_master = _h("_ilus_email_master")
        if callable(_ilus_email_master):
            try:
                html_body = _ilus_email_master({
                    "subject": subject,
                    "title": f"Cotización {numero}",
                    "subtitle": empresa,
                    "message": cuerpo_para_master,
                })
            except Exception as e:
                print(f"[logistica_cotizaciones] _ilus_email_master falló cid={cid}: {e}", flush=True)
                html_body = cuerpo_para_master
        else:
            html_body = cuerpo_para_master

        adj = [(fname, pdf_bytes, "application/pdf")]
        enviados, fallidos = [], []
        for dest in buenos:
            try:
                ok = _send_ilus_email(dest, subject, html_body, modulo="transporte", attachments=adj)
            except Exception as e:
                print(f"[logistica_cotizaciones] enviar {dest}: {e}", flush=True)
                ok = False
            (enviados if ok else fallidos).append(dest)

        user = (current_username() if callable(current_username) else None) or "sistema"
        if enviados and cot.get("estado") == "draft":
            mysql_execute("UPDATE transport_cotizaciones SET estado='sent', updated_by=%s WHERE id=%s",
                          (user, cid))
        _lc_log(cid, "enviar_correo", {"enviados": enviados, "fallidos": fallidos}, user)

        if not enviados:
            return jsonify({"ok": False, "error": "No se pudo enviar a ningún destinatario."}), 502
        return jsonify({"ok": True, "enviados": len(enviados), "fallidos": fallidos})

    _rutas_fase2 = [
        "settings [GET/POST]", "cotizar-flete [POST]", "crear [POST]", "listar [GET]",
        "<id> [GET]", "<id>/courier [POST]", "<id>/items [POST/PATCH/DELETE]",
        "<id>/recalcular [POST]", "<id>/estado [POST]", "<id>/eliminar [POST]",
        "<id>/clonar [POST]", "<id>/log [GET]", "<id>/calculo [GET]", "<id>/enviar [POST]",
    ]

    # ════════════════════════════════════════════════════════════════
    #  FASE 1 — PDF + visor (ya construidos). Gateados con _tr_required
    #  (permiso "transporte" existente) -- se dejan intactos, Regla #4.2.
    # ════════════════════════════════════════════════════════════════
    if _tr_required is None:
        print("[logistica_cotizaciones] _tr_required no disponible en app.py: "
              "/pdf y /ver (fase 1) NO fueron registradas, el resto del CRUD sí", flush=True)
    else:
        @app.route("/transporte/cotizaciones/<int:cid>/pdf")
        @_tr_required
        def lc_cotizacion_pdf(cid):
            """PDF de la cotizacion de transporte (Playwright). ?descargar=1
            fuerza la descarga (attachment) en vez de abrir inline."""
            mysql_fetchone = _h("mysql_fetchone")
            if not mysql_fetchone or not mysql_fetchone(
                    "SELECT id FROM transport_cotizaciones WHERE id=%s AND COALESCE(eliminada,0)=0", (cid,)):
                return "Cotización no encontrada", 404
            if not _h("_pw_pdf"):
                return "Generador de PDF no disponible en este entorno", 503
            try:
                _res = _lc_cotizacion_pdf_bytes(cid)
            except Exception as _e_pdf:
                print(f"[lc_cotizacion_pdf] cid={cid}: {_e_pdf}", flush=True)
                return ("El generador de PDF no está disponible ahora. "
                        "Intenta de nuevo en unos minutos."), 503
            if not _res:
                return "Cotización no encontrada", 404
            pdf_bytes, _fname = _res
            _disp = "attachment" if request.args.get("descargar") else "inline"
            return Response(pdf_bytes, mimetype="application/pdf",
                             headers={"Content-Disposition": f'{_disp}; filename="{_fname}"'})

        @app.route("/transporte/cotizaciones/<int:cid>/ver")
        @_tr_required
        def lc_cotizacion_ver(cid):
            """Visor rapido: la MISMA plantilla del PDF renderizada como HTML
            al instante (sin Playwright) -- calcado de tk_cotizacion_ver."""
            _res = _lc_cotizacion_pdf_ctx(cid)
            if not _res:
                return "Cotización no encontrada", 404
            ctx_pdf, cot = _res
            ctx_pdf["modo_visor"] = True
            ctx_pdf["logo_shs_html"] = _lc_logo_shs_html()
            return render_template("tickets/cotizacion_pdf.html", **ctx_pdf)

        @app.route("/transporte/cotizaciones")
        @_tr_required
        def lc_cotizaciones_lista():
            """Pantalla lista (templates/transporte/cotizaciones.html) -- la
            página que faltaba: sin esta ruta el módulo entero quedaba sin
            forma de llegar a él desde el navegador. Mismo patrón de filtros
            (q/estado) que tr_manifiestos()."""
            mysql_fetchall = _h("mysql_fetchall")
            chile_fmt = _h("chile_fmt_filter")

            estado = (request.args.get("estado") or "").strip().lower()
            q = (request.args.get("q") or "").strip()
            where = ["COALESCE(c.eliminada,0)=0"]
            params = []
            if estado in _ESTADOS_VALIDOS:
                where.append("c.estado=%s")
                params.append(estado)
            if q:
                where.append("(c.numero LIKE %s OR c.empresa LIKE %s OR c.rut LIKE %s)")
                like = f"%{q}%"
                params.extend([like, like, like])

            rows = mysql_fetchall(
                "SELECT c.*, r.numero AS reutilizada_en_numero "
                "FROM transport_cotizaciones c "
                "LEFT JOIN transport_cotizaciones r ON r.id = c.reutilizada_en_id "
                f"WHERE {' AND '.join(where)} "
                "ORDER BY c.id DESC LIMIT 500", tuple(params)) or []

            cotizaciones = []
            for r in rows:
                r = dict(r)
                if r.get("created_at"):
                    r["created_at"] = (chile_fmt(r["created_at"]) if callable(chile_fmt)
                                       else str(r["created_at"]))
                if r.get("valida_hasta") is not None:
                    r["valida_hasta"] = str(r["valida_hasta"])
                cotizaciones.append(r)

            return render_template("transporte/cotizaciones.html", cotizaciones=cotizaciones)

        @app.route("/transporte/cotizaciones/<int:cid>/detalle-calculo")
        @_tr_required
        def lc_cotizacion_detalle_calculo(cid):
            """Desglose interno del cálculo (courier, peso, margen,
            descuento) -- solo superadmin. Reusa la MISMA lógica que ya
            expone GET .../api/<id>/calculo (lc_calculo) invocando esa view
            function directamente -- mismo mecanismo de reuso que ya usa
            _lc_invocar_comparador para no duplicar cálculo."""
            if not _lc_solo_superadmin():
                return "No tienes permiso para ver el detalle de cálculo de esta cotización.", 403
            view = app.view_functions.get("lc_calculo")
            if view is None:
                return "El detalle de cálculo no está disponible en este momento.", 503
            resp = view(cid)
            data = resp.get_json(silent=True) if hasattr(resp, "get_json") else None
            if not data or not data.get("ok"):
                return "Cotización no encontrada", 404

            return render_template_string(
                """
{% extends "base.html" %}
{% block title %}Detalle de cálculo — Cotización #{{ cid }}{% endblock %}
{% block content %}
<div style="max-width:900px;margin:0 auto;">
  <h4 style="font-weight:800;margin-bottom:2px;">Detalle de cálculo — Cotización #{{ cid }}</h4>
  <p style="color:#6b7280;font-size:.85rem;margin-bottom:18px;">
    Courier: <strong>{{ data.courier.courier_nombre or '—' }}</strong> ·
    Costo flete: <strong>${{ '{:,}'.format(data.courier.costo_courier)|replace(',', '.') }}</strong>
    {% if data.courier.comuna_resuelta %} · Comuna: {{ data.courier.comuna_resuelta }}{% endif %}
  </p>
  <div class="table-responsive">
    <table class="table table-sm align-middle">
      <thead><tr>
        <th>Ítem</th><th class="text-end">Flete prorrateado</th><th class="text-end">Margen</th>
        <th class="text-end">Subtotal</th><th class="text-end">Desc. línea</th><th class="text-end">Total</th>
      </tr></thead>
      <tbody>
        {% for it in data.items %}
        <tr>
          <td>{{ it.descripcion or '—' }}</td>
          <td class="text-end">${{ '{:,}'.format(it.costo_courier_prorrateo)|replace(',', '.') }}</td>
          <td class="text-end">{{ it.margen_pct_aplicado }}% (${{ '{:,}'.format(it.margen_monto)|replace(',', '.') }})</td>
          <td class="text-end">${{ '{:,}'.format(it.subtotal)|replace(',', '.') }}</td>
          <td class="text-end">{{ it.descuento_pct }}%</td>
          <td class="text-end"><strong>${{ '{:,}'.format(it.total)|replace(',', '.') }}</strong></td>
        </tr>
        <tr><td colspan="6" style="color:#6b7280;font-size:.78rem;">{{ it.formula }}</td></tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
  <div style="margin-top:14px;padding:14px;background:#f8fafc;border-radius:10px;font-size:.85rem;">
    {{ data.header.formula }}
  </div>
  <p style="margin-top:18px"><a href="/transporte/cotizaciones">&larr; Volver a Cotizaciones</a></p>
</div>
{% endblock %}
                """,
                cid=cid, data=data)

        _rutas_fase2.append("<id>/pdf [GET] + <id>/ver [GET] + [GET] (lista) + "
                            "<id>/detalle-calculo [GET]")

    print("[logistica_cotizaciones] rutas registradas: " + ", ".join(_rutas_fase2), flush=True)
