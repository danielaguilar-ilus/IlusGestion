# -*- coding: utf-8 -*-
"""
════════════════════════════════════════════════════════════════════════
INDICADORES FINANCIEROS DE COTIZACIONES — módulo compartido
════════════════════════════════════════════════════════════════════════
Encargo de Daniel (20-08-2026): *"a ese panel le hace falta vida, hagamos
algo espectacular pero no tan llamativo, pero que contengan indicadores
financieros, períodos, filtros y buscadores, indicadores asociados a cada
Dpto SSTT y Logística, ambos tienen cotizadores"* + *"Standariza el de
logística y SSTT"*.

POR QUÉ UN MÓDULO Y NO CÓDIGO EN CADA PANEL
ILUS tiene DOS cotizadores, con dos tablas distintas:
  · SSTT      → `tk_cotizaciones`        (numeración COT-)
  · Logística → `transport_cotizaciones` (numeración CTR-)
Calcular "cuánto cotizamos, cuánto ganamos y cuánto se aprobó" por
separado en cada uno es la receta ya conocida: dos copias que un día
divergen y muestran cifras distintas para la misma pregunta. Hoy mismo
costó caro — el contador de tareas obligatorias llegó a tener SEIS copias
y la pantalla se contradecía a sí misma. Acá el cálculo vive una vez.

QUÉ SE PUEDE CALCULAR DE VERDAD (verificado contra los CREATE TABLE)
Las dos tablas comparten: estado, subtotal, descuento_monto, iva_monto,
total, valida_hasta, ticket_id, created_at. Y las dos guardan COSTO, que
es lo que permite el margen:
  · Logística → `costo_courier` (y ya trae margen_pct/margen_monto)
  · SSTT      → `costo_tecnico` + `costo_ruta`
Por eso el costo entra como EXPRESIÓN SQL parametrizada por el caller, en
vez de asumir un nombre de columna que solo existe en una de las dos.

🔴 NO se inventa ningún indicador que no tenga dato detrás. Si mañana se
quiere "ticket promedio por vendedor" hay que mirar antes si el vendedor
está en la tabla — no estimarlo.

El ENUM de estado es IDÉNTICO en ambas ('draft','sent','approved',
'rejected','expired'); lo que difería era la traducción en pantalla (una
mostraba "DRAFT" en inglés). Acá se traduce una sola vez para las dos.
════════════════════════════════════════════════════════════════════════
"""

from datetime import date, timedelta

# Traducción única de estados. Antes cada template resolvía lo suyo y el
# panel de SSTT mostraba "DRAFT" crudo en inglés, contra la regla del
# proyecto de que nada visible va en inglés.
COTIZ_ESTADOS = {
    "draft":    ("Borrador",  "#6b7280", "#f3f4f6", "bi-pencil-fill"),
    "sent":     ("Enviada",   "#2563eb", "#dbeafe", "bi-send-fill"),
    "approved": ("Aprobada",  "#15803d", "#dcfce7", "bi-check-circle-fill"),
    "rejected": ("Rechazada", "#b91c1c", "#fee2e2", "bi-x-circle-fill"),
    "expired":  ("Vencida",   "#a16207", "#fef3c7", "bi-clock-history"),
}

COTIZ_PERIODOS = (
    ("hoy",   "Hoy"),
    ("7d",    "7 días"),
    ("mes",   "Este mes"),
    ("trim",  "Trimestre"),
    ("anio",  "Año"),
    ("todo",  "Todo"),
)


def cotiz_estado_meta(estado):
    """(label, color, fondo, icono) de un estado. Nunca revienta: un valor
    desconocido se muestra tal cual en gris en vez de romper la página."""
    e = (estado or "").strip().lower()
    if e in COTIZ_ESTADOS:
        return COTIZ_ESTADOS[e]
    return (e.title() or "—", "#6b7280", "#f3f4f6", "bi-question-circle")


def cotiz_rango_periodo(periodo, hoy=None):
    """Traduce el período elegido a (desde, hasta) como fechas.

    Devuelve (None, None) para 'todo' — el caller omite el filtro.
    `hoy` se inyecta para poder testear sin depender del reloj.
    """
    h = hoy or date.today()
    p = (periodo or "mes").strip().lower()
    if p == "hoy":
        return h, h
    if p == "7d":
        return h - timedelta(days=6), h
    if p == "mes":
        return h.replace(day=1), h
    if p == "trim":
        # Trimestre calendario en curso (ene-mar, abr-jun, jul-sep, oct-dic).
        ini_mes = 1 + 3 * ((h.month - 1) // 3)
        return h.replace(month=ini_mes, day=1), h
    if p == "anio":
        return h.replace(month=1, day=1), h
    return None, None


def cotiz_kpis(mysql_fetchone, tabla, expr_costo,
               periodo="mes", where_extra="", params_extra=(), hoy=None):
    """Indicadores financieros de un panel de cotizaciones.

    Args:
      mysql_fetchone: el helper del proyecto (se inyecta para no importar
                      app.py desde acá y crear un import circular).
      tabla:          'tk_cotizaciones' | 'transport_cotizaciones'
      expr_costo:     expresión SQL del costo. Ej: 'COALESCE(costo_courier,0)'
                      o 'COALESCE(costo_tecnico,0)+COALESCE(costo_ruta,0)'.
                      🔴 La arma el CALLER con literales del código, NUNCA
                      con entrada del usuario (iría concatenada al SQL).
      where_extra:    condiciones extra ya parametrizadas con %s.

    Returns: dict listo para el template. Todos los montos en pesos.
    """
    desde, hasta = cotiz_rango_periodo(periodo, hoy)

    where = ["1=1"]
    params = []
    # `eliminada` solo existe en transporte; se filtra únicamente ahí para
    # no romper la consulta de SSTT con una columna inexistente.
    if tabla == "transport_cotizaciones":
        where.append("COALESCE(eliminada,0)=0")
    if desde and hasta:
        where.append("DATE(created_at) BETWEEN %s AND %s")
        params.extend([desde, hasta])
    if where_extra:
        where.append(where_extra)
        params.extend(list(params_extra))

    sql = (
        "SELECT COUNT(*) AS n, "
        "  COALESCE(SUM(total),0) AS monto, "
        f"  COALESCE(SUM({expr_costo}),0) AS costo, "
        "  SUM(CASE WHEN estado='approved' THEN 1 ELSE 0 END) AS n_aprob, "
        "  COALESCE(SUM(CASE WHEN estado='approved' THEN total ELSE 0 END),0) AS monto_aprob, "
        f"  COALESCE(SUM(CASE WHEN estado='approved' THEN {expr_costo} ELSE 0 END),0) AS costo_aprob, "
        "  SUM(CASE WHEN estado='sent' THEN 1 ELSE 0 END) AS n_env, "
        "  COALESCE(SUM(CASE WHEN estado='sent' THEN total ELSE 0 END),0) AS monto_env, "
        "  SUM(CASE WHEN estado='rejected' THEN 1 ELSE 0 END) AS n_rech, "
        "  SUM(CASE WHEN estado='draft' THEN 1 ELSE 0 END) AS n_draft "
        f" FROM {tabla} WHERE " + " AND ".join(where)
    )

    try:
        r = mysql_fetchone(sql, tuple(params)) or {}
    except Exception as e:
        # Un panel sin KPIs sigue siendo usable; uno que revienta, no.
        print(f"[cotiz_kpis] {tabla}: {e}", flush=True)
        r = {}

    def _n(k):
        try:
            return float(r.get(k) or 0)
        except (TypeError, ValueError):
            return 0.0

    monto_aprob = _n("monto_aprob")
    costo_aprob = _n("costo_aprob")
    margen_aprob = monto_aprob - costo_aprob

    # Conversión sobre lo DECIDIDO (aprobadas + rechazadas), no sobre el
    # total: las que siguen en borrador o enviadas todavía no dijeron que
    # no, y meterlas en el denominador hunde el número sin motivo.
    n_aprob, n_rech = _n("n_aprob"), _n("n_rech")
    decididas = n_aprob + n_rech

    return {
        "periodo": periodo,
        "desde": desde, "hasta": hasta,
        "n": int(_n("n")),
        "monto": _n("monto"),
        "costo": _n("costo"),
        "margen": _n("monto") - _n("costo"),
        "margen_pct": round((_n("monto") - _n("costo")) / _n("monto") * 100, 1) if _n("monto") else 0.0,
        "n_aprob": int(n_aprob),
        "monto_aprob": monto_aprob,
        "margen_aprob": margen_aprob,
        "margen_aprob_pct": round(margen_aprob / monto_aprob * 100, 1) if monto_aprob else 0.0,
        "n_env": int(_n("n_env")),
        "monto_env": _n("monto_env"),
        "n_rech": int(n_rech),
        "n_draft": int(_n("n_draft")),
        "conversion": round(n_aprob / decididas * 100, 1) if decididas else None,
        "decididas": int(decididas),
    }
