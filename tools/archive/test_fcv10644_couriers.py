"""
Test de validación FCV 10644 — caso muestra Lo Barnechea (173.89 kg).

Compara cada precio que devuelve el sistema vs los valores conocidos del
Excel maestro. Marca verde si difiere <=5%, rojo si difiere >5%.

Uso:
    python test_fcv10644_couriers.py

Requiere:
    - MySQL accesible (config.py).
    - Tabla transport_courier_comunas con datos de Lo Barnechea.
    - Tabla courier_tariff_audit creada.

NO toca datos; sólo lee.
"""
import json
import sys
import os

# Asegurar PYTHONPATH y cargar .env ANTES de importar app
sys.path.insert(0, os.path.dirname(__file__))
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
except Exception:
    pass  # dotenv opcional — si las env vars ya están seteadas, no hace falta


def _color(c, txt):
    codes = {'g': '\033[32m', 'r': '\033[31m', 'y': '\033[33m',
             'b': '\033[34m', 'c': '\033[36m', 'd': '\033[2m', 'x': '\033[0m'}
    return f"{codes.get(c, '')}{txt}{codes['x']}"


# Precios esperados extraídos del Excel maestro (auditoría 21/05/2026)
ESPERADO = {
    'fedex':                  105237,   # directo
    'starken':                116329,
    'clickex':                158793,
    'transporte felca':        92281,
    'transportes milling':     96599,
    'blue express':           187558,   # vía Envíame
    'transportes melling':     96599,   # alias
    'envíame':                133923,   # vía Starken
}

PESO_KG = 173.89
COMUNA  = "Lo Barnechea"
DOC_REF = "FCV 10644 - A.Y.M Asociados Limitada"
TOL_PCT = 5.0  # tolerancia 5%


def main():
    print()
    print(_color('c', '═' * 75))
    print(_color('c', f'  TEST FCV 10644 — Validación cotización Lo Barnechea (peso {PESO_KG} kg)'))
    print(_color('c', '═' * 75))
    print(f'  Documento: {DOC_REF}')
    print(f'  Comuna:    {COMUNA}')
    print(f'  Peso:      {PESO_KG} kg → bracket 100+')
    print(f'  Tolerancia: ±{TOL_PCT}%')
    print()

    # Import dentro de main para no penalizar errores en import time
    try:
        import app  # noqa: F401
        from app import (app as flask_app, _courier_tarifa_lookup, mysql_fetchall,
                          _courier_aplicar_margen_iva, _courier_margen_iva_cfg)
    except Exception as ex:
        print(_color('r', f"  ✗ No se pudo importar app.py: {ex}"))
        return 2

    # ── Mostrar política comercial activa (margen + IVA) ──────────
    try:
        cfg = _courier_margen_iva_cfg()
        print(_color('c', f'  Política ILUS — margen {cfg["margen_pct"]:.0f}% + IVA {cfg["iva_pct"]:.0f}%'))
        print()
    except Exception:
        pass

    # ── Push app_context para que mysql_fetchall pueda leer config ──
    flask_app.app_context().push()

    # Cargar couriers activos
    try:
        couriers = mysql_fetchall(
            "SELECT id, LOWER(nombre) AS k, nombre FROM transport_couriers WHERE activo=1"
        ) or []
    except Exception as ex:
        print(_color('r', f"  ✗ Error consultando BD: {ex}"))
        return 2

    if not couriers:
        print(_color('r', "  ✗ No hay couriers activos en BD."))
        return 2

    couriers_map = {c['k']: c for c in couriers}

    ok_count   = 0
    diff_count = 0
    sin_data   = 0
    sospechosos = 0

    print(f"  {'Courier':<22} {'Esp.Costo':>10} {'Costo':>10} {'Diff%':>6}  "
          f"{'Venta(cliente)':>14} {'Estado':<10}")
    print(_color('d', '  ' + '─' * 86))

    for c_key, esperado_clp in ESPERADO.items():
        c = couriers_map.get(c_key)
        if not c:
            sin_data += 1
            print(f"  {c_key:<24} {esperado_clp:>12,} {'(courier inactivo)':>22}  "
                  + _color('y', 'SIN DATA'))
            continue

        try:
            trace = _courier_tarifa_lookup(c['id'], COMUNA, PESO_KG, return_trace=True)
        except Exception as ex:
            print(f"  {c['nombre']:<24} {esperado_clp:>12,} {'ERROR':>12} "
                  + _color('r', f' ✗ {ex}'))
            sin_data += 1
            continue

        if not trace or trace.get('precio') is None:
            print(f"  {c['nombre']:<24} {esperado_clp:>12,} {'—':>12} "
                  + _color('y', 'SIN COBERT.'))
            sin_data += 1
            continue

        calc = int(round(trace['precio']))
        diff_pct = (calc - esperado_clp) / esperado_clp * 100 if esperado_clp else 0
        sosp = bool(trace.get('advertencias'))

        if abs(diff_pct) <= TOL_PCT:
            estado = _color('g', '✓ OK')
            ok_count += 1
        else:
            estado = _color('r', '✗ DIFIERE')
            diff_count += 1

        marker = ''
        if sosp:
            marker = _color('y', ' ⚠')
            sospechosos += 1

        validado_mark = _color('b', '✓v') if trace.get('validado') else ''
        bracket_used = trace.get('bracket_aplicado', '?')

        # Calcular precio VENTA con margen + IVA (lo que el cliente paga)
        try:
            dsg = _courier_aplicar_margen_iva(calc)
            venta_str = f"${int(dsg['precio_venta']):,}".replace(',', '.')
        except Exception:
            venta_str = "?"

        diff_str = f"{diff_pct:+.1f}%"
        print(f"  {c['nombre']:<22} {esperado_clp:>10,} {calc:>10,} "
              f"{diff_str:>6}  {venta_str:>14} {estado:<22}  "
              f"{_color('d', f'[{bracket_used}]')} {validado_mark}{marker}")

        if sosp:
            for adv in trace['advertencias']:
                print(_color('y', f'      ⚠ {adv}'))

    print(_color('d', '  ' + '─' * 72))
    print()
    print(f"  Resumen:  "
          f"{_color('g', f'{ok_count} OK')}  "
          f"{_color('r', f'{diff_count} difieren')}  "
          f"{_color('y', f'{sin_data} sin data')}  "
          f"{_color('y', f'{sospechosos} sospechosos')}")

    # Veredicto final
    total_validos = ok_count + diff_count
    if diff_count == 0 and sin_data == 0:
        print()
        print(_color('g', '  ╭───────────────────────────────────────╮'))
        print(_color('g', '  │  TODOS LOS COURIERS DENTRO DE TOLER.  │'))
        print(_color('g', '  ╰───────────────────────────────────────╯'))
        rc = 0
    elif diff_count == 0:
        print()
        print(_color('y', '  ⚠ Sin diferencias, pero faltan couriers con data.'))
        rc = 0
    else:
        print()
        print(_color('r', f'  ✗ {diff_count} courier(s) fuera de tolerancia ±{TOL_PCT}%'))
        print(_color('r', '    → Ejecuta /transporte/couriers/<id> + "Marcar validada"'))
        print(_color('r', '      o vía API POST /api/transporte/couriers/<id>/tarifa-validar'))
        rc = 1

    print()
    print(_color('c', '  Tarifas históricas en audit log:'))
    try:
        rows = mysql_fetchall(
            "SELECT courier_nombre, bracket_aplicado, precio_calculado, validado, fuente, "
            "       cotizacion_at "
            "FROM courier_tariff_audit "
            "WHERE LOWER(TRIM(comuna))=LOWER(TRIM(%s)) "
            "ORDER BY cotizacion_at DESC LIMIT 15",
            (COMUNA,)
        ) or []
        if not rows:
            print(_color('d', '    (sin entradas — el audit se llenará al primer cotizar)'))
        else:
            for r in rows:
                v = _color('g', '✓v') if r['validado'] else _color('d', '—')
                ts = str(r.get('cotizacion_at') or '')[:16]
                print(f"    {ts:<16} {r['courier_nombre']:<22} "
                      f"{r['bracket_aplicado']:<6} "
                      f"${r['precio_calculado']:>10,}  "
                      f"{_color('d', r['fuente']):<20}  {v}")
    except Exception as ex:
        print(_color('y', f'    (no se pudo leer audit: {ex})'))

    print()
    return rc


if __name__ == '__main__':
    sys.exit(main())
