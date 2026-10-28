"""Ficha del ticket (Daniel 2026-10-28): «Estado y avance» con barra de tiempo
como Retiros, y la identificación del cliente con cada dato en su propia ficha
(antes eran tres hijos sueltos en una grilla de 2 columnas y cada etiqueta
caía corrida un lugar: el RUT quedaba bajo «EMPRESA»)."""
import os
import re

_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*ruta):
    return open(os.path.join(_RAIZ, *ruta), encoding="utf-8").read()


def test_cada_dato_del_cliente_es_una_ficha_con_su_etiqueta_y_su_valor():
    js = _leer("static", "tickets_ficha.js")
    i = js.index("function tkIdFilaHtml(campo, t){")
    cuerpo = js[i:i + 1500]
    # Un solo contenedor por dato: etiqueta (dt) y valor (dd) juntos.
    assert cuerpo.count("tk-id-item") >= 1
    assert "<dt class=\"tk-id-item-top\">" in cuerpo and "<dd data-field=" in cuerpo
    assert "</dd></div>" in cuerpo
    # El ícono ya no es un hijo suelto de la grilla.
    assert "return '<i class=\"bi bi-'+campo.icon" not in cuerpo


def test_la_ficha_trae_el_bloque_estado_y_avance_con_barra_de_tiempo():
    html = _leer("templates", "tickets", "ficha.html")
    for ident in ("tkEstadoAvance", "eaEstado", "eaPct", "eaBarraAv", "eaSla", "eaBarraT", "eaSlaSub"):
        assert f'id="{ident}"' in html, ident
    # va antes de la guía (a la vista apenas se abre el ticket)
    assert html.index('id="tkEstadoAvance"') < html.index('id="tkGuia"')


def test_el_js_pinta_estado_avance_y_plazo_desde_lo_que_calcula_el_servidor():
    js = _leer("static", "tickets_ficha_v2.js")
    assert "function pintarEstadoAvance(t, d)" in js
    assert "pintarEstadoAvance(t, d)" in js.split("window.tkV2AlCargar = function")[1]
    cuerpo = js[js.index("function pintarEstadoAvance"):js.index("window.tkV2AlCargar = function")]
    assert "d.guia" in cuerpo and "t.sla_resp" in cuerpo and "espera_h" in cuerpo
    # la ficha nunca se cae por este bloque
    assert re.search(r"try \{ pintarEstadoAvance\(t, d\); \} catch", js)


def test_la_bandeja_tambien_muestra_la_barra_de_tiempo_del_sla():
    js = _leer("static", "tickets_list.js")
    i = js.index("function tkSlaCelda(tk)")
    assert 'class="barra"' in js[i:i + 2500]
    assert ".tkc-sla .barra" in _leer("static", "tickets_list.css")


def test_el_tracking_calcula_horas_quien_y_tiempos_y_una_reapertura_no_pisa_el_ingreso():
    import shutil
    import subprocess
    import tempfile
    node = shutil.which("node")
    if not node:
        import pytest
        pytest.skip("node no disponible")
    js = _leer("static", "tickets_ficha.js")
    bloque = js[js.index("function tkParseCl(s){"):js.index("function renderStepper(t, mensajes){")]
    prueba = r"""
function actMeta(r){ return !r ? {} : (typeof r === 'object' ? r : JSON.parse(r)); }
function tkStepIndexParaEstado(e){ if(e==='open') return 0; if(['in_progress','ot_generated'].includes(e)) return 1; if(e==='resolved') return 2; if(e==='closed') return 3; return 0; }
""" + bloque + r"""
const assert = require('assert');
const t = {created_at: '27/10/2026 09:10', created_by: 'ana'};
const msgs = [
  {tipo:'cambio_estado', created_at:'27/10/2026 11:25', usuario:'daniel.aguilar', metadata:{nuevo:'in_progress'}},
  {tipo:'cambio_estado', created_at:'27/10/2026 16:40', usuario:'victor.cabrera', metadata:{nuevo:'resolved'}},
  {tipo:'cambio_estado', created_at:'28/10/2026 08:05', usuario:'sistema', metadata:{nuevo:'open'}},
  {tipo:'cambio_estado', created_at:'28/10/2026 08:20', usuario:'daniel.aguilar', metadata:{nuevo:'in_progress'}},
];
let p = tkTrackingPuntos(t, msgs, 1);
assert.strictEqual(p[0].txt, '27/10/2026 09:10');          // el ingreso no se pisa
assert.strictEqual(p[1].txt, '28/10/2026 08:20');          // En Curso: la marca vigente
assert.strictEqual(p[2], null);                            // Resuelto ya no vale tras reabrir
assert.strictEqual(p.reab.txt, '28/10/2026 08:05');
// Sin reapertura: ingreso -> En Curso -> Resuelto, con sus horas y quién.
p = tkTrackingPuntos(t, msgs.slice(0, 2), 2);
assert.strictEqual(p[1].quien, 'daniel.aguilar'); assert.strictEqual(p[2].txt, '27/10/2026 16:40');
assert.strictEqual(tkDurTxt(p[2].ts - p[1].ts), '5 h 15 min');
assert.strictEqual(tkDurTxt(p[1].ts - p[0].ts), '2 h 15 min');
assert.strictEqual(tkDurTxt(50 * 3600000), '2 días 2 h');
assert.strictEqual(tkNombreCorto('daniel.aguilar@sphs.cl'), 'Daniel Aguilar');
assert.strictEqual(tkNombreCorto(''), 'Automático');
assert.strictEqual(tkFechaCorta('27/10/2026 09:10'), '27/10 09:10');
console.log('OK');
"""
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
        f.write(prueba)
        ruta = f.name
    r = subprocess.run([node, ruta], capture_output=True, text=True, timeout=60)
    assert r.returncode == 0 and "OK" in r.stdout, r.stderr or r.stdout
