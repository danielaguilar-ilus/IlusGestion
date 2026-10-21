"""REGLA #39 — estándar N° de serie + GPS en las plantillas, y una plantilla por cada clasificación.

Lógica pura (plantillas_serie_gps.py) + chequeos de texto sobre app.py y la plantilla (sin importar app.py).
"""
import os
import re

import plantillas_serie_gps as psg

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*ruta):
    with open(os.path.join(RAIZ, *ruta), encoding="utf-8") as f:
        return f.read()


def _it(i, titulo, tipo="check", orden=None):
    return {"id": i, "orden": i if orden is None else orden, "titulo": titulo, "tipo_respuesta": tipo}


# ── analizar / plan ─────────────────────────────────────────────────────────────────────────────────────────────
def test_plantilla_vacia_falta_ambos():
    p = psg.plan([])
    assert p["estado"] == "falta_ambos"
    assert p["acciones"] == ["agregar_serie", "agregar_gps"]
    assert p["agregan"] == [psg.SERIE_TITULO, psg.GPS_TITULO]


def test_plantilla_estandar_no_propone_nada():
    p = psg.plan([_it(1, "N° de serie", "serie"), _it(2, "Ubicación", "gps"), _it(3, "Revisar")])
    assert p["estado"] == "ok" and p["acciones"] == []


def test_texto_libre_de_serie_se_convierte_no_se_duplica():
    items = [_it(1, "Foto general", "foto"), _it(2, "N° de serie del equipo", "texto"), _it(3, "Ubicación", "gps")]
    p = psg.plan(items)
    assert p["acciones"] == ["convertir_serie"]
    assert p["convierte"] == "N° de serie del equipo" and p["id_convertible"] == 2
    assert p["agregan"] == []


def test_foto_de_serie_no_cuenta_como_objeto_ni_se_convierte():
    items = [_it(1, "Foto del N° de serie", "foto"), _it(2, "Foto de placa de serie", "texto")]
    p = psg.plan(items)
    assert p["acciones"] == ["agregar_serie", "agregar_gps"]


def test_pregunta_si_no_sobre_serie_no_se_convierte():
    p = psg.plan([_it(1, "¿Tiene N° de serie visible?", "sino")])
    assert "convertir_serie" not in p["acciones"] and "agregar_serie" in p["acciones"]


def test_solo_falta_gps():
    p = psg.plan([_it(1, "Serie", "serie")])
    assert p["estado"] == "falta_gps" and p["acciones"] == ["agregar_gps"]


# ── ordenes_para_aplicar ───────────────────────────────────────────────────────────────────────────────────────
def test_aplicar_agrega_arriba_y_desplaza_el_resto():
    c = psg.ordenes_para_aplicar([_it(1, "A"), _it(2, "B")])
    assert c["desplazar"] == 2 and c["convertir_id"] is None
    assert [(n["tipo_respuesta"], n["orden"]) for n in c["nuevos"]] == [("serie", 1), ("gps", 2)]


def test_aplicar_convierte_y_pone_arriba():
    c = psg.ordenes_para_aplicar([_it(1, "A"), _it(2, "N° de serie", "texto")])
    assert c["convertir_id"] == 2 and c["orden_convertido"] == 1
    assert [n["tipo_respuesta"] for n in c["nuevos"]] == ["gps"] and c["nuevos"][0]["orden"] == 2
    assert c["desplazar"] == 2


def test_aplicar_sin_cambios():
    c = psg.ordenes_para_aplicar([_it(1, "S", "serie"), _it(2, "G", "gps")])
    assert c["desplazar"] == 0 and c["nuevos"] == [] and c["convertir_id"] is None


def test_los_dos_objetos_son_obligatorios_y_completos():
    for it in (psg.item_serie(1), psg.item_gps(2)):
        assert it["obligatoria"] == 1 and it["titulo"] and it["tipo_respuesta"] in ("serie", "gps")
        assert {"descripcion", "requiere_foto", "foto_si_hallazgo", "unidad", "rango_min", "rango_max",
                "opciones_lista_json", "target_field"} <= set(it)


# ── huella ───────────────────────────────────────────────────────────────────────────────────────────────────────
def test_huella_cambia_si_la_plantilla_cambia():
    a = [_it(1, "A"), _it(2, "B")]
    assert psg.huella(a) == psg.huella([dict(x) for x in a])
    assert psg.huella(a) != psg.huella(a + [_it(3, "C")])
    assert psg.huella(a) != psg.huella([_it(1, "A"), _it(2, "B", "texto")])


# ── plantilla NUEVA nace con los dos objetos ──────────────────────────────────────────────────────────────────
def test_nueva_vacia_nace_con_serie_y_gps():
    out, agregados = psg.completar_items_nueva([])
    assert [i["tipo_respuesta"] for i in out] == ["serie", "gps"] and [i["orden"] for i in out] == [1, 2]
    assert len(agregados) == 2


def test_nueva_con_items_los_pone_primero_y_renumera():
    out, _ = psg.completar_items_nueva([
        {"orden": 1, "titulo": "Revisar", "tipo_respuesta": "check"},
        {"orden": 2, "titulo": "Foto", "tipo_respuesta": "foto"}])
    assert [i["tipo_respuesta"] for i in out] == ["serie", "gps", "check", "foto"]
    assert [i["orden"] for i in out] == [1, 2, 3, 4]


def test_nueva_que_ya_trae_el_estandar_no_se_duplica():
    items = [{"orden": 1, "titulo": "S", "tipo_respuesta": "serie"}, {"orden": 2, "titulo": "G", "tipo_respuesta": "gps"}]
    out, agregados = psg.completar_items_nueva(items)
    assert out == items and agregados == []


def test_nueva_con_serie_en_texto_la_convierte():
    out, agregados = psg.completar_items_nueva([
        {"orden": 1, "titulo": "Revisar", "tipo_respuesta": "check"},
        {"orden": 2, "titulo": "N° de serie del equipo", "tipo_respuesta": "texto"}])
    assert [i["tipo_respuesta"] for i in out] == ["serie", "gps", "check"]
    assert out[0]["titulo"] == "N° de serie del equipo" and out[0]["obligatoria"] == 1
    assert len(agregados) == 2


# ── LU y bloques ─────────────────────────────────────────────────────────────────────────────────────────────────
def test_lu_y_bloques():
    assert psg.es_lu("(LU) Trotadora") and psg.es_lu("  (lu) algo") and not psg.es_lu("Trotadora (LU)")
    assert psg.es_bloque_revision("Revisión — Registro por equipo") and not psg.es_bloque_revision("Instalación estándar")


# ── clases del catálogo sin plantilla ──────────────────────────────────────────────────────────────────────────
def test_faltantes_por_clase():
    clases = [{"id": 1, "nombre": "Selector de peso"}, {"id": 2, "nombre": "Trotadora"}]
    plantillas = [{"nombre": "selector de peso", "categoria_admin": "instalacion"},
                  {"nombre": "Trotadora", "categoria_admin": "instalacion"},
                  {"nombre": "Trotadora", "categoria_admin": "mantencion"}]
    falt = psg.faltantes_por_clase(clases, plantillas)
    pares = {(f["clase"], f["categoria"]) for f in falt}
    assert ("Selector de peso", "instalacion") not in pares
    assert ("Selector de peso", "mantencion") in pares and ("Selector de peso", "trabajo_interno") in pares
    assert ("Trotadora", "instalacion") not in pares and ("Trotadora", "mantencion") not in pares
    assert ("Trotadora", "visitas") in pares


def test_faltantes_ignora_nombres_vacios_y_repetidos():
    falt = psg.faltantes_por_clase([{"id": 1, "nombre": " "}, {"id": 2, "nombre": "Rack"}, {"id": 3, "nombre": "RACK"}], [])
    assert len([f for f in falt if f["categoria"] == "instalacion"]) == 1


# ── cableado en app.py y en la pantalla ────────────────────────────────────────────────────────────────────────
def test_app_tiene_las_rutas_y_la_guardia():
    app = _leer("app.py")
    for ruta in ("/mantenciones/api/plantillas/serie-gps\"", "/mantenciones/api/plantillas/serie-gps/aplicar",
                 "/mantenciones/api/plantillas/serie-gps/crear-clases"):
        assert ruta in app
    assert "import plantillas_serie_gps as _psg" in app
    assert "_psg.completar_items_nueva(items_norm)" in app
    # el arranque jamás aplica el estándar (REGLA #32): solo lo llaman los endpoints de una persona
    assert len(re.findall(r"_psg_aplicar_una\(", app)) == 2   # definición + una llamada
    assert "_psg_crear_clase(" in app


def test_las_lu_exigen_superadmin_y_marca_expresa():
    app = _leer("app.py")
    assert "_psg.es_lu(nombre) and not (es_super and confirma_lu)" in app


def test_la_bitacora_se_escribe_antes_de_tocar_los_items():
    app = _leer("app.py")
    i = app.index("def _psg_aplicar_una")
    cuerpo = app[i:i + 4500]
    assert cuerpo.index("serie_gps_estandar") < cuerpo.index("UPDATE mant_tarea_plantilla_items")


def test_pantalla_con_vista_previa_paginacion_y_marcar_todo():
    h = _leer("templates", "mantenciones", "plantillas.html")
    for marca in ("abrirSerieGps()", "modalSerieGps", "sgToggleTodo()", "Seleccionar/deseleccionar todo",
                  "por página", "Siguiente", "ilusConfirm(", "confirma_lu", "/mantenciones/api/plantillas/serie-gps"):
        assert marca in h, marca
    assert "confirm(" not in re.sub(r"ilusConfirm\(", "", h.split("REGLA #39) — ESTÁNDAR")[-1]).replace("sgToggleTodo", "")
