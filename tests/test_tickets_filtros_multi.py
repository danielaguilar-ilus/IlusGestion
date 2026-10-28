"""Filtros de la bandeja de Tickets (Daniel 2026-10-28): buscador general y
selección múltiple de estado, tipo, prioridad y responsable en UN parámetro."""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import tickets_module as tm  # noqa: E402


def W(**args):
    return tm._tk_list_where(args)


def test_sin_filtros_no_hay_where():
    assert W() == ("", [])


def test_un_solo_valor_sigue_igual_que_antes():
    w, p = W(estado="open", tipo="repair", prioridad="alta")
    assert "t.estado=%s" in w and "t.tipo=%s" in w and "t.prioridad=%s" in w
    assert p == ["open", "repair", "alta"]


def test_estados_multiples_van_en_un_IN_y_descartan_los_invalidos():
    w, p = W(estado="open,in_progress,inventado")
    assert "t.estado IN (%s,%s)" in w
    assert p == ["open", "in_progress"]


def test_activos_sigue_siendo_el_atajo():
    w, p = W(estado="__activos__")
    assert "NOT IN ('resolved','closed','cancelado')" in w and p == []


def test_tipo_y_prioridad_multiples():
    w, p = W(tipo="repair,quotation", prioridad="urgente,alta")
    assert "t.tipo IN (%s,%s)" in w and "t.prioridad IN (%s,%s)" in w
    assert p == ["repair", "quotation", "urgente", "alta"]


def test_responsables_multiples_con_barra_y_sin_asignar_en_OR():
    w, p = W(asignado_a="Ana Pérez|Luis Soto|__sin_asignar__")
    assert "t.asignado_a IN (%s,%s)" in w
    assert "t.asignado_a IS NULL" in w and " OR " in w
    assert p == ["Ana Pérez", "Luis Soto"]


def test_solo_sin_asignar_y_un_nombre_con_coma_no_se_parte():
    w, p = W(asignado_a="__sin_asignar__")
    assert "IS NULL" in w and p == []
    w, p = W(asignado_a="Pérez, Ana")
    assert p == ["Pérez, Ana"]


def test_buscador_general_cubre_correo_telefono_y_motivo():
    w, p = W(q="ana@x.cl")
    for col in ("t.numero_ticket", "t.empresa", "t.nombre_contacto", "t.rut", "t.email",
                "t.phone", "t.sucursal", "t.motivo_principal", "t.descripcion", "t.titulo"):
        assert col + " LIKE %s" in w
    assert p == ["%ana@x.cl%"] * 10


def test_buscador_numerico_tambien_busca_por_id():
    w, p = W(q="355")
    assert "t.id=%s" in w and 355 in p


def test_buscador_encuentra_un_rut_con_o_sin_puntos():
    w, p = W(q="76.123.456-7")
    assert "REPLACE(REPLACE(t.rut" in w and "%761234567%" in p
    w, p = W(q="76123456")
    assert "REPLACE(REPLACE(t.rut" in w


def test_valores_siempre_parametrizados_nunca_pegados_al_sql():
    w, p = W(q="'; DROP TABLE tk_tickets; --", estado="open", asignado_a="x'; --")
    assert "DROP" not in w and "--" not in w
    assert not re.search(r"'[^']*x'", w)


def test_el_conteo_de_chips_no_pierde_selecciones_porque_viajan_en_un_parametro():
    # _tk_conteo_estados hace {k: v for k, v in args.items()}: con un MultiDict
    # solo sobrevive el PRIMER valor de una clave repetida. Por eso van juntos.
    from werkzeug.datastructures import MultiDict
    args = MultiDict([("tipo", "repair,quotation"), ("asignado_a", "Ana|Luis")])
    copia = {k: v for k, v in args.items()}
    w, p = tm._tk_list_where(copia)
    assert p == ["repair", "quotation", "Ana", "Luis"]


def test_la_cotizacion_desde_el_ticket_hereda_el_documento():
    # Daniel 2026-10-28: el número de factura y los datos del ticket se heredan.
    js = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "static", "tickets_cotizaciones.js"), encoding="utf-8").read()
    i = js.index("async function _cotWizAplicarDeepLinkTicket")
    cuerpo = js[i:i + 6000]
    assert "d.documentos" in cuerpo
    assert "it.tido = docsTk[0].erp_tido" in cuerpo and "_WIZ.docRef" in cuerpo
