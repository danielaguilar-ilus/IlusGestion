"""La compra de repuestos hereda los documentos de Random de sus OT y tickets
de origen (Daniel 2026-10-28), solo en la descripción INTERNA del ticket de
compra: nunca en el correo al proveedor (REGLAS #19 y #26)."""
import os

_APP = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app.py")
_SRC = open(_APP, encoding="utf-8").read()


def _helper():
    ini = _SRC.index("def _otrep_docs_origen(sols):")
    fin = _SRC.index("def _otrep_crear_compra():", ini)
    ns = {}

    def fake_fetchall(sql, params=()):
        if "mant_visita_documentos" in sql:
            return [{"visita_id": 10, "erp_tido": "FCV", "erp_nudo": "11439"},
                    {"visita_id": 10, "erp_tido": "NVV", "erp_nudo": "5001"}]
        if "tk_ticket_documentos" in sql:
            return [{"ticket_id": 77, "erp_tido": "BLV", "erp_nudo": "23732"},
                    {"ticket_id": 77, "erp_tido": "FCV", "erp_nudo": "11439"}]
        return []
    ns["mysql_fetchall"] = fake_fetchall
    exec(compile(_SRC[ini:fin], "otrep_docs_origen", "exec"), ns)
    return ns["_otrep_docs_origen"]


def test_junta_los_documentos_de_la_ot_y_del_ticket_sin_repetir():
    f = _helper()
    sols = [{"id": 1, "visita_id": 10, "ticket_id": 77},
            {"id": 2, "visita_id": None, "ticket_id": 77},
            {"id": 3, "visita_id": None, "ticket_id": None}]
    out = f(sols)
    assert out[1] == ["FCV-11439", "NVV-5001", "BLV-23732"]
    assert out[2] == ["BLV-23732", "FCV-11439"]
    assert out[3] == []


def test_un_fallo_de_lectura_no_rompe_la_compra():
    ini = _SRC.index("def _otrep_docs_origen(sols):")
    fin = _SRC.index("def _otrep_crear_compra():", ini)
    ns = {"mysql_fetchall": lambda *a, **k: (_ for _ in ()).throw(RuntimeError("bd caída"))}
    exec(compile(_SRC[ini:fin], "x", "exec"), ns)
    assert ns["_otrep_docs_origen"]([{"id": 1, "visita_id": 10}]) == {}


def test_crear_compra_lo_usa_y_el_correo_al_proveedor_no_lo_lleva():
    ini = _SRC.index("def _otrep_crear_compra():")
    fin = _SRC.index("\ndef ", ini + 10)
    crear = _SRC[ini:fin]
    assert "_otrep_docs_origen(sols)" in crear and "Documentos de origen" in crear
    # No se toca lo que alimenta el candado «documento ya asociado a un ticket».
    assert "INSERT INTO tk_ticket_documentos" not in crear
    assert "numero_documento" not in crear
    i = _SRC.index("def _otrep_compra_enviar_correo")
    j = _SRC.index("\ndef ", i + 10)
    assert "docs_origen" not in _SRC[i:j] and "Documentos de origen" not in _SRC[i:j]
