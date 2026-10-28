"""Tickets 2.0 · Guía del ticket (tickets_guia.py, módulo puro).
El sistema SUGIERE el siguiente paso; nunca decide por la persona (REGLA #32)."""
import tickets_guia as G


def _t(**k):
    base = {"estado": "open", "asignado_a": "", "titulo": "Trotadora no enciende", "descripcion": "x",
            "rut": "76123456-7", "empresa": "Gimnasio Uno", "nombre_contacto": "Ana", "motivo_principal": "",
            "diagnostico_solucion": "", "visita_id": None, "visita_numero_ot": "", "visita_estado": "",
            "n_equipos": 1, "n_adjuntos": 0, "n_cotizaciones": 0, "n_repuestos": 0, "contactos_con_via": 1,
            "unread_count": 0, "sla": None, "es_tecnico": False}
    base.update(k)
    return base


def _claves(g):
    return [p["clave"] for p in g["pasos"]]


def test_orden_de_pasos_fijo_y_por_clave():
    g = G.evaluar(_t())
    assert _claves(g) == [c for c, _ in G.PASOS]
    assert g["progreso"]["total"] == len(G.PASOS) - len(G.OPCIONALES)


def test_sin_responsable_lo_primero_es_hacerse_cargo():
    g = G.evaluar(_t())
    assert g["siguiente"]["clave"] == "responsable"
    assert "tomar" in g["siguiente"]["acciones"]
    assert [p for p in g["pasos"] if p["clave"] == "responsable"][0]["estado"] == "actual"


def test_con_responsable_y_cliente_esperando_toca_responder_con_el_sla():
    sla = {"nivel": "vencido", "pill": "Vencido hace 2 h", "sub": "llegó ayer 16:10"}
    g = G.evaluar(_t(asignado_a="Victor", sla=sla))
    s = g["siguiente"]
    assert s["clave"] == "respuesta" and "responder" in s["acciones"] and s["tono"] == "vencido"
    assert "Vencido hace 2 h" in s["texto"]
    g2 = G.evaluar(_t(asignado_a="Victor", sla={"nivel": "ok", "pill": "Quedan 3 h"}))
    assert g2["siguiente"]["clave"] == "respuesta" and g2["siguiente"]["tono"] == "actual"


def test_respondido_sigue_con_motivo_luego_gestion_luego_diagnostico():
    base = _t(asignado_a="Victor", sla={"nivel": "respondido", "pill": "En orden"})
    assert G.evaluar(base)["siguiente"]["clave"] == "motivo"
    base["motivo_principal"] = "Banda desgastada"
    s = G.evaluar(base)["siguiente"]
    assert s["clave"] == "gestion" and {"generar_ot", "asociar_ot", "cotizar", "repuesto"} <= set(s["acciones"])
    base["visita_id"] = 9
    base["visita_numero_ot"] = "OT-2026-00009"
    base["visita_estado"] = "en_curso"
    s = G.evaluar(base)["siguiente"]
    assert s["clave"] == "gestion" and s["tono"] == "espera" and "OT-2026-00009" in s["titulo"]
    base["visita_estado"] = "cerrada"
    assert G.evaluar(base)["siguiente"]["clave"] == "diagnostico"
    base["diagnostico_solucion"] = "Se cambió la banda y se calibró."
    s = G.evaluar(base)["siguiente"]
    assert s["clave"] == "cierre" and "resolver" in s["acciones"]


def test_un_tecnico_no_recibe_la_sugerencia_de_cotizar():
    g = G.evaluar(_t(asignado_a="Lenin", motivo_principal="x", es_tecnico=True,
                     sla={"nivel": "respondido"}, n_cotizaciones=3))
    assert "cotizar" not in g["siguiente"]["acciones"]
    gestion = [p for p in g["pasos"] if p["clave"] == "gestion"][0]
    assert "cotizaci" not in gestion["resumen"] and "cotizar" not in gestion["acciones"]


def test_resuelto_sin_diagnostico_pide_el_diagnostico_antes_de_cerrar():
    g = G.evaluar(_t(estado="resolved", asignado_a="Victor"))
    assert g["siguiente"]["clave"] == "diagnostico"
    g2 = G.evaluar(_t(estado="resolved", asignado_a="Victor", diagnostico_solucion="Listo"))
    assert g2["siguiente"]["clave"] == "cierre" and "cerrar" in g2["siguiente"]["acciones"]


def test_cerrado_no_pide_nada_y_la_respuesta_no_queda_pendiente():
    g = G.evaluar(_t(estado="closed", sla={"nivel": "cerrado"}))
    assert g["cerrado"] and g["siguiente"]["tono"] == "fin" and g["siguiente"]["acciones"] == []
    assert [p for p in g["pasos"] if p["clave"] == "respuesta"][0]["estado"] == "hecho"


def test_cliente_incompleto_dice_exactamente_que_falta():
    g = G.evaluar(_t(asignado_a="Victor", rut="", sla={"nivel": "respondido"}))
    assert g["siguiente"]["clave"] == "cliente" and "RUT" in g["siguiente"]["texto"]


def test_progreso_cuenta_solo_obligatorios():
    g = G.evaluar(_t(asignado_a="Victor", motivo_principal="m", diagnostico_solucion="d", sla={"nivel": "respondido"}))
    p = g["progreso"]
    # cliente, contactos, problema, equipos, responsable, respuesta, motivo, diagnóstico = 8 de 9 (falta cierre)
    assert (p["hechos"], p["total"]) == (8, 9) and p["pct"] == 89
