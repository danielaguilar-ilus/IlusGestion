#!/usr/bin/env python3
"""Gate de regresión del Agente ILUS de contratos (party-aware, sin IA).

Por qué existe (REGLA #9 + auditoría 2026-06-16): el parser de contratos
`_contrato_extraer_determinista` distingue al CLIENTE de ILUS (el arrendador).
Un bug real metía el correo del representante de ILUS en el campo dirección.
Este test extrae las funciones del parser de `app.py` SIN levantar Flask/BD
(las lee con `ast`), y verifica con casos sintéticos que:

  - no se confunden los datos de ILUS con los del cliente,
  - una dirección nunca es un correo/RUT,
  - se respetan negaciones, cargos, multas, conectores de parte, etc.

Cómo correrlo (antes de pushear):  python tests/test_contrato_parser.py
Sale con código != 0 si algo falla → sirve como gate.
"""
import ast
import os
import re
import sys
import time
import unicodedata

APP = os.path.join(os.path.dirname(__file__), "..", "app.py")

# Funciones/constantes del parser que se extraen de app.py (deben ser top-level).
WANT_FUN = {
    "_rut_cuerpo", "_norm_txt", "_es_rut_ilus", "_es_email_ilus", "_es_nombre_ilus",
    "_es_direccion_ilus", "_es_tel_ilus", "_marcador_ilus_cerca", "_parece_email",
    "_parece_rut", "_valida_direccion", "_valida_comuna", "_contrato_partes",
    "_contrato_fechas", "_contrato_equipos", "_contrato_extraer_determinista",
}
WANT_CONST = {"_ILUS_RUT_CUERPOS", "_ILUS_EMAIL_DOMINIOS", "_ILUS_DIR_TOKENS", "_ILUS_TEL_DIGITOS"}


def cargar_parser():
    src = open(APP, encoding="utf-8").read()
    tree = ast.parse(src)
    chunks, presentes = [], set()
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in WANT_FUN:
            chunks.append(ast.get_source_segment(src, node))
            presentes.add(node.name)
        elif isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name) and tgt.id in WANT_CONST:
                    chunks.append(ast.get_source_segment(src, node))
                    presentes.add(tgt.id)
    # PRESENCIA: si el fix de segmentación no está, falla en ROJO (no KeyError silencioso).
    faltan = (WANT_FUN | WANT_CONST) - presentes
    if faltan:
        print("FALLO DE PRESENCIA: faltan en app.py ->", ", ".join(sorted(faltan)))
        print("El fix party-aware del Agente de contratos no está en app.py.")
        sys.exit(2)
    ns = {"re": re}
    exec("\n\n".join(chunks), ns)
    return ns["_contrato_extraer_determinista"]


_NA = lambda s: "".join(c for c in unicodedata.normalize("NFD", (s or "").lower())
                       if unicodedata.category(c) != "Mn")

ok = fail = 0


def chk(cond, desc):
    global ok, fail
    print(("  ok   " if cond else "  FAIL ") + desc)
    ok += bool(cond)
    fail += (not cond)


def main():
    E = cargar_parser()

    # ── Caso ancla: contrato de arriendo tipo "comparecencia" (el bug original) ──
    real = ("EN SANTIAGO COMPARECEN: SPORTS AND HEALTH SOLUTIONS SpA, RUT 76.996.964-0, representada por "
            "don FELIPE GOMEZ, correo felipe.gomez@sphs.cl, domiciliada en Avenida Apoquindo 4499, Las Condes, "
            "ARRENDADORA, por una parte; y por la otra, FIT CREW ENTERTAINMENT LIMITADA, RUT 78.401.622-6, "
            "representada por don MAURICIO SILVA RIVERA, cedula 16.233.843-9, domiciliados en CACHAPOAL 473 "
            "ACHUPALLAS, Comuna Vina del Mar, Ciudad Vina del Mar, Region Valparaiso, ARRENDATARIA; exponen.")
    c = E(real)["cliente"]
    chk("@" not in (c["direccion"] or ""), "[ancla] dirección no es un correo")
    chk("sphs.cl" not in (c["contacto_email"] or "").lower(), "[ancla] email no es @sphs.cl (ILUS)")
    chk((c["rut"] or "").startswith("78.401.622"), "[ancla] RUT = cliente FIT CREW (no ILUS)")
    chk("FIT CREW" in (c["razon_social"] or "").upper(), "[ancla] razón social = FIT CREW")
    chk("CACHAPOAL" in (c["direccion"] or "").upper(), "[ancla] dirección = cliente (no Apoquindo)")
    chk("SILVA" in (c["contacto_nombre"] or "").upper(), "[ancla] contacto = rep del cliente")

    # ── Orden invertido (cliente primero) ──
    s1 = ("EN SANTIAGO COMPARECEN: COMERCIAL DELTA SPA, RUT 77.222.333-4, representada por don PEDRO SOTO, "
          "domiciliados en Los Olmos 55, Comuna Nunoa, en adelante el CLIENTE, por una parte; y por la otra, "
          "SPORTS AND HEALTH SOLUTIONS SpA, RUT 76.996.964-0, ARRENDADORA; los comparecientes exponen.")
    c = E(s1)["cliente"]
    chk((c["rut"] or "").startswith("77.222"), "[invertido] RUT = cliente Delta")
    chk("DELTA" in (c["razon_social"] or "").upper(), "[invertido] razón = Delta")

    # ── Conector alterno "de la otra parte" ──
    s7 = ("COMPARECEN SPORTS AND HEALTH SOLUTIONS SpA RUT 76.996.964-0 ARRENDADORA, por una parte; y de la "
          "otra parte, CLUB ANDES SPA, RUT 77.300.400-5, Comuna Rancagua, ARRENDATARIA; exponen.")
    c = E(s7)["cliente"]
    chk((c["rut"] or "").startswith("77.300"), "[conector] RUT = cliente Andes")
    chk((c["comuna"] or "") == "Rancagua", "[conector] comuna = Rancagua")

    # ── Correo externo (gmail) del representante de ILUS NO se toma ──
    s8 = ("SPORTS AND HEALTH SOLUTIONS SpA, RUT 76.996.964-0, ARRENDADORA, representada por don Felipe Gomez, "
          "correo felipe.gomez.ilus@gmail.com. GIMNASIO OMEGA SPA, RUT 77.600.700-8, Comuna Temuco, ARRENDATARIA.")
    c = E(s8)["cliente"]
    chk((c["contacto_email"] or "") == "", "[email-ilus] gmail del rep de ILUS no se toma")

    # ── Email-trampa en campo Dirección etiquetado ──
    s3 = "Razon social: CLUB SUR\nRUT: 77.888.999-0\nDireccion: ventas@trampa.cl\nComuna: La Florida\n"
    c = E(s3)["cliente"]
    chk((c["direccion"] or "") in ("", None) or "@" not in c["direccion"], "[trampa] correo no entra en dirección")
    chk((c["contacto_email"] or "") == "ventas@trampa.cl", "[trampa] correo va a contacto_email")
    chk((c["comuna"] or "") == "La Florida", "[trampa] comuna = La Florida")

    # ── Negación "NO incluye mantenciones gratuitas" ──
    s9 = ("Razon social: GIMNASIO POWER SPA\nRUT 76.123.456-7\nComuna: Maipu\n"
          "El contrato NO incluye mantenciones gratuitas. Mantencion trimestral. Valor mensual $300.000.")
    r = E(s9)
    chk(r["contrato"]["incluye_mant_gratis"] is False, "[negación] no marca mant gratis")
    chk(r["contrato"]["frecuencia_meses"] == 3, "[negación] frecuencia = 3")

    # ── Cargo del representante ──
    s10 = ("COMPARECEN ... por una parte; y por la otra, EMPRESA SUR LIMITADA, RUT 77.800.900-1, "
           "representada por su Gerente General don JUAN PEREZ, cedula 12.345.678-9, Comuna Concepcion, ARRENDATARIA.")
    c = E(s10)["cliente"]
    chk("gerente" in (c["contacto_cargo"] or "").lower(), "[cargo] cargo = Gerente General")
    chk("PEREZ" in (c["contacto_nombre"] or "").upper(), "[cargo] nombre = Juan Perez")

    # ── Multa no debe ganar como costo_total ──
    s11 = ("Razon social: CLUB NORTE SPA\nRUT 77.111.222-3\nComuna: Iquique\n"
           "La renta de arrendamiento sera mensualmente $453.782. En caso de mora la multa sera de $5.000.000.")
    ct = E(s11)["contrato"]
    chk(ct["costo_total"] == 453782, "[multa] costo_total = renta (no multa)")
    chk(ct["monto_mensual"] == 453782, "[multa] monto_mensual = renta")

    # ── Dos clientes → aviso ──
    s12 = ("COMPARECEN ILUS ARRENDADORA por una parte; y por la otra, GIMNASIO A SPA RUT 77.111.111-1 y "
           "GIMNASIO B SPA RUT 77.222.222-2, ambos arrendatarios, Comuna Vina del Mar, ARRENDATARIA; exponen.")
    al = E(s12)["contrato"]["alertas"]
    chk(any("mas de un cliente" in _NA(a) for a in al), "[multi] avisa de 2+ clientes")

    # ── Solo ILUS → no filtra datos de ILUS como cliente ──
    s6 = ("SPORTS AND HEALTH SOLUTIONS SpA, RUT 76.996.964-0, correo felipe.gomez@sphs.cl, "
          "domiciliada en Avenida Apoquindo 4499, Las Condes.")
    c = E(s6)["cliente"]
    chk(not c["rut"] and not c["contacto_email"] and not c["direccion"], "[solo-ilus] no toma datos de ILUS")

    # ── Cota de tiempo (anti-DoS): 200KB adversarial < 600ms ──
    adv = ("domiciliado en " + "a" * 30 + " direccion correo ") * 7000
    adv = adv[:200000]
    t0 = time.perf_counter()
    E(adv)
    dt = time.perf_counter() - t0
    chk(dt < 0.6, f"[perf] 200KB adversarial en {dt * 1000:.0f}ms (<600ms)")

    print(f"\n===== {ok} ok, {fail} FAIL =====")
    sys.exit(1 if fail else 0)


if __name__ == "__main__":
    main()
