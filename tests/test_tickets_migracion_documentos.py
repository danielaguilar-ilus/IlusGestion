"""
TEST FILE -- NO ES PRODUCCION. NO IMPORTAR DESDE app.py.

Migracion de DOCUMENTOS de los tickets historicos (2026-09-06).

CONTEXTO (Daniel, textual): "Si, es necesario. Por calidad de informacion,
ya que mientras mas calidad mejor queda la informacion. Los tickets deben
guardar el documento y los productos indexados."

EL HUECO: hasta ahora el documento del ticket vivia como TEXTO LIBRE en
tk_tickets.numero_documento ("FCV-11439", "fcv 11439", "11439"...). La tabla
estructurada tk_ticket_documentos (tido/nudo) solo la llenaban los caminos
que nacen de un documento ERP. Sin esa fila el ticket no se cruza con su
cotizacion, con su OT ni con el candado de "este documento ya tiene ticket".

LO QUE ESTE TEST DEFIENDE (lo caro de equivocarse):
  1. NO ADIVINAR. "11439" a secas, "factura 11439 y 11440" o "boleta 23140"
     NO se convierten en un documento: no se sabe el tipo, y cruzar mal dos
     documentos manda plata y equipos al cliente que no es.
  2. La simulacion NO ESCRIBE. Abrir la pantalla no puede tocar la base.
  3. Es IDEMPOTENTE: lo que ya esta registrado no se vuelve a insertar
     (ni siquiera escrito distinto: "FCV-011439" == "FCV-11439").
  4. Las dos rutas nuevas existen y no chocan con ninguna vista ya
     registrada (dos vistas con el mismo nombre impiden que la app arranque).

Se corre igual que el resto de la bateria:
    python tests/test_tickets_migracion_documentos.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

fallos = []


def check(cond, msg):
    if cond:
        print(f"  OK   {msg}")
    else:
        print(f"  FALLA {msg}")
        fallos.append(msg)


import tickets_module as tm  # noqa: E402


# ══════════════════════════════════════════════════════════════════
#  1. EL LECTOR DE TEXTO — casos reales del campo `numero_documento`
#     y del `documento_garantia` de los equipos.
# ══════════════════════════════════════════════════════════════════
print("\n1) Lector de documentos escritos a mano")

CASOS_OK = [
    ("FCV-11439",            [("FCV", "11439")]),
    ("fcv 11439",            [("FCV", "11439")]),
    ("FCV11439",             [("FCV", "11439")]),
    ("FCV: 11439",           [("FCV", "11439")]),
    ("FCV N° 11439",         [("FCV", "11439")]),
    ("FCV-0011439",          [("FCV", "11439")]),   # ceros a la izquierda: formato display
    ("(FCV 11439)",          [("FCV", "11439")]),
    ("VD-6162",              [("VD", "6162")]),
    ("WEB-1234",             [("WEB", "1234")]),
    ("GUIA DE DESPACHO GDV 4455", [("GDV", "4455")]),
    # Formato que escribe tk_api_crear_desde_documento cuando el ticket
    # nace de VARIOS documentos ERP: ", ".join("TIDO-NUDO").
    ("FCV-11439, NVV-2233",  [("FCV", "11439"), ("NVV", "2233")]),
    ("BLV 23140 / FCV 11303", [("BLV", "23140"), ("FCV", "11303")]),
]
for txt, esperado in CASOS_OK:
    r = tm._tk_docs_desde_texto(txt)
    check(r["estado"] == "ok" and r["docs"] == esperado,
          f"reconoce {txt!r} -> {esperado}")

CASOS_NO = [
    "11439",                    # sin tipo: puede ser factura, boleta o nota
    "factura 11439 y 11440",    # dos numeros y ningun tipo del ERP
    "boleta 23140",
    "NOTA DE VENTA 11439",
    "guia 123",                 # 'GUIA' no es un TIDO de Random
    "OT 1234",
    "TK-2026-00012",
    "GR2024",                   # numero de serie: tipo de 2 letras pegado
    "N/A", "-", "SIN DOCUMENTO", "no lo tiene",
    "FCV-0000",                 # numero vacio
]
for txt in CASOS_NO:
    r = tm._tk_docs_desde_texto(txt)
    check(r["estado"] == "no_reconocido" and not r["docs"],
          f"NO adivina {txt!r}")

CASOS_PARCIAL = [
    "FCV 11439 y 11440",          # el 11440 puede ser otra factura
    "FCV 11439 del 12/03/2026",   # fecha pegada
    "RUT 76.996.964-0 FCV 11439",
]
for txt in CASOS_PARCIAL:
    r = tm._tk_docs_desde_texto(txt)
    check(r["estado"] == "parcial" and r["docs"],
          f"marca como PARCIAL (no migra solo) {txt!r}")

check(tm._tk_docs_desde_texto("")["estado"] == "vacio",
      "un campo vacio no es un error, es 'vacio'")
check(tm._tk_doc_partes("GUIA 123") == ("GUIA", "123"),
      "_tk_doc_partes conserva su comportamiento historico (sin whitelist)")
check(tm._tk_doc_partes("GUIA 123", solo_conocidos=True) is None,
      "_tk_doc_partes con solo_conocidos=True exige un TIDO real del ERP")
check(tm._tk_doc_partes("FCV-11439", solo_conocidos=True) == ("FCV", "11439"),
      "_tk_doc_partes con solo_conocidos=True sigue aceptando lo bueno")


# ══════════════════════════════════════════════════════════════════
#  2. LA MIGRACION — se registra en una app Flask de mentira con una
#     base de datos falsa: nada toca MySQL ni el ERP.
# ══════════════════════════════════════════════════════════════════
print("\n2) Migracion sobre una base falsa")

from flask import Flask  # noqa: E402

ESCRITURAS = []          # toda query que pase por mysql_execute

TICKETS = [
    # id, numero, numero_documento
    {"id": 10, "numero_ticket": "TK-2026-00010", "numero_documento": "FCV-11439",
     "created_at": None},
    {"id": 11, "numero_ticket": "TK-2026-00011", "numero_documento": "11439",
     "created_at": None},
    {"id": 12, "numero_ticket": "TK-2026-00012", "numero_documento": "FCV 500 y 501",
     "created_at": None},
    {"id": 13, "numero_ticket": "TK-2026-00013", "numero_documento": "BLV-23140",
     "created_at": None},   # este YA esta registrado (idempotencia)
    {"id": 14, "numero_ticket": "TK-2026-00014", "numero_documento": None,
     "created_at": None},   # sin texto, pero sus equipos traen el documento
]
DOCS_YA = [{"ticket_id": 13, "erp_tido": "BLV", "erp_nudo": "0023140"}]
EQ_COUNT = [{"ticket_id": 13, "n": 2}]          # 10, 11, 12 y 14 sin equipos
EQ_GARANTIA = [{"ticket_id": 14, "documento_garantia": "GDV-4455"}]


def fake_fetchall(q, params=None):
    ql = " ".join(str(q).split())
    if "FROM tk_tickets ORDER BY id DESC" in ql:
        return list(TICKETS)
    if "FROM tk_ticket_documentos" in ql and "erp_tido" in ql:
        return list(DOCS_YA)
    if "COUNT(*) AS n FROM tk_ticket_equipos" in ql:
        return list(EQ_COUNT)
    if "documento_garantia" in ql and "DISTINCT" in ql:
        return list(EQ_GARANTIA)
    return []


def fake_fetchone(q, params=None):
    return None


def fake_execute(q, params=None):
    ESCRITURAS.append((" ".join(str(q).split()), params))


class _FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, q, params=None):
        fake_execute(q, params)

    def fetchone(self):
        return None

    def fetchall(self):
        return []

    lastrowid = 1


class _FakeConn:
    def cursor(self):
        return _FakeCursor()

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def fake_login_required(view):
    return view


app = Flask(__name__, template_folder=os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates"))
app.config["TESTING"] = True

ctx = {
    "mysql_fetchone": fake_fetchone,
    "mysql_fetchall": fake_fetchall,
    "mysql_execute": fake_execute,
    "get_mysql": lambda: _FakeConn(),
    "login_required": fake_login_required,
    "current_username": lambda: "daniel.aguilar@sphs.cl",
}

tm.register_tickets_routes(app, ctx)
ESCRITURAS.clear()          # lo que escribio el arranque (CREATE/seed) no cuenta

rutas = {r.endpoint for r in app.url_map.iter_rules()}
check("tk_admin_migrar_documentos" in rutas, "existe la pantalla /tickets/admin/migrar-documentos")
check("tk_api_migrar_documentos" in rutas, "existe el endpoint POST /tickets/api/admin/migrar-documentos")

migrar = app.config.get("_tk_migrar_documentos_historicos")
check(callable(migrar), "la migracion queda expuesta en app.config para tests/uso programatico")

# ── 2.1 SIMULACION: no escribe NADA ──────────────────────────────
sim = migrar(aplicar=False)
check(not ESCRITURAS, "la simulacion no escribio ni una fila")
check(sim["modo"] == "simulacion", "la simulacion se identifica como tal")
check(sim["tickets_total"] == 5, "cuenta los 5 tickets")
check(sim["con_documento_reconocible"] == 3,
      f"3 tickets con documento reconocible (10, 13 y 14) — dio {sim['con_documento_reconocible']}")
check(sim["parciales"] == 1, "el ticket 12 ('FCV 500 y 501') queda como parcial")
check(sim["no_reconocidos"] == 1, "el ticket 11 ('11439' suelto) queda como NO reconocido")
check(sim["sin_texto"] == 0, "el ticket 14 no cuenta como 'sin texto': su equipo trae el documento")
check(sim["documentos_a_insertar"] == 2,
      f"solo faltan 2 documentos (FCV-11439 y GDV-4455) — dio {sim['documentos_a_insertar']}")
check(sim["ya_completos"] == 1, "el ticket 13 ya estaba completo (BLV-0023140 == BLV-23140)")
check(sim["tickets_sin_productos"] == 2,
      "los tickets 10 y 14 tienen documento y ningun equipo: candidatos a indexar")

pares_sim = [d for m in sim["muestra_a_insertar"] for d in m["docs"]]
check(sorted(pares_sim) == ["FCV-11439", "GDV-4455"],
      f"la muestra dice exactamente que se insertaria: {pares_sim}")
check(any("11439" in (d.get("texto") or "") for m in sim["muestra_no_reconocidos"]
          for d in m["detalle"]),
      "la muestra de no reconocidos trae el TEXTO ORIGINAL para poder decidir")

# ── 2.2 APLICAR: escribe solo lo reconocido, sin tocar el ERP ─────
res = migrar(aplicar=True, verificar_erp=False, indexar_productos=False, limit=100)
inserts = [(q, p) for (q, p) in ESCRITURAS if "INSERT IGNORE INTO tk_ticket_documentos" in q]
check(res["modo"] == "aplicado", "el resultado se identifica como aplicado")
check(len(inserts) == 2, f"escribe exactamente 2 documentos — escribio {len(inserts)}")
check(all("INSERT IGNORE" in q for q, _ in inserts),
      "usa INSERT IGNORE (idempotencia apoyada en el UNIQUE de la tabla)")
insertados = sorted((p[1], p[2]) for _, p in inserts)
check(insertados == [("FCV", "11439"), ("GDV", "4455")],
      f"registra el documento del ticket 10 y el del equipo del ticket 14: {insertados}")
check(not any(p[1] == "BLV" for _, p in inserts),
      "NO vuelve a insertar el BLV que ya estaba (idempotente)")
check(not any(p[1] == "FCV" and p[2] == "500" for _, p in inserts),
      "NO migra el parcial del ticket 12 sin que se lo pidan")
logs = [(q, p) for (q, p) in ESCRITURAS if "INSERT INTO tk_mensajes" in q]
check(len(logs) == 2,
      f"deja auditoria en la Actividad de cada ticket tocado — dejo {len(logs)}")

# ── 2.3 Con incluir_parciales SI entra el ambar, y nada mas ───────
ESCRITURAS.clear()
res2 = migrar(aplicar=True, incluir_parciales=True, verificar_erp=False,
              indexar_productos=False, limit=100)
inserts2 = sorted((p[1], p[2]) for (q, p) in ESCRITURAS
                  if "INSERT IGNORE INTO tk_ticket_documentos" in q)
check(("FCV", "500") in inserts2,
      "con incluir_parciales=True el 'FCV 500' del ticket 12 si se registra")
check(("FCV", "501") not in inserts2 and ("BLV", "501") not in inserts2,
      "el 501 suelto NUNCA se inventa, ni siquiera incluyendo parciales")

# ── 2.4 El tope por tanda se respeta ──────────────────────────────
ESCRITURAS.clear()
res3 = migrar(aplicar=True, verificar_erp=False, indexar_productos=False, limit=1)
check(res3["tickets_procesados"] == 1 and res3["restantes"] == 1,
      "procesa por tandas y reporta cuantos tickets quedan")


# ── 2.5 Indexar productos: usa el MISMO motor que la ficha ───────
# Se enchufa un _cubicador_fetch de mentira (el mismo helper que usa la
# pantalla real via _tk_fetch_doc_lineas). Ninguna linea de este test
# habla con el ERP: solo se comprueba que la migracion pase por ahi.
print("\n3) Indexado de productos y verificacion contra el ERP (motor simulado)")

DOCS_ERP = {
    ("FCV", "11439"): (
        {"cliente_nombre": "CLIENTE DE PRUEBA", "cliente_rut": "76996964-0",
         "email": "", "telefono": "", "direccion": "", "comuna": "", "fecha": "2026-03-12"},
        [{"sku": "TRX-100", "descripcion_erp": "Trotadora TRX 100", "cantidad": 2,
          "saldo": 2, "es_zz": False},
         {"sku": "ZZINSTALACION", "descripcion_erp": "Servicio instalación",
          "cantidad": 1, "saldo": 1, "es_zz": True}],
    ),
    # GDV-4455 a proposito NO existe en el ERP de mentira.
}


def fake_cubicador_fetch(tido, nudo, fast=False):
    par = ((tido or "").upper(), (nudo or "").strip())
    if par in DOCS_ERP:
        return DOCS_ERP[par]
    return None, None


ctx["_cubicador_fetch"] = fake_cubicador_fetch

ESCRITURAS.clear()
res4 = migrar(aplicar=True, verificar_erp=True, indexar_productos=True, limit=100)
eq_ins = [(q, p) for (q, p) in ESCRITURAS if "INSERT IGNORE INTO tk_ticket_equipos" in q]
doc_ins = [(q, p) for (q, p) in ESCRITURAS if "INSERT IGNORE INTO tk_ticket_documentos" in q]
check(len(eq_ins) == 1,
      f"indexa 1 producto (la linea ZZ de servicio no es un equipo) — indexo {len(eq_ins)}")
check(eq_ins and eq_ins[0][1][1] == "TRX-100",
      "el producto indexado es el SKU real de la linea del documento")
check(eq_ins and eq_ins[0][1][5] == "FCV-11439",
      "el equipo queda con su documento_garantia 'TIDO-NUDO', igual que el botón de la ficha")
check(res4["productos_indexados"] == 1 and res4["tickets_indexados"] == 1,
      "el resumen reporta lo indexado")
check(all(p[1] != "GDV" for _, p in doc_ins),
      "un documento que el ERP no reconoce NO se registra (con verificar_erp=True)")
check(res4["documentos_no_en_erp"] == 1,
      "y se reporta cuantos no aparecieron en el ERP para poder revisarlos")
check(any(p[3] == "2026-03-12" for _, p in doc_ins),
      "el documento se guarda con la FECHA real que devolvio el ERP")

print("\n" + "=" * 60)
if fallos:
    print(f"RESULTADO: {len(fallos)} verificacion(es) FALLARON")
    for f_ in fallos:
        print(f"  - {f_}")
    sys.exit(1)
print("RESULTADO: todas las verificaciones pasaron")
sys.exit(0)
