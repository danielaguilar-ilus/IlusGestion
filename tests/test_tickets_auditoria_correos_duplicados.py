"""Auditoría de SOLO LECTURA de correos del cliente guardados dos veces (Daniel 04-nov-2026: «se siguen repitiendo
las imágenes»). La causa ya está corregida (test_tickets_correo_reclamo_atomico.py); esto lista lo que quedó duplicado
ANTES del arreglo para que una persona decida qué quitar. Cuida tres cosas: que agrupe bien (mismo ticket + mismo
Message-ID, con el texto idéntico o no), que NO escriba nada y que solo la vea un superadmin. La prueba con MySQL real
se salta si no hay un servidor de pruebas en el puerto 3399."""
import ast
import functools
import json
import os
import re
from datetime import datetime

import pytest

_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MODULO = os.path.join(_RAIZ, "tickets_module.py")


@functools.lru_cache(maxsize=None)
def _analizado():
    with open(_MODULO, encoding="utf-8") as fh:
        fuente = fh.read()
    return fuente, ast.parse(fuente)   # el módulo tiene 15 mil líneas: se analiza una sola vez


def _funcion(nombre):
    """Texto de una función anidada de tickets_module.py (están dentro de register_tickets_routes), sin sangría."""
    fuente, arbol = _analizado()
    for nodo in ast.walk(arbol):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return re.sub(r"^ {4}", "", ast.get_source_segment(fuente, nodo), flags=re.M)
    raise AssertionError("no existe " + nombre)


class _Peticion:
    def __init__(self, args=None):
        self.args = args or {}


def _jsonify(cuerpo):
    return cuerpo


def _ns(superadmin, mysql_fetchall, args=None, con_formato_real=False):
    ns = {"json": json, "request": _Peticion(args), "jsonify": _jsonify,
          "_tk_solo_superadmin": lambda: superadmin, "mysql_fetchall": mysql_fetchall}
    if con_formato_real:
        ns.update({"chile_fmt": None, "_CL_TZ": None, "datetime": datetime,
                   "date": __import__("datetime").date, "timezone": __import__("datetime").timezone})
        exec(compile(_funcion("_fmt_dt"), "<fmt_dt>", "exec"), ns)
        exec(compile(_funcion("_fmt_row"), "<fmt_row>", "exec"), ns)
    else:
        ns["_fmt_row"] = lambda r: dict(r)
    exec(compile(_funcion("_tk_dup_agrupar"), "<agrupar>", "exec"), ns)
    exec(compile(_funcion("_tk_dup_leer"), "<leer>", "exec"), ns)       # la lectura la comparten la auditoría y la limpieza
    exec(compile(_funcion("tk_api_auditoria_correos_duplicados"), "<auditoria>", "exec"), ns)
    return ns


def _fila(i, ticket, mid, huella="h1", tipo="client_message", metadata=None):
    meta = metadata if metadata is not None else json.dumps({"via": "email", "message_id": mid, "from": "c@x.cl"})
    return {"id": i, "ticket_id": ticket, "tipo": tipo, "metadata": meta, "usuario": "Cliente",
            "created_at": None, "message_date": None, "huella": huella, "largo": 100, "vista": "Hola"}


def _agrupar(filas):
    return _ns(True, lambda *a: [])["_tk_dup_agrupar"](filas)


def test_agrupa_las_copias_del_mismo_correo_en_el_mismo_ticket():
    g = _agrupar([_fila(4463, 2304, "<a@x>"), _fila(4464, 2304, "<a@x>"), _fila(5000, 2304, "<otro@x>")])
    assert len(g) == 1
    assert g[0]["ticket_id"] == 2304 and g[0]["message_id"] == "<a@x>"
    assert [c["id"] for c in g[0]["copias"]] == [4463, 4464] and g[0]["mismo_contenido"] is True


def test_tres_copias_se_conservan_en_orden_de_id():
    g = _agrupar([_fila(30, 7, "<a@x>"), _fila(10, 7, "<a@x>"), _fila(20, 7, "<a@x>")])
    assert [c["id"] for c in g[0]["copias"]] == [10, 20, 30]   # la primera (id menor) es la que se conservaría


def test_el_mismo_message_id_en_otro_ticket_no_cuenta_como_duplicado():
    assert _agrupar([_fila(1, 100, "<a@x>"), _fila(2, 200, "<a@x>")]) == []


def test_un_correo_unico_no_aparece():
    assert _agrupar([_fila(1, 100, "<a@x>"), _fila(2, 100, "<b@x>")]) == []


def test_contenido_distinto_se_marca_para_que_lo_mire_una_persona():
    g = _agrupar([_fila(1, 9, "<a@x>", huella="aaa"), _fila(2, 9, "<a@x>", huella="bbb")])
    assert g[0]["mismo_contenido"] is False


def test_metadata_vacia_rota_o_sin_message_id_se_ignora_sin_romper():
    sin_metadata = _fila(2, 5, "x")
    sin_metadata["metadata"] = None
    filas = [_fila(1, 5, "x", metadata=""), sin_metadata,
             _fila(3, 5, "x", metadata="{no es json"), _fila(4, 5, "x", metadata='{"via":"email"}'),
             _fila(5, 5, "x", metadata='["lista"]'), _fila(6, 5, "x", metadata='{"message_id": "  "}')]
    assert _agrupar(filas) == []


def test_los_grupos_salen_ordenados_por_ticket_y_primer_mensaje():
    g = _agrupar([_fila(9, 300, "<c@x>"), _fila(10, 300, "<c@x>"), _fila(1, 100, "<a@x>"), _fila(2, 100, "<a@x>")])
    assert [x["ticket_id"] for x in g] == [100, 300]


def test_un_no_superadmin_recibe_403_y_no_se_toca_la_base():
    consultas = []
    ns = _ns(False, lambda *a: consultas.append(a) or [])
    cuerpo, estado = ns["tk_api_auditoria_correos_duplicados"]()
    assert estado == 403 and cuerpo["ok"] is False and consultas == []


def test_la_ruta_pide_login_de_tickets_y_es_solo_get():
    fuente = _analizado()[0].replace("\r\n", "\n")
    assert ('@app.route("/tickets/api/auditoria/correos-duplicados", methods=["GET"])\n'
            "    @_tickets_required\n"
            "    def tk_api_auditoria_correos_duplicados():") in fuente


def test_no_escribe_nada_en_la_base():
    cuerpo = _funcion("tk_api_auditoria_correos_duplicados") + _funcion("_tk_dup_agrupar") + _funcion("_tk_dup_leer")
    assert "mysql_execute" not in cuerpo and "_tk_log(" not in cuerpo
    for palabra in ("INSERT", "UPDATE", "DELETE", "REPLACE", "ALTER", "DROP", "TRUNCATE"):
        assert palabra not in cuerpo, palabra


def test_arma_el_resultado_con_lo_que_se_quitaria_y_sus_adjuntos():
    llamadas = []

    def fetchall(sql, params=()):
        llamadas.append((" ".join(sql.split()), params))
        if "FROM tk_mensajes" in sql:
            return [_fila(4463, 2304, "<a@x>"), _fila(4464, 2304, "<a@x>")]
        if "FROM tk_adjuntos" in sql:
            return [{"id": 970, "mensaje_id": 4463, "archivo_nombre": "IMG_3683.jpeg", "mime_type": "image/jpeg", "file_size_kb": 142},
                    {"id": 973, "mensaje_id": 4464, "archivo_nombre": "IMG_3683.jpeg", "mime_type": "image/jpeg", "file_size_kb": 142}]
        if "FROM tk_tickets" in sql:
            return [{"id": 2304, "numero_ticket": "TAA-918", "titulo": "Fotos", "empresa": "Auryva", "nombre_contacto": "X", "estado": "open"}]
        return []

    r = _ns(True, fetchall, {"dias": "30"})["tk_api_auditoria_correos_duplicados"]()
    assert r["ok"] and r["solo_lectura"] and r["ventana_dias"] == 30 and r["revisados"] == 2
    assert r["resumen"] == {"tickets_afectados": 1, "grupos": 1, "mensajes_a_quitar": 1,
                            "adjuntos_a_quitar": 1, "grupos_con_contenido_distinto": 0}
    gr = r["grupos"][0]
    assert gr["numero_ticket"] == "TAA-918" and gr["empresa"] == "Auryva"
    assert gr["conservar"]["id"] == 4463 and [q["id"] for q in gr["quitar"]] == [4464]
    assert gr["conservar"]["adjuntos"][0]["id"] == 970 and gr["quitar"][0]["adjuntos"][0]["id"] == 973
    # solo se leyó: y los parámetros del filtro van como parámetros, no pegados al SQL
    assert all(s.lstrip().upper().startswith("SELECT") for s, _ in llamadas)
    assert llamadas[0][1] == ('%"message_id"%', 30)


def test_dias_invalidos_caen_al_valor_por_defecto():
    visto = []
    ns = _ns(True, lambda sql, params=(): visto.append(params) or [], {"dias": "no-es-numero"})
    assert ns["tk_api_auditoria_correos_duplicados"]()["ventana_dias"] == 120
    ns = _ns(True, lambda sql, params=(): [], {"dias": "999999"})
    assert ns["tk_api_auditoria_correos_duplicados"]()["ventana_dias"] == 3650


def _conexion_prueba():
    try:
        import pymysql
        return pymysql.connect(host="127.0.0.1", port=3399, user="root", password="", charset="utf8mb4",
                               autocommit=True, connect_timeout=2, cursorclass=pymysql.cursors.DictCursor)
    except Exception:
        return None


def test_con_mysql_real_el_sql_funciona_y_el_resultado_se_puede_serializar():
    con = _conexion_prueba()
    if con is None:
        pytest.skip("no hay MySQL de pruebas en el puerto 3399")
    try:
        with con.cursor() as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS repro_auditoria")
            cur.execute("USE repro_auditoria")
            for tabla in ("tk_adjuntos", "tk_mensajes", "tk_tickets"):
                cur.execute("DROP TABLE IF EXISTS " + tabla)
            cur.execute("CREATE TABLE tk_tickets (id INT PRIMARY KEY, numero_ticket VARCHAR(30), titulo VARCHAR(200), "
                        "empresa VARCHAR(200), nombre_contacto VARCHAR(200), estado VARCHAR(30)) DEFAULT CHARSET=utf8mb4")
            cur.execute("CREATE TABLE tk_mensajes (id INT AUTO_INCREMENT PRIMARY KEY, ticket_id INT NOT NULL, "
                        "tipo ENUM('comentario','mensaje','client_message','otro') NOT NULL DEFAULT 'comentario', "
                        "contenido MEDIUMTEXT NULL, metadata TEXT NULL, usuario VARCHAR(190) NULL, "
                        "message_date DATETIME NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP) DEFAULT CHARSET=utf8mb4")
            cur.execute("CREATE TABLE tk_adjuntos (id INT AUTO_INCREMENT PRIMARY KEY, ticket_id INT NOT NULL, mensaje_id INT NULL, "
                        "archivo_url VARCHAR(500), archivo_nombre VARCHAR(300), mime_type VARCHAR(150), file_size_kb INT NULL) "
                        "DEFAULT CHARSET=utf8mb4")
            cur.execute("INSERT INTO tk_tickets VALUES (2304,'TAA-918','Fotos','Auryva SpA','Ana','open'),"
                        "(2305,'TAA-919','Otro','Beta Ltda','Beto','open')")
            meta = lambda mid: json.dumps({"via": "email", "message_id": mid, "from": "c@x.cl"}, ensure_ascii=False)
            filas = [(2304, "client_message", "Hola con fotos", meta("<a@x>")),
                     (2304, "client_message", "Hola con fotos", meta("<a@x>")),
                     (2304, "client_message", "Otro correo", meta("<b@x>")),
                     (2305, "client_message", "Hola con fotos", meta("<a@x>")),      # mismo id, OTRO ticket: no es duplicado
                     (2305, "client_message", "texto A", meta("<c@x>")),
                     (2305, "mensaje", "texto B", meta("<c@x>")),                     # mismo id, contenido distinto
                     (2305, "comentario", "nota interna", None),                      # sin metadata
                     (2305, "client_message", "roto", "{no es json")]                 # metadata inválida
            for tk, tipo, texto, m in filas:
                cur.execute("INSERT INTO tk_mensajes (ticket_id, tipo, contenido, metadata, usuario) VALUES (%s,%s,%s,%s,'Cliente')",
                            (tk, tipo, texto, m))
            cur.execute("INSERT INTO tk_adjuntos (ticket_id, mensaje_id, archivo_url, archivo_nombre, mime_type, file_size_kb) VALUES "
                        "(2304,1,'u1','IMG_3681.jpeg','image/jpeg',113),(2304,2,'u2','IMG_3681.jpeg','image/jpeg',113),"
                        "(2304,2,'u3','IMG_3682.jpeg','image/jpeg',128)")

        def fetchall(sql, params=()):
            with con.cursor() as cur:
                cur.execute(sql, params)
                return list(cur.fetchall())

        r = _ns(True, fetchall, con_formato_real=True)["tk_api_auditoria_correos_duplicados"]()
        json.dumps(r)   # lo que devuelve la ruta debe poder viajar como JSON (fechas ya formateadas)
        assert r["resumen"] == {"tickets_afectados": 2, "grupos": 2, "mensajes_a_quitar": 2,
                                "adjuntos_a_quitar": 2, "grupos_con_contenido_distinto": 1}
        g1 = next(x for x in r["grupos"] if x["ticket_id"] == 2304)
        assert g1["numero_ticket"] == "TAA-918" and g1["empresa"] == "Auryva SpA" and g1["mismo_contenido"] is True
        assert g1["conservar"]["id"] == 1 and [q["id"] for q in g1["quitar"]] == [2]
        assert len(g1["conservar"]["adjuntos"]) == 1 and len(g1["quitar"][0]["adjuntos"]) == 2
        assert re.match(r"\d\d/\d\d/\d{4} \d\d:\d\d", g1["conservar"]["guardado"])
        g2 = next(x for x in r["grupos"] if x["ticket_id"] == 2305)
        assert g2["mismo_contenido"] is False
        with con.cursor() as cur:   # y la prueba misma comprueba que no cambió nada: sigue habiendo 8 mensajes
            cur.execute("SELECT COUNT(*) AS n FROM tk_mensajes")
            assert cur.fetchone()["n"] == 8
    finally:
        con.close()
