"""Un correo del cliente se guarda UNA sola vez, aunque dos barridos lo lean a la vez (Daniel 04-nov-2026:
«se siguen repitiendo las imágenes»).

Qué pasaba: el lector de correo preguntaba «¿este Message-ID ya está?» con un SELECT y marcaba el correo recién DESPUÉS
de guardar el mensaje. Cada petición usa una conexión con REPEATABLE READ y autocommit apagado, así que un barrido que
había empezado antes de que otro marcara el correo seguía leyendo su foto vieja de la base, no lo veía y lo ingresaba
otra vez con sus fotos. Medido en producción (04-nov): 17 de 26 tickets con movimiento reciente traían el mismo correo
(mismo Message-ID y mismo texto) dos veces; el caso del ticket 2304 eran 3 fotos IMG_3681-3683 a las 20:48 y 20:49.

Ahora el correo se RECLAMA con `INSERT IGNORE` sobre la PRIMARY KEY (que mira lo último comprometido, no la foto) y se
mira cuántas filas entraron, justo antes de guardar el mensaje. Estas pruebas cuidan esa forma; la prueba con MySQL real
se salta si no hay un servidor de pruebas en el puerto 3399."""
import ast
import functools
import os
import re

import pytest

_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MODULO = os.path.join(_RAIZ, "tickets_module.py")


@functools.lru_cache(maxsize=None)
def _analizado(ruta):
    with open(ruta, encoding="utf-8") as fh:
        fuente = fh.read()
    return fuente, ast.parse(fuente)   # el módulo tiene 15 mil líneas: se analiza una sola vez


def _fuente():
    return _analizado(_MODULO)[0]


def _funcion(nombre):
    """Texto de una función anidada de tickets_module.py (están dentro de register_tickets_routes)."""
    fuente, arbol = _analizado(_MODULO)
    for nodo in ast.walk(arbol):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == nombre:
            return ast.get_source_segment(fuente, nodo)
    raise AssertionError("no existe " + nombre)


def _helper(ctx, mysql_execute):
    """Compila _tk_mail_reclamar tal como está en el módulo, con un ctx y un mysql_execute de mentira."""
    ns = {"ctx": ctx, "mysql_execute": mysql_execute}
    exec(compile(re.sub(r"^ {4}", "", _funcion("_tk_mail_reclamar"), flags=re.M), "<reclamar>", "exec"), ns)
    return ns["_tk_mail_reclamar"]


def test_solo_hay_un_lugar_que_escribe_la_marca():
    assert _fuente().count("INSERT IGNORE INTO tk_mail_ingeridos") == 1, \
        "la marca de «correo ya ingresado» debe escribirse solo desde _tk_mail_reclamar"
    assert "INSERT IGNORE INTO tk_mail_ingeridos" in _funcion("_tk_mail_reclamar")


def test_el_inbox_reclama_antes_de_guardar_el_mensaje():
    cuerpo = _funcion("_tk_leer_correo_entrante")
    reclamo = cuerpo.index("_tk_mail_reclamar(message_id")
    guardado = cuerpo.index('"client_message"')
    assert reclamo < guardado, "el correo debe reclamarse ANTES de insertar el mensaje del cliente"
    # y antes de subir los adjuntos: si el proceso muere subiendo, se pierde un adjunto, nunca se duplica la conversación
    assert guardado < cuerpo.index("INSERT INTO tk_adjuntos")
    # si otro barrido ya lo reclamó, se salta sin guardar nada
    tramo = cuerpo[reclamo:guardado]
    assert "continue" in tramo and 'resumen["duplicados"] += 1' in tramo


def test_enviados_reclama_antes_de_guardar_el_mensaje():
    cuerpo = _funcion("_tk_leer_correo_enviado")
    reclamo = cuerpo.index("_tk_mail_reclamar(message_id")
    guardado = cuerpo.index('"email_enviados"')
    assert reclamo < guardado
    assert "continue" in cuerpo[reclamo:guardado]


def test_reclamar_devuelve_true_solo_al_primero():
    llamadas = []

    def con_filas(n):
        def _rc(sql, params):
            llamadas.append((sql, params))
            return n
        return _rc

    primero = _helper({"mysql_execute_returning_rowcount": con_filas(1)}, lambda *a: None)
    assert primero("<a@x>", 7, "cli@x.cl", "Re: Ticket") is True
    segundo = _helper({"mysql_execute_returning_rowcount": con_filas(0)}, lambda *a: None)
    assert segundo("<a@x>", 7, "cli@x.cl", "Re: Ticket") is False
    assert all("INSERT IGNORE INTO tk_mail_ingeridos" in sql for sql, _ in llamadas)


def test_reclamar_recorta_a_las_columnas_y_tolera_vacios():
    vistos = []
    reclamar = _helper({"mysql_execute_returning_rowcount": lambda sql, p: vistos.append(p) or 1}, lambda *a: None)
    reclamar("<a@x>", 1, "x" * 400, "y" * 500)
    reclamar("<b@x>", 1, None, None)
    assert len(vistos[0][2]) == 190 and len(vistos[0][3]) == 300
    assert vistos[1][2] == "" and vistos[1][3] == ""


def test_si_la_base_falla_el_error_sube_para_reintentar():
    def _rc(sql, params):
        raise RuntimeError("base caída")
    reclamar = _helper({"mysql_execute_returning_rowcount": _rc}, lambda *a: None)
    with pytest.raises(RuntimeError):
        reclamar("<a@x>", 1, "c@x.cl", "s")


def test_sin_contador_de_filas_cae_al_comportamiento_anterior():
    escrito = []
    reclamar = _helper({}, lambda sql, params: escrito.append(params))
    assert reclamar("<a@x>", 1, "c@x.cl", "s") is True
    assert len(escrito) == 1


def _conexion_prueba():
    try:
        import pymysql
        return pymysql.connect(host="127.0.0.1", port=3399, user="root", password="", charset="utf8mb4",
                               autocommit=False, connect_timeout=2)
    except Exception:
        return None


def test_con_mysql_real_un_barrido_con_foto_vieja_no_duplica():
    """El caso real: B abre su foto de la base, A reclama y comete, B intenta reclamar (debe salir 0)."""
    a = _conexion_prueba()
    if a is None:
        pytest.skip("no hay MySQL de pruebas en el puerto 3399")
    b = _conexion_prueba()
    try:
        with a.cursor() as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS repro_reclamo")
            cur.execute("USE repro_reclamo")
            cur.execute("DROP TABLE IF EXISTS tk_mail_ingeridos")
            cur.execute("CREATE TABLE tk_mail_ingeridos (message_id VARCHAR(255) PRIMARY KEY, ticket_id INT NULL, "
                        "from_email VARCHAR(190) NULL, subject VARCHAR(300) NULL, "
                        "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, KEY idx_ticket (ticket_id)) "
                        "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
        a.commit()
        with b.cursor() as cur:
            cur.execute("USE repro_reclamo")
            cur.execute("SELECT COUNT(*) FROM tk_mail_ingeridos")   # B abre su foto vieja
            cur.fetchone()

        def reclamar(conn):
            def _rc(sql, params):
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    n = cur.rowcount
                conn.commit()
                return n
            return _helper({"mysql_execute_returning_rowcount": _rc}, lambda *x: None)("<m@x>", 2304, "c@x.cl", "s")

        assert reclamar(a) is True
        with b.cursor() as cur:   # el SELECT de antes, con la foto vieja, NO ve el correo: ahí nacía el duplicado
            cur.execute("SELECT message_id FROM tk_mail_ingeridos WHERE message_id=%s", ("<m@x>",))
            assert cur.fetchone() is None
        assert reclamar(b) is False, "el reclamo atómico sí ve que ya estaba"
    finally:
        a.close()
        b.close()
