"""Crear el repuesto que no existe, desde la solicitud (2026-10-04, Daniel: "si
lo vamos a crear, creémoslo bien con todas las restricciones que significa
crear" + foto y equipo compatible OBLIGATORIOS).

Prueba repstock_solicitud_crear_repuesto extrayéndola de app.py con ast (importar
app.py levanta Flask, la base y los crons), con dobles de BD, request y GCS:
  - solo gestión (un técnico no crea repuestos con proveedor, REGLA #19);
  - solo sobre una solicitud todavía "solicitada";
  - descripción, stock mínimo, proveedor, equipo compatible (o "aún no sé") y
    foto son obligatorios;
  - no crea duplicados (misma descripción sin importar tildes/mayúsculas);
  - nace "por llegar" (cantidad 0, sin ubicación), con su modelo, su foto
    COPIADA y la solicitud validada contra él.

Correr con:  py -m unittest tests.test_repuestos_crear_desde_solicitud
"""
import ast
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")
_FUNCIONES = ("repstock_solicitud_crear_repuesto", "_otrep_norm_desc")
_NODOS = {}


def _nodos():
    if not _NODOS:
        with open(APP_PY, encoding="utf-8") as fh:
            arbol = ast.parse(fh.read())
        for n in arbol.body:
            if isinstance(n, ast.FunctionDef) and n.name in _FUNCIONES:
                n.decorator_list = []
                _NODOS[n.name] = n
    return _NODOS


class _Form(dict):
    def get(self, k, default=None):
        return super().get(k, default)


class _Archivo:
    def __init__(self, nombre):
        self.filename = nombre


class _Cur:
    def __init__(self, log):
        self.log = log
        self.lastrowid = 900

    def execute(self, sql, params=None):
        self.log.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Conn:
    def __init__(self, log):
        self.log = log
        self.commits = 0

    def cursor(self):
        return _Cur(self.log)

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        pass


class _Blob:
    def __init__(self, nombre):
        self.name = nombre


class _Bucket:
    def __init__(self, copias):
        self.copias = copias

    def blob(self, nombre):
        return _Blob(nombre)

    def copy_blob(self, blob, bucket, nuevo):
        self.copias.append((blob.name, nuevo))


def _jsonify(d):
    return d


def _armar(form, files=None, solicitud=None, existentes=None, tecnico=False):
    estado = {"sql": [], "copias": [], "cambios": [], "subidas": []}
    sol = dict({"id": 7, "estado": "solicitado", "maquina_id": 3, "cliente_id": 11, "n_fotos": 1,
                "incidencia_id": None}, **(solicitud or {}))

    def fetchone(sql, params=None):
        if "FROM mant_ot_repuesto_solicitudes" in sql:
            return sol
        if "FROM mant_proveedores_repuesto" in sql:
            return {"id": params[0]} if params and params[0] == 5 else None
        if "FROM mant_repuestos_marcas" in sql:
            return {"id": params[0], "nombre": "Drax"}
        if "FROM cat_productos" in sql:
            return {"id": params[0]}
        if "FROM mant_maquinas" in sql:
            return {"id": 3, "nombre": "Trotadora X3", "sku": "X3"}
        if "FROM mant_ot_repuesto_evidencias" in sql:
            return {"public_id": "otrep/foto_1.jpg"}
        return None

    def fetchall(sql, params=None):
        return existentes or []

    class _Req:
        pass
    req = _Req()
    req.form = _Form(form)
    req.files = _Form(files or {})

    def cambiar(sid, nuevo, user, datos):
        estado["cambios"].append((sid, nuevo, datos))
        return True, 200, {"ok": True}

    def subir(f, folder=None, public_id=None, resource_type=None):
        estado["subidas"].append(public_id)
        return {"public_id": folder + "/" + public_id + ".jpg"}

    amb = {
        "re": re, "os": os, "time": __import__("time"), "jsonify": _jsonify, "request": req,
        "mysql_fetchone": fetchone, "mysql_fetchall": fetchall,
        "get_mysql": lambda: _Conn(estado["sql"]),
        "_es_rol_tecnico": lambda: tecnico, "_oculta_proveedores": lambda: tecnico,
        "_validate_uploaded_image": lambda f, label="": ("jpg", None),
        "_uploader_upload": subir, "_uploader_destroy": lambda k: None,
        "_gcs_bucket": lambda: _Bucket(estado["copias"]),
        "_otrep_modelos_de_maquina": lambda m: [{"id": 44}],
        "_otrep_producto_de_maquina": lambda m: None,
        "_repstock_next_sku": lambda marca, conn: "REP-DRAX-0009",
        "_repstock_log_movimiento": lambda cur, *a, **k: cur.execute("MOV", a),
        "_mant_log": lambda *a, **k: None, "_otrep_evento": lambda *a, **k: None,
        "_otrep_cambiar_estado": cambiar, "current_username": lambda: "Juan Pablo",
        "print": lambda *a, **k: None,
    }
    for n in ("_otrep_norm_desc", "repstock_solicitud_crear_repuesto"):
        exec(compile(ast.Module(body=[_nodos()[n]], type_ignores=[]), "<app>", "exec"), amb)
    return amb["repstock_solicitud_crear_repuesto"], estado


_OK = {"descripcion": "Correa de motor X3", "stock_minimo": "1", "proveedor_id": "5",
       "modelo_modo": "equipo", "foto_evidencia_id": "31"}


def _llamar(form, **kw):
    fn, est = _armar(form, **kw)
    r = fn(7)
    if isinstance(r, tuple):
        return r[0], r[1], est
    return r, 200, est


class CrearRepuestoDesdeSolicitud(unittest.TestCase):
    def test_tecnico_no_crea(self):
        d, http, _ = _llamar(_OK, tecnico=True)
        self.assertEqual(http, 403)

    def test_solicitud_ya_gestionada(self):
        d, http, _ = _llamar(_OK, solicitud={"estado": "validado"})
        self.assertEqual(http, 409)

    def test_faltan_obligatorios(self):
        for campo, valor in (("descripcion", ""), ("stock_minimo", ""), ("proveedor_id", ""), ("proveedor_id", "99")):
            f = dict(_OK, **{campo: valor})
            d, http, _ = _llamar(f)
            self.assertEqual(http, 400, campo)

    def test_equipo_compatible_obligatorio(self):
        f = dict(_OK); f.pop("modelo_modo")
        d, http, _ = _llamar(f)
        self.assertEqual(http, 400)
        self.assertIn("equipo", d["error"])

    def test_aun_no_se_queda_pendiente(self):
        d, http, est = _llamar(dict(_OK, modelo_modo="pendiente"))
        self.assertTrue(d["ok"])
        insert = next(p for s, p in est["sql"] if "INSERT INTO mant_repuestos_stock " in s)
        self.assertEqual(insert[-2], 1)  # modelo_pendiente
        self.assertFalse(any("mant_repuestos_stock_modelos" in s for s, _ in est["sql"]))

    def test_foto_obligatoria(self):
        f = dict(_OK); f.pop("foto_evidencia_id")
        d, http, _ = _llamar(f)
        self.assertEqual(http, 400)
        self.assertIn("foto", d["error"])

    def test_no_duplica_por_descripcion(self):
        existentes = [{"id": 70, "sku": "REP-DRAX-0001", "descripcion": "CORREA de Motor  X3", "codigo_fabricante": None}]
        d, http, est = _llamar(dict(_OK, descripcion="Corréa de motor x3"), existentes=existentes)
        self.assertEqual(http, 409)
        self.assertEqual(d["duplicado"]["id"], 70)
        self.assertEqual(est["sql"], [])

    def test_crea_por_llegar_con_modelo_foto_copiada_y_valida(self):
        d, http, est = _llamar(_OK)
        self.assertTrue(d["ok"])
        self.assertEqual(d["sku"], "REP-DRAX-0009")
        sqls = [s for s, _ in est["sql"]]
        ins = next(s for s in sqls if "INSERT INTO mant_repuestos_stock " in s)
        self.assertIn("VALUES (%s,%s,0,NULL", ins)  # cantidad 0 y sin ubicación
        self.assertTrue(any("mant_repuestos_stock_modelos" in s for s in sqls))
        self.assertTrue(any("mant_repuestos_stock_fotos" in s for s in sqls))
        self.assertEqual(est["copias"][0][0], "otrep/foto_1.jpg")  # se COPIA, no se comparte
        self.assertEqual(est["cambios"][0][1], "validado")
        self.assertEqual(est["cambios"][0][2]["repuesto_stock_id"], 900)

    def test_foto_nueva_deja_copia_en_la_solicitud_sin_fotos(self):
        f = dict(_OK); f.pop("foto_evidencia_id")
        d, http, est = _llamar(f, files={"foto": _Archivo("correa.jpg")}, solicitud={"n_fotos": 0})
        self.assertTrue(d["ok"])
        self.assertTrue(est["subidas"])
        self.assertTrue(any("mant_ot_repuesto_evidencias" in s for s, _ in est["sql"]))
        self.assertTrue(any("n_fotos=n_fotos+1" in s for s, _ in est["sql"]))


if __name__ == "__main__":
    unittest.main()
