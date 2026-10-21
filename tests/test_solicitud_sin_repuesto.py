"""Solicitud de repuesto sin crear el repuesto (2026-10-14, Daniel: "quiero poder levantar
una solicitud de repuesto, no necesariamente crear el repuesto, indicando los posibles
nombres o descripción… para que pueda ser tomada por un ticket, o gestionando una OT, o
siendo consumido por un producto de la bodega de repuesto").

Prueba, extrayendo funciones de app.py con ast (importar app.py levanta Flask y la base):
  - los otros nombres se limpian (sin repetidos, máx. 8);
  - validada con camino «OT» y sin repuesto de bodega queda «Lista para OT»; con camino
    «ticket» y sin repuesto queda «Lista para lote»;
  - validar sin repuesto exige elegir el camino ticket u OT (nunca en silencio);
  - asociar una OT y cerrar como «se reparó» son de gestión, nunca de un técnico;
  - una solicitud cerrada desde el ticket de un cliente no cierra ese ticket.

Correr con:  py -m unittest tests.test_solicitud_sin_repuesto
"""
import ast
import json
import os
import re
import unittest
from datetime import datetime, timedelta

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PY = os.path.join(RAIZ, "app.py")
_FUN = ("_otrep_norm_desc", "_otrep_nombres_alt", "_otrep_feriados", "_otrep_a_chile", "_otrep_dia_habil",
        "_otrep_horas_habiles", "_otrep_sumar_horas_habiles", "_otrep_fmt_horas", "_otrep_etapa_y_plazo")
_CONST = ("_OTREP_ETAPA_LABEL", "_OTREP_PLAZO_H", "_OTREP_JORNADA", "_OTREP_COMPRA_DIAS_DEF", "_OTREP_FERIADOS",
          "_OTREP_RESOLUCIONES", "_OTREP_CAMINOS")
_CACHE = {}


def _arbol():
    if "a" not in _CACHE:
        with open(APP_PY, encoding="utf-8") as fh:
            _CACHE["src"] = fh.read()
        _CACHE["a"] = ast.parse(_CACHE["src"])
    return _CACHE["a"]


def _amb():
    if "amb" not in _CACHE:
        nodos = [n for n in _arbol().body
                 if (isinstance(n, ast.FunctionDef) and n.name in _FUN)
                 or (isinstance(n, ast.Assign) and any(getattr(t, "id", "") in _CONST for t in n.targets))]
        amb = {"datetime": datetime, "timedelta": timedelta, "json": json, "re": re}
        exec(compile(ast.Module(body=nodos, type_ignores=[]), "<app>", "exec"), amb)
        _CACHE["amb"] = amb
    return _CACHE["amb"]


def _fuente(nombre):
    _arbol()
    fn = next(n for n in _CACHE["a"].body if isinstance(n, ast.FunctionDef) and n.name == nombre)
    return ast.get_source_segment(_CACHE["src"], fn), fn


class OtrosNombres(unittest.TestCase):
    def test_limpia_y_no_repite(self):
        f = _amb()["_otrep_nombres_alt"]
        self.assertEqual(f("Correa de transmisión; correa de transmision ; banda motriz"),
                         ["Correa de transmisión", "banda motriz"])
        self.assertEqual(f(["a", "", "  pad  "]), ["pad"])  # menos de 2 letras no cuenta
        self.assertEqual(len(f(";".join("nombre %d" % i for i in range(20)))), 8)
        self.assertEqual(f(None), [])
        self.assertEqual(f('["Belt 2540", "Correa X3"]'), ["Belt 2540", "Correa X3"])


class EtapasPorCamino(unittest.TestCase):
    def _s(self, **kw):
        s = {"abierta": True, "estado": "validado", "validado_at": datetime.utcnow() - timedelta(minutes=5),
             "created_at": datetime.utcnow() - timedelta(hours=1)}
        s.update(kw)
        return s

    def test_camino_ot_sin_repuesto_va_a_lista_para_ot(self):
        out = _amb()["_otrep_etapa_y_plazo"](self._s(camino="ot", repuesto_stock_id=None, stock_disponible=None))
        self.assertEqual(out["etapa"], "ot")
        self.assertIn("OT", out["siguiente"])

    def test_camino_ticket_sin_repuesto_va_al_lote(self):
        out = _amb()["_otrep_etapa_y_plazo"](self._s(camino="ticket", repuesto_stock_id=None, stock_disponible=None))
        self.assertEqual(out["etapa"], "lote")

    def test_cerrada_como_reparada_lo_dice(self):
        out = _amb()["_otrep_etapa_y_plazo"](self._s(estado="rechazado", abierta=False, resolucion="reparado"))
        self.assertEqual(out["etapa"], "cerrada")
        self.assertEqual(out["siguiente"], "Se reparó sin repuesto")


class ReglasDelServidor(unittest.TestCase):
    def test_validar_sin_repuesto_exige_camino(self):
        src, _ = _fuente("_otrep_cambiar_estado")
        self.assertIn('if _camino not in ("ticket", "ot"):', src)
        self.assertIn("Elige cómo se gestiona", src)

    def test_asociar_ot_y_resolver_son_de_gestion(self):
        # 2026-10-15 (revisión Fase B): asociar/quitar una OT exige el permiso de OT (el mismo
        # que vincular-ot), no solo gestión de repuestos.
        for nombre, dec in (("repstock_solicitud_asociar_ot", "_otrep_generar_ot_required"),
                            ("repstock_solicitud_desasociar_ot", "_otrep_generar_ot_required"),
                            ("repstock_solicitud_resolver", "_otrep_gestion_required"),
                            ("repstock_solicitud_asociar_repuesto", "_otrep_gestion_required")):
            src, fn = _fuente(nombre)
            decs = [getattr(d, "id", "") for d in fn.decorator_list]
            self.assertIn(dec, decs, nombre)
            self.assertIn("_es_rol_tecnico()", src, nombre)
        src, _ = _fuente("repstock_solicitud_asociar_repuesto")
        self.assertIn("_oculta_proveedores()", src)

    def test_asociar_ot_valida_antes_de_escribir(self):
        src, _ = _fuente("repstock_solicitud_asociar_ot")
        self.assertLess(src.index('_otrep_cambiar_estado(sid, "validado"'), src.index("SET ot_generada_id=%s"))
        self.assertIn('d.get("solo_ver")', src)
        self.assertNotIn("OR id=", src)

    def test_resolver_suelta_la_compra(self):
        src, _ = _fuente("repstock_solicitud_resolver")
        self.assertIn("_otrep_compra_tras_estado(", src)
        src2, _ = _fuente("repstock_solicitud_ot_estado")
        self.assertIn("_otrep_compra_tras_estado(", src2)

    def test_reabrir_limpia_la_resolucion(self):
        src, _ = _fuente("_otrep_cambiar_estado")
        i = src.index('if nuevo == "solicitado":')
        self.assertIn('"resolucion=NULL", "resolucion_nota=NULL", "camino=NULL"', src[i:i + 900])
        self.assertNotIn("COALESCE(resolucion", src)

    def test_camino_ot_sin_repuesto_no_se_compra(self):
        _, fn = _fuente("_otrep_compra_elegible")
        amb = {}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), "<app>", "exec"), amb)
        f = amb["_otrep_compra_elegible"]
        self.assertFalse(f({"estado": "validado", "camino": "ot", "repuesto_stock_id": None, "stock_disponible": None}))
        self.assertTrue(f({"estado": "validado", "camino": "ticket", "repuesto_stock_id": None, "stock_disponible": None}))


class OtPorNumero(unittest.TestCase):
    """«97» nunca es el id interno 97 (era OT-2026-00002): se busca por número de OT."""

    def _f(self, filas):
        _, fn = _fuente("_otrep_ot_por_numero")
        llamadas = []

        def fetchall(sql, params):
            llamadas.append((sql, params))
            return filas

        amb = {"re": re, "mysql_fetchall": fetchall,
               "_now_chile": lambda: datetime(2026, 10, 15)}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), "<app>", "exec"), amb)
        return amb["_otrep_ot_por_numero"], llamadas

    def test_numero_completo(self):
        f, ll = self._f([{"id": 272, "numero_ot": "OT-2026-00097"}])
        v, err = f("ot-2026-97")
        self.assertEqual(v["id"], 272)
        self.assertEqual(ll[0][1], ("OT-2026-00097",))
        self.assertNotIn(" id=", ll[0][0])

    def test_numero_corto_busca_por_sufijo(self):
        f, ll = self._f([{"id": 272, "numero_ot": "OT-2026-00097"}])
        v, err = f("97")
        self.assertEqual(v["id"], 272)
        self.assertEqual(ll[0][1], ("OT-%-00097",))

    def test_numero_corto_ambiguo_prefiere_el_anio(self):
        f, _ = self._f([{"id": 900, "numero_ot": "OT-2027-00097"}, {"id": 272, "numero_ot": "OT-2026-00097"}])
        v, err = f("97")
        self.assertEqual(v["id"], 272)

    def test_texto_raro_no_busca(self):
        f, ll = self._f([])
        v, err = f("abc; drop")
        self.assertIsNone(v)
        self.assertFalse(ll)


class Privacidad(unittest.TestCase):
    def test_nota_de_resolucion_y_camino_son_de_gestion(self):
        _arbol()
        for nombre in ("_OTREP_SOL_SOLO_GESTION_PROV", "_OTREP_SOL_NO_EXTERNO"):
            nodo = next(n for n in _CACHE["a"].body if isinstance(n, ast.Assign)
                        and any(getattr(t, "id", "") == nombre for t in n.targets))
            valores = ast.literal_eval(nodo.value)
            self.assertIn("resolucion_nota", valores, nombre)
            self.assertIn("camino", valores, nombre)

    def test_historial_del_repuesto_cerrado_a_tecnicos(self):
        _, fn = _fuente("repstock_stock_solicitudes")
        self.assertIn("_no_tecnico", [getattr(d, "id", "") for d in fn.decorator_list])

    def test_ticket_del_cliente_no_se_cierra_solo(self):
        src, _ = _fuente("_otrep_resolver_ticket_si_corresponde")
        self.assertIn("tipo LIKE 'spare_parts%%'", src)

    def test_aviso_de_plazo_cabe_en_el_enum_de_la_campana(self):
        # 2026-10-15: 'repuesto_plazo' e 'ia_alerta_proactiva' se usaban fuera del ENUM y el
        # INSERT fallaba en silencio (modo estricto): la campana nunca avisaba.
        _arbol()
        enums = re.findall(r"ALTER TABLE mant_notificaciones MODIFY COLUMN tipo \"\s*\"\s*ENUM\((.*?)\) DEFAULT 'otro'",
                           _CACHE["src"], re.S)
        self.assertEqual(len(enums), 2)
        usados = set(re.findall(r'_mant_notificar\(\s*[^,()]+,\s*"([a-z_]+)"', _CACHE["src"])) \
            | set(re.findall(r'_mant_notificar\([^)]*?tipo="([a-z_]+)"', _CACHE["src"], re.S))
        self.assertIn("repuesto_plazo", usados)
        for e in enums:
            valores = set(re.findall(r"'([a-z_]+)'", e))
            self.assertLessEqual(usados, valores, usados - valores)


if __name__ == "__main__":
    unittest.main()


class ReferenciasYAviso(unittest.TestCase):
    """2026-10-21 (Daniel: «que lleguen y alerten a los administradores… basadas en el producto, referenciadas a la
    OT y a la factura… con número de serie»)."""

    def test_el_evento_creada_referencia_y_avisa(self):
        src, _ = _fuente("_otrep_evento")
        self.assertIn('accion == "creada" and a_estado == "solicitado"', src)
        self.assertIn("_otrep_referenciar(sid)", src)
        self.assertIn("_otrep_avisar_nueva(sid, usuario)", src)

    def test_el_aviso_va_solo_a_gestion(self):
        src, _ = _fuente("_otrep_avisar_nueva")
        self.assertIn('("superadmin", "admin")', src)
        self.assertIn('!= "tecnico"', src)
        self.assertIn('"repuesto_nuevo"', src)
        self.assertNotIn("_send_ilus_email", src)   # sin correo: solo campana

    def test_referencia_nunca_pisa(self):
        src, _ = _fuente("_otrep_referenciar")
        self.assertIn("serie=COALESCE(serie,%s)", src)
        self.assertIn("documento=COALESCE(documento,%s)", src)

    def test_documento_oculto_a_tecnicos(self):
        _arbol()
        for nombre in ("_OTREP_SOL_SOLO_GESTION_PROV", "_OTREP_SOL_NO_EXTERNO"):
            nodo = next(n for n in _CACHE["a"].body if isinstance(n, ast.Assign)
                        and any(getattr(t, "id", "") == nombre for t in n.targets))
            self.assertIn("documento", ast.literal_eval(nodo.value), nombre)

    def test_docs_titulos_lee_la_lista_del_dict(self):
        src, _ = _fuente("_otrep_docs_titulos")
        self.assertIn('.get("documentos")', src)
