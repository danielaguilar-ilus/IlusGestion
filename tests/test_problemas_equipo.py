"""«Problemas y reparaciones» de cada equipo (2026-10-21, Daniel: «Si hay una máquina con un problema, yo tengo que saber cuándo se
le levantó el problema, cuándo se le reparó todo. Se tiene que ver por la ficha del cliente»).

Prueba, extrayendo _maq_problemas de app.py con ast (importar app.py levanta Flask y la base) y una base de mentira:
  - una parada abierta dice desde cuándo y hace cuánto; una cerrada, cuándo volvió a operar y en cuánto tiempo;
  - una solicitud de repuesto abierta dice qué se espera; una instalada, cuándo;
  - los hallazgos van solo para la gestión, sin repetir los de fuera de servicio ni de repuesto (ya tienen su fecha exacta);
  - abiertos primero; cada problema con su OT (número y enlace);
  - la ficha del cliente y la hoja de vida dibujan la tarjeta con la misma macro, sin plata ni proveedores.

Correr con:  py -m unittest tests.test_problemas_equipo
"""
import ast
import os
import re
import unittest
from datetime import datetime, timedelta

import jinja2

import ot_hallazgos

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CACHE = {}


def _leer(rel):
    with open(os.path.join(RAIZ, rel), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


def _fuente(nombre):
    if "src" not in _CACHE:
        _CACHE["src"] = _leer("app.py")
        _CACHE["a"] = ast.parse(_CACHE["src"])
    for n in _CACHE["a"].body:
        if isinstance(n, ast.FunctionDef) and n.name == nombre:
            return ast.get_source_segment(_CACHE["src"], n)
    raise AssertionError(nombre)


AHORA = datetime.utcnow().replace(microsecond=0)


class _Base:
    def __init__(self):
        self.consultas = []
        self.paradas = [
            {"maquina_id": 9, "inicio_at": AHORA - timedelta(days=3), "fin_at": None, "inicio_aprox": 0,
             "motivo": "Motor no enciende", "abierta_por": "Lenin Urbina", "cerrada_por": None,
             "ref_tabla": "mant_visitas", "ref_id": 513, "ref_fin_tabla": None, "ref_fin_id": None},
            {"maquina_id": 9, "inicio_at": AHORA - timedelta(days=40), "fin_at": AHORA - timedelta(days=38), "inicio_aprox": 0,
             "motivo": "Piola cortada", "abierta_por": "Jaizer", "cerrada_por": "Jaizer",
             "ref_tabla": "mant_visitas", "ref_id": 400, "ref_fin_tabla": "mant_visitas", "ref_fin_id": 410},
        ]
        self.solicitudes = [
            {"maquina_id": 9, "visita_id": 513, "repuesto_nombre": "Pasillo", "cantidad": 2, "origen": "repuesto", "estado": "pedido",
             "created_at": AHORA - timedelta(days=3), "instalado_at": None, "updated_at": AHORA, "solicitado_por": "Lenin Urbina"},
            {"maquina_id": 10, "visita_id": 400, "repuesto_nombre": "Piola", "cantidad": 1, "origen": "piola", "estado": "instalado",
             "created_at": AHORA - timedelta(days=40), "instalado_at": AHORA - timedelta(days=38), "updated_at": AHORA - timedelta(days=38),
             "solicitado_por": "Jaizer"},
        ]
        self.hallazgos = [
            {"maquina_id": 9, "visita_id": 513, "severidad": "falla", "titulo": "Banda desalineada", "estado": "tomado",
             "detectado_en": AHORA - timedelta(days=3), "detectado_por": "Lenin Urbina", "tomado_por": "Víctor",
             "descartado_en": None, "descartado_por": None, "ot_seguimiento_id": None, "seg_estado": None, "seg_cerrada": None},
        ]

    def fetchall(self, sql, params=()):
        s = " ".join(sql.split())
        self.consultas.append(s)
        if "FROM mant_maquina_paradas" in s:
            return [dict(r) for r in self.paradas]
        if "FROM mant_ot_repuesto_solicitudes" in s:
            return [dict(r) for r in self.solicitudes]
        if "FROM mant_ot_hallazgos h" in s:
            assert "h.severidad NOT IN ('fuera_servicio','repuesto')" in s
            return [dict(r) for r in self.hallazgos]
        if s.startswith("SELECT id, numero_ot FROM mant_visitas WHERE id IN"):
            return [{"id": i, "numero_ot": "OT-2026-%05d" % i} for i in params]
        raise AssertionError("consulta no prevista: " + s[:160])


def _amb(base):
    amb = {
        "datetime": datetime, "mysql_fetchall": base.fetchall, "_hv_asegurar": lambda: None, "_hall_ok": lambda: True,
        "_oh": ot_hallazgos, "_FS_MARCA_RIESGO": "⚠ PUEDE LASTIMAR A ALGUIEN",
        "_OTREP_ABIERTOS": ("solicitado", "validado", "pedido", "recibido"),
        "_OTREP_ESTADO_LABEL": {"pedido": "Pedido al proveedor", "instalado": "Instalado"},
        "chile_fmt_filter": lambda dt, f="": dt.strftime(f),
    }
    exec(compile(ast.parse(_fuente("_maq_problemas")), "<_maq_problemas>", "exec"), amb)
    return amb


class TestMaqProblemas(unittest.TestCase):
    def setUp(self):
        self.base = _Base()
        self.f = _amb(self.base)["_maq_problemas"]

    def test_paradas_repuestos_y_hallazgos_con_cuando_se_levanto_y_cuando_se_reparo(self):
        r = self.f([9, 10])
        p9 = r[9]
        self.assertEqual([x["abierto"] for x in p9], [True, True, True, False], "abiertos primero")
        fs = next(x for x in p9 if x["titulo"] == "Fuera de servicio: Motor no enciende")
        self.assertTrue(fs["estado_txt"].startswith("Sigue detenido · hace 3 días"))
        self.assertEqual(fs["levantado_ot"], {"numero": "OT-2026-00513", "url": "/ot/513"})
        self.assertEqual(fs["levantado_por"], "Lenin Urbina")
        vieja = p9[-1]
        self.assertEqual(vieja["titulo"], "Fuera de servicio: Piola cortada")
        self.assertEqual(vieja["resuelto_txt"], "Volvió a operar")
        self.assertEqual(vieja["resuelto_ot"]["numero"], "OT-2026-00410")
        self.assertEqual(vieja["estado_txt"], "En 2 días")
        rep = next(x for x in p9 if x["tipo"] == "repuesto")
        self.assertEqual(rep["titulo"], "Repuesto: Pasillo × 2")
        self.assertTrue(rep["estado_txt"].startswith("Pedido al proveedor · hace"))
        hal = next(x for x in p9 if x["tipo"] == "hallazgo")
        self.assertEqual(hal["estado_txt"][:23], "Caso tomado por Víctor ")
        p10 = r[10]
        self.assertEqual(p10[0]["titulo"], "Cambio de piola")
        self.assertEqual(p10[0]["resuelto_txt"], "Instalado")

    def test_un_tecnico_no_recibe_hallazgos(self):
        r = self.f([9], con_hallazgos=False)
        self.assertFalse([x for x in r[9] if x["tipo"] == "hallazgo"])
        self.assertFalse([c for c in self.base.consultas if "mant_ot_hallazgos" in c])

    def test_sin_equipos_no_consulta_nada(self):
        self.assertEqual(self.f([]), {})
        self.assertEqual(self.base.consultas, [])

    def test_si_la_base_falla_la_ficha_sigue(self):
        def rota(*a, **k):
            raise RuntimeError("caída")
        amb = _amb(self.base)
        amb["mysql_fetchall"] = rota
        self.assertEqual(amb["_maq_problemas"]([9]), {})

    def test_sin_plata_ni_proveedores(self):
        src = _fuente("_maq_problemas")
        self.assertNotRegex(src, r"(?i)costo|monto|proveedor_id|precio|margen")


class TestPlantillas(unittest.TestCase):
    def _env(self):
        def cargar(n):
            return _leer("templates/" + n)
        env = jinja2.Environment(loader=jinja2.FunctionLoader(cargar), autoescape=True)
        return env

    def test_la_macro_dibuja_las_dos_fechas(self):
        base = _Base()
        probs = _amb(base)["_maq_problemas"]([9])[9]
        html = self._env().from_string("{% import 'mantenciones/_problemas.html' as pb %}{{ pb.lista(p) }}").render(p=probs)
        txt = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))
        self.assertIn("Se levantó el", txt)
        self.assertIn("Volvió a operar el", txt)
        self.assertIn('href="/ot/513"', html)

    def test_la_ficha_y_la_hoja_de_vida_la_usan(self):
        ficha = _leer("templates/mantenciones/ficha.html")
        i = ficha.index('<div class="ftab-pane" id="tab-equipos">')
        self.assertIn('id="eqProblemas"', ficha[i:i + 9000])
        self.assertIn("{% import 'mantenciones/_problemas.html' as _pb %}", ficha)
        hv = _leer("templates/mantenciones/maquina_ficha.html")
        self.assertIn('id="hvProblemas"', hv)
        for t in (ficha, hv):
            jinja2.Environment(extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"]).parse(t)

    def test_el_servidor_le_quita_los_hallazgos_al_tecnico(self):
        src = _leer("app.py")
        self.assertIn("_maq_problemas([_m[\"id\"] for _m in maquinas], con_hallazgos=not _es_rol_tecnico())", src)
        self.assertIn("_maq_problemas([mid], con_hallazgos=not _es_rol_tecnico())", src)


if __name__ == "__main__":
    unittest.main()
