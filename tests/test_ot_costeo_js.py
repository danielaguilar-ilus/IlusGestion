# -*- coding: utf-8 -*-
"""🔎 Costeo de OT · Explorar (FASE 1, 2026-10-29) — el navegador (static/ot_costeo_explorar.js) ejecutado con Node y comparado
con su espejo en Python (ot_costeo.py): mismo filtro, misma suma (que a su vez es la de _ot_fin_agregado), misma política
de margen, mismas fechas. Más lo que la pantalla promete: cada visual excluye su propio filtro, limpiar recalcula, el
estado vive en el enlace, sin alert/confirm/prompt nativos y sin nombres cortados.

Datos de MENTIRA (REGLA #22). Se salta si no hay Node.
Correr con:  python -m unittest tests.test_ot_costeo_js
"""
import json
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ot_costeo as C  # noqa: E402
import ot_gerencial as G  # noqa: E402
import ot_margen_politica as POL  # noqa: E402
from tests import _ger_app as A  # noqa: E402
from tests.test_ot_costeo import construir  # noqa: E402

JS = os.path.join(A.RAIZ, "static", "ot_costeo_explorar.js")

FILTROS = [
    {}, {"tec": ["u:41"]}, {"emp": ["emp:mili servicios spa"], "cc": ["sstt"]}, {"mes": ["2026-10"]},
    {"clase": ["sin_medir"]}, {"cob": ["garantia", "regalia"]}, {"banda": ["objetivo", "tolerancia"]},
    {"doc": ["factura", "sin_documento"]}, {"ttipo": ["externo"], "anio": ["2026"]}, {"nsem": ["42", "43"]},
    {"tec": ["no-existe"]},
]
EXCEPTO = [([], {}), (["tec"], {"tec": ["u:41"], "cc": ["sstt"]}), (["cc", "tec"], {"tec": ["u:41"], "cc": ["sstt"]}),
           (["mes"], {"mes": ["2026-10"], "emp": ["propios"]})]


def correr_node(dataset, escenario):
    """Carga el JS real como módulo y ejecuta `escenario` (cuerpo de función con `X` = el módulo y `D` = el dataset)."""
    with tempfile.TemporaryDirectory() as tmp:
        ruta_d = os.path.join(tmp, "d.json")
        with open(ruta_d, "w", encoding="utf-8") as fh:
            json.dump(dataset, fh, ensure_ascii=False)
        script = ("const X = require(%s); const D = JSON.parse(require('fs').readFileSync(%s, 'utf8'));\n"
                  "const salida = (function () {\n%s\n})();\n"
                  "process.stdout.write(JSON.stringify(salida));" % (json.dumps(JS), json.dumps(ruta_d), escenario))
        ruta_s = os.path.join(tmp, "s.js")
        with open(ruta_s, "w", encoding="utf-8") as fh:
            fh.write(script)
        r = subprocess.run(["node", ruta_s], capture_output=True, text=True, encoding="utf-8", timeout=120)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


CLAVES_KPI = ("n", "cobre", "me_cobraron", "queda", "pct", "n_fuera", "cobre_fuera", "n_no_cobra", "costo_no_cobra", "n_ot",
              "a_pagar", "costo_total", "val_reg", "n_perdidas", "perdido", "aparte_costo", "n_aparte", "banda")


@unittest.skipUnless(shutil.which("node"), "node no está instalado")
class TestEspejoPythonJS(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        trios, cls.p = construir()
        cls.filas = [t[2] for t in trios]
        cls.actuales = C.actuales(cls.filas)
        cls.dataset = C.armar_dataset(cls.filas, cls.p, {"generado": "29/10/2026 10:00"})
        cls.res = correr_node(cls.dataset, """
          const todas = X.aObjetos(D), act = todas.filter(r => !r.prev), pol = D.meta.politica, reg = D.meta.regaladas;
          const FILTROS = %s, EXCEPTO = %s;
          const filtros = FILTROS.map(f => { const fs = X.filtrar(act, f); return {ids: fs.map(r => r.id), k: X.kpis(fs, reg, pol)}; });
          const excepto = EXCEPTO.map(e => X.filtrar(act, e[1], e[0]).map(r => r.id));
          const grupos = {};
          ['tec', 'emp', 'cc', 'mes', 'doc', 'banda'].forEach(d => { grupos[d] = X.agrupar(act, d, reg, pol); });
          const prevSin = X.previo(todas, {emp: ['emp:mili servicios spa']}, D.meta);
          const prevMes = X.previo(todas, {mes: ['2026-10'], ttipo: ['externo']}, D.meta);
          const prevDos = X.previo(todas, {mes: ['2026-10'], anio: ['2026']}, D.meta);
          const prevFuera = X.previo(todas, {mes: ['2026-08']}, D.meta);
          return {filtros, excepto, grupos, n_todas: todas.length, n_act: act.length,
                  prev: {sin: prevSin.filas.map(r => r.id), mes: prevMes.filas.map(r => r.id), mes_txt: prevMes.texto,
                         dos: prevDos.filas, dos_txt: prevDos.texto, fuera: prevFuera.filas, fuera_txt: prevFuera.texto}};
        """ % (json.dumps(FILTROS), json.dumps(EXCEPTO)))

    def test_el_dataset_por_columnas_se_rearma_igual(self):
        self.assertEqual(self.res["n_todas"], len(self.filas))
        self.assertEqual(self.res["n_act"], len(self.actuales))

    def test_mismo_filtro_y_misma_suma_que_python(self):
        for f, js in zip(FILTROS, self.res["filtros"]):
            py = C.filtrar(self.actuales, f)
            self.assertEqual(js["ids"], [r["id"] for r in py], f)
            kp = C.kpis(py)
            for k in CLAVES_KPI:
                self.assertEqual(js["k"][k], kp[k], f"{f} · {k}")
            self.assertEqual({c: [a["n"], a["costo"]] for c, a in js["k"]["aparte"].items()},
                             {c: [a["n"], a["costo"]] for c, a in kp["aparte"].items()}, f)

    def test_y_esa_suma_es_la_de_ot_fin_agregado(self):
        fins = {t[2]["id"]: t[1] for t in construir()[0]}
        for f, js in zip(FILTROS, self.res["filtros"]):
            ag = A.cuenta_real()["_ot_fin_agregado"]([fins[i] for i in js["ids"]])
            for k in ("cobre", "me_cobraron", "queda", "pct", "n", "n_fuera", "cobre_fuera"):
                self.assertEqual(js["k"][k], ag[k], f"{f} · {k}")

    def test_cada_visual_excluye_su_propio_filtro(self):
        for (exc, f), js in zip(EXCEPTO, self.res["excepto"]):
            self.assertEqual(js, [r["id"] for r in C.filtrar(self.actuales, f, exc)], (exc, f))
        # la lista de técnicos con un técnico elegido sigue mostrando a TODOS (los del centro elegido)
        con = self.res["excepto"][1]
        solo = C.filtrar(self.actuales, {"tec": ["u:41"], "cc": ["sstt"]})
        self.assertGreater(len(con), len(solo))

    def test_agrupar_da_lo_mismo_que_sumar_cada_grupo(self):
        for dim, grupos in self.res["grupos"].items():
            self.assertEqual(sum(g["n_ot"] for g in grupos), len(self.actuales))
            for g in grupos:
                py = C.kpis(C.filtrar(self.actuales, {dim: [g["clave"]]}))
                for k in ("cobre", "me_cobraron", "queda", "pct", "a_pagar", "banda"):
                    self.assertEqual(g[k], py[k], f"{dim}={g['clave']} · {k}")

    def test_el_periodo_anterior_para_comparar(self):
        pr = self.res["prev"]
        esperado = [r["id"] for r in self.filas if r["prev"] and r["emp"] == "emp:mili servicios spa"]
        self.assertEqual(pr["sin"], esperado, "sin filtro de tiempo: el período anterior cargado")
        self.assertEqual(pr["mes"], [r["id"] for r in C.filtrar(self.filas, {"mes": ["2026-09"], "ttipo": ["externo"]})],
                         "octubre se compara con septiembre, con los mismos otros filtros")
        self.assertEqual(pr["mes_txt"], "Septiembre 2026")
        self.assertIsNone(pr["dos"])
        self.assertIn("dos unidades de tiempo", pr["dos_txt"])
        self.assertIsNone(pr["fuera"], "julio (el anterior a agosto) no está cargado completo: no se compara a medias")
        self.assertIn("amplía «Desde»", pr["fuera_txt"])


@unittest.skipUnless(shutil.which("node"), "node no está instalado")
class TestPiezasPuras(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        rnd = random.Random(29)
        cls.valores = [round(rnd.uniform(-5e6, 5e7), rnd.choice((2, 3, 4))) for _ in range(400)]
        cls.valores += [0.125, 0.375, -0.125, 1234.125, 12.25, 12.35, 99.995, 0.5, 1.5, 2.5, -2.5, 1e-9, 100.0, 0.0]
        cls.pcts = [None, -0.01, 0, 0.01, 19.99, 20, 25, 29.99, 30, 31, 100] + [round(rnd.uniform(-60, 90), 1) for _ in range(60)]
        d0 = date(2023, 12, 20)
        cls.fechas = [(d0 + timedelta(days=i)).isoformat() for i in range(0, 1200, 3)]
        cls.cmp = [(1000, None, "sube"), (1000, 1000.2, "sube"), (1500, 1000, "sube"), (500, 1000, "sube"), (500, 1000, "baja"),
                   (500, 1000, "neutro"), (10, 0.5, "sube"), (-300, 200, "sube")]
        cls.res = correr_node({"valores": cls.valores, "pcts": cls.pcts, "fechas": cls.fechas, "cmp": cls.cmp}, """
          const pol = {objetivo: 30, tolerancia: 20, perdida: 0};
          const e0 = {d: '2026-01-01', h: '2026-10-29', fe: 'creacion', al: 'todas', g: 'sem', m: 'queda', mx: 'val_reg', mr: 'emp',
                      f: {tec: ['u:41', 'emp:a, b & c'], mes: ['2026-10']}};
          let f = {};
          const pasos = [];
          f = X.alternar(f, 'tec', 'u:41', 'solo'); pasos.push(f);
          f = X.alternar(f, 'tec', 'u:42', 'solo'); pasos.push(f);
          f = X.alternar(f, 'tec', 'u:41', 'sumar'); pasos.push(f);
          f = X.alternar(f, 'cc', 'sstt', 'sumar'); pasos.push(f);
          f = X.quitar(f, 'tec', 'u:42'); pasos.push(f);
          f = X.alternar(f, 'cc', 'sstt', 'solo'); pasos.push(f);
          f = X.quitar(f, 'tec'); pasos.push(f);
          const par1 = X.alternarPar({}, 'tec', 'u:1', 'cc', 'sstt', 'solo');
          const par2 = X.alternarPar(par1, 'tec', 'u:1', 'cc', 'sstt', 'solo');
          return {
            round2: D.valores.map(v => X.pyRound(v, 2)), round1: D.valores.map(v => X.pyRound(v, 1)),
            banda: D.pcts.map(p => X.banda(p, null, pol)), banda_q: X.banda(null, -5, pol),
            semana: D.fechas.map(X.semanaISO), lunes: D.fechas.map(X.lunesDe),
            etiq: D.fechas.slice(0, 40).map(d => [X.etiquetaTiempo('dia', d), X.etiquetaTiempo('sem', X.lunesDe(d)),
                                                   X.etiquetaTiempo('mes', d.slice(0, 7))]),
            cmp: D.cmp.map(c => X.comparar(c[0], c[1], c[2])),
            pasos, par1, par2,
            hash: X.estadoAHash(e0), vuelta: X.hashAEstado(X.estadoAHash(e0)),
            malo: X.hashAEstado('#explorar?fe=DROP&g=xx&f=%7Bmal'), malo2: X.hashAEstado('#explorar?f=' + encodeURIComponent(JSON.stringify({hack: ['1'], tec: [1, {}, 'u:2']}))),
            nada: X.hashAEstado('#otra-cosa'),
            clp: [X.clp(1234567), X.clp(-1234), X.clp(0), X.clp(null), X.clp(5000, true)],
            pct: [X.pctTxt(12.5), X.pctTxt(30), X.pctTxt(null), X.pctTxt(-3.25)],
            pcts_txt: D.pcts.map(p => X.pctTxt(p)),
            fecha: X.fechaTxt('2026-10-21'),
            claves_mes: X.clavesRango('mes', '2025-11-15', '2026-02-02'), claves_sem: X.clavesRango('sem', '2026-10-01', '2026-10-20'),
            claves_dia_largo: X.clavesRango('dia', '2026-01-01', '2026-12-31'),
            desplazar: [X.desplazar('anio', '2026'), X.desplazar('mes', '2026-01'), X.desplazar('sem', '2026-01-05'),
                        X.desplazar('dia', '2026-03-01'), X.desplazar('nsem', '10'), X.desplazar('nsem', '01')]
          };
        """)

    def test_redondeo_igual_a_python(self):
        self.assertEqual(self.res["round2"], [round(v, 2) for v in self.valores])
        self.assertEqual(self.res["round1"], [round(v, 1) for v in self.valores])

    def test_banda_igual_a_la_politica(self):
        self.assertEqual(self.res["banda"], [POL.banda(p) for p in self.pcts])
        self.assertEqual(self.res["banda_q"], "rojo")

    def test_semanas_iso_y_lunes_igual_a_python(self):
        self.assertEqual(self.res["semana"], [date.fromisoformat(d).isocalendar()[1] for d in self.fechas])
        self.assertEqual(self.res["lunes"], [C.derivar(date.fromisoformat(d))["sem"] for d in self.fechas])

    def test_etiquetas_de_tiempo_iguales_a_python(self):
        for d, (dia, sem, mes) in zip(self.fechas, self.res["etiq"]):
            der = C.derivar(date.fromisoformat(d))
            self.assertEqual(dia, C.etiqueta_tiempo("dia", d))
            self.assertEqual(sem, C.etiqueta_tiempo("sem", der["sem"]))
            self.assertEqual(mes, C.etiqueta_tiempo("mes", der["mes"]))
            self.assertIsNone(re.search(r"\d{4}-\d{2}", dia + sem + mes), "nunca ISO a la vista")

    def test_comparar_igual_a_ot_gerencial(self):
        for (v, p, m), js in zip(self.cmp, self.res["cmp"]):
            self.assertEqual(js, G.comparar(v, p, m), (v, p, m))

    def test_clic_deja_solo_ese_ctrl_suma_y_limpiar_recalcula(self):
        pasos = self.res["pasos"]
        self.assertEqual(pasos[0], {"tec": ["u:41"]})
        self.assertEqual(pasos[1], {"tec": ["u:42"]}, "un clic deja SOLO ese valor")
        self.assertEqual(pasos[2], {"tec": ["u:42", "u:41"]}, "Ctrl+clic (o un toque) suma")
        self.assertEqual(pasos[3], {"tec": ["u:42", "u:41"], "cc": ["sstt"]})
        self.assertEqual(pasos[4], {"tec": ["u:41"], "cc": ["sstt"]}, "la × del chip quita ese valor")
        self.assertEqual(pasos[5], {"tec": ["u:41"]}, "clic sobre lo único elegido lo quita")
        self.assertEqual(pasos[6], {}, "quitar la dimensión entera")
        self.assertEqual(self.res["par1"], {"tec": ["u:1"], "cc": ["sstt"]}, "la celda filtra las dos dimensiones")
        self.assertEqual(self.res["par2"], {}, "otro clic en la misma celda las suelta")

    def test_el_estado_vive_en_el_enlace(self):
        self.assertTrue(self.res["hash"].startswith("#explorar?"))
        v = self.res["vuelta"]
        self.assertEqual((v["d"], v["h"], v["fe"], v["al"], v["g"], v["m"], v["mx"], v["mr"]),
                         ("2026-01-01", "2026-10-29", "creacion", "todas", "sem", "queda", "val_reg", "emp"))
        self.assertEqual(v["f"], {"tec": ["u:41", "emp:a, b & c"], "mes": ["2026-10"]}, "valores con comas y & sobreviven")
        self.assertEqual((self.res["malo"]["fe"], self.res["malo"]["g"], self.res["malo"]["f"]), ("cierre", "mes", {}),
                         "un enlace dañado arranca con los valores por defecto")
        self.assertEqual(self.res["malo2"]["f"], {"tec": ["1", "u:2"]}, "solo dimensiones conocidas y valores de texto")
        self.assertIsNone(self.res["nada"])

    def test_formatos_chilenos(self):
        self.assertEqual(self.res["clp"], ["$1.234.567", "-$1.234", "$0", "—", "+$5.000"])
        self.assertEqual(self.res["pct"], ["12,5 %", "30 %", "—", "-3,2 %"])
        for p, js in zip(self.pcts, self.res["pcts_txt"]):
            if p is None or abs(p) >= 0.05:          # (Python escribe «-0 %» para -0,01; el navegador, «0 %»: mejor así)
                self.assertEqual(js, G.pct_txt(p), "igual que el Resumen: %s" % p)
        self.assertEqual(self.res["fecha"], "21/10/2026")

    def test_la_serie_no_se_salta_los_periodos_sin_ot(self):
        self.assertEqual(self.res["claves_mes"], ["2025-11", "2025-12", "2026-01", "2026-02"])
        self.assertEqual(self.res["claves_sem"], ["2026-09-28", "2026-10-05", "2026-10-12", "2026-10-19"])
        self.assertEqual(self.res["claves_dia_largo"], [], "con más de 120 días solo los días con OT")
        self.assertEqual(self.res["desplazar"], ["2025", "2025-12", "2025-12-29", "2026-02-28", "09", None])


class TestFuentes(unittest.TestCase):
    """Lo que se lee del código sin ejecutarlo."""

    @classmethod
    def setUpClass(cls):
        cls.js = A.leer("static", "ot_costeo_explorar.js")
        cls.css = A.leer("static", "ot_costeo_explorar.css")

    def test_sin_alert_confirm_ni_prompt_nativos(self):
        for malo in ("alert(", "confirm(", "prompt("):
            self.assertNotIn(malo, self.js, "REGLA #1")

    def test_nombres_completos_nunca_cortados(self):
        for malo in ("text-overflow", "line-clamp", "ellipsis"):
            self.assertNotIn(malo, self.css, "REGLA #15")
        self.assertNotIn("…'", self.js.split("Cargando las OT")[0], "ningún nombre se corta con puntos suspensivos")
        self.assertIsNone(re.search(r"lab\([^)]*\)\.(slice|substr|substring)\(", self.js), "un nombre se muestra entero")
        self.assertIn("overflow-wrap:anywhere", self.css)

    def test_celular_primero_y_paleta_ilus(self):
        for pista in ("min-height:44px", "font-size:16px", "#dc2626", "#0a0a0a", "#16a34a", "#f59e0b", "prefers-reduced-motion",
                      "focus-visible", "@media (min-width:768px)", "overflow-x:auto"):
            self.assertIn(pista, self.css, pista)
        self.assertIn("grid-template-columns:repeat(2,minmax(0,1fr))", self.css, "en el celular las cifras van de a 2")

    def test_la_seleccion_no_usa_el_rojo_y_la_tarjeta_no_lleva_borde_de_acento(self):
        on = re.search(r"\.cx-item\.on\{[^}]*\}", self.css).group(0)
        self.assertNotIn("rojo", on)
        self.assertNotIn("dc2626", on)
        self.assertIsNone(re.search(r"\.cx-kpi[^{]*\{[^}]*border-(top|left):\s*[3-9]px", self.css))

    def test_paginacion_como_etiquetas_y_seleccionar_todo(self):
        for pista in ("Mostrando ", "Por página", "Anterior", "Página ", "Siguiente", "Seleccionar/deseleccionar todo",
                      "Excel de lo filtrado", "Modo presentación", "Limpiar todo", "¿Cómo se calcula?", "Se escapa", "Sin medir"):
            self.assertIn(pista, self.js, pista)
        self.assertIn("filas.some(function (r) { return !UI.sel[r.id]; })", self.js, "REGLA #14: si falta alguno, marca todos")

    def test_lo_que_trae_del_servidor_es_solo_de_superadmin(self):
        self.assertIn("credentials: 'same-origin'", self.js)
        self.assertNotIn("localStorage", self.js, "nada de plata guardada en el navegador")


if __name__ == "__main__":
    unittest.main()
