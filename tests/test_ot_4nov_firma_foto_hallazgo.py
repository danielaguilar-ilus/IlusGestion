# -*- coding: utf-8 -*-
"""📸 4-nov-2026 — OT-2026-00359: la pantalla decía «Todos los equipos tienen sus obligatorias completas — ya puedes firmar y cerrar la OT» y,
al firmar, el servidor respondía «faltan fotos». Daniel: «los muchachos no pueden cerrar la OT 359, dice que está completo pero también incompleto».

Causa (medida en producción, solo lectura): desde el 30-oct la firma exige la foto de TODO hallazgo (una tarea en Falla o Alerta, o un Resultado
distinto de «Apto»): `_ot_hallazgos_sin_foto_razones`. La pantalla, en cambio, solo pedía esa foto en las tareas con la marca «foto si hay hallazgo»
de su plantilla (`foto_si_hallazgo`), y las 3 tareas de esta OT (Escaladora X1, Dynamis Seated Leg Press, Versa Half Rack: «Falla», `foto_si_hallazgo=0`,
`requiere_foto=0`) no la tenían: ni se contaban como pendientes (todo «Listo») ni tenían botón de foto. Un candado sin llave.

Se prueba:
  1. `otdChkEvaluar` con el modo estricto: sin la marca de la plantilla, un hallazgo sin foto queda pendiente («foto_hallazgo»); con la foto, no.
  2. La definición de «hallazgo» de la pantalla es la MISMA que la del servidor (ot_hallazgos.evaluar_tarea) en una matriz de casos.
  3. Solo con la OT sin firmar (la constante de la plantilla), y sin cambiar nada del modo de siempre (el espejo de _chk_revision_evaluar sigue igual).
  4. El servidor dice EN QUÉ EQUIPO está cada hallazgo sin foto (texto de la firma y /puede-firmar) y la pantalla lleva al técnico hasta allí.
Sin BD, sin Flask, sin ERP (REGLA #4.1), sin correos ni clientes (REGLA #22). Las funciones JS corren de verdad en node si está instalado.
Correr:  python -m unittest tests.test_ot_4nov_firma_foto_hallazgo
"""
import json
import re
import unittest

import ot_hallazgos as OH
from tests import _fuente_app as FA
from tests.test_ot_revision_v3 import HAY_NODE, _js_funcion, _node

D = FA.texto("templates/ot2/detalle.html")

FUNCS = ("otdChkNorm", "otdChkVj", "otdChkOpciones", "otdChkEsResultado", "otdChkOpcion", "otdChkValorVerif",
         "otdChkGrupoClave", "otdChkOpcionEsHallazgo", "otdChkEsHallazgoFirma", "otdChkEvaluar")
OPC_RESULTADO = ["Apto", "Apto con observaciones", "Reparar", "Dar de baja", "Fuera de servicio", "No llegó", "Pendiente de diagnóstico"]


def _tarea(tid, tipo, titulo, vj=None, ops=None, fotos=0, fsh=0, obl=1, completada=1, maquina=None):
    """Una tarea tal como la entrega /api/visitas/<id>/checklist (fotos = lista; valor_json = dict)."""
    return {"id": tid, "maquina_id": maquina if maquina is not None else tid, "item_manual_id": None, "tipo_respuesta": tipo,
            "titulo": titulo, "obligatoria": obl, "completada": completada, "foto_si_hallazgo": fsh,
            "opciones_lista_json": json.dumps(ops, ensure_ascii=False) if ops is not None else None,
            "valor_json": vj, "fotos": [{"url": "https://x/%d.jpg" % i} for i in range(fotos)]}


def _js(cuerpo):
    return _node("\n".join(_js_funcion(n, D) for n in FUNCS) + "\n" + cuerpo)


def _evaluar(tareas, estricto):
    out = _js("const T = " + json.dumps(tareas, ensure_ascii=False) + ";\n"
              "const r = otdChkEvaluar(T, {}, " + ("true" if estricto else "false") + ");\n"
              "console.log(JSON.stringify(r.por_tarea));")
    return {int(k): v for k, v in out.items()}


# Las tres tareas reales de la OT-2026-00359 (respondidas «Falla», sin la marca de foto y sin foto).
OT359 = [
    _tarea(29153, "verificacion", "Prueba de funcionamiento del motor y consola", {"valor": "falla"}),
    _tarea(29811, "verificacion", "Verificar tapas y protecciones.", {"valor": "falla"}),
    _tarea(29839, "verificacion", "• Verificar ruedas y frenos en modelos moviles (si aplica).", {"valor": "falla"}),
]


@unittest.skipUnless(HAY_NODE, "node no está instalado")
class TestModoEstricto(unittest.TestCase):
    def test_sin_el_modo_estricto_nada_cambia(self):
        """El espejo de _chk_revision_evaluar sigue igual: sin `estricto`, estas tareas quedan «hechas» (así decía la pantalla)."""
        for e in _evaluar(OT359, False).values():
            self.assertTrue(e["exigible"] and e["hecha"])
            self.assertIsNone(e["motivo"])

    def test_los_tres_hallazgos_de_la_ot_359_quedan_pendientes_de_foto(self):
        ev = _evaluar(OT359, True)
        for tid in (29153, 29811, 29839):
            self.assertTrue(ev[tid]["exigible"], tid)
            self.assertFalse(ev[tid]["hecha"], tid)
            self.assertEqual(ev[tid]["motivo"], "foto_hallazgo", tid)

    def test_con_la_foto_ya_no_falta(self):
        con_foto = [dict(t, fotos=[{"url": "https://x/a.jpg"}]) for t in OT359]
        for e in _evaluar(con_foto, True).values():
            self.assertTrue(e["hecha"])
            self.assertIsNone(e["motivo"])

    def test_aprobado_y_no_aplica_no_piden_foto(self):
        ev = _evaluar([_tarea(1, "verificacion", "Cable", {"valor": "aprobado"}),
                       _tarea(2, "verificacion", "Poleas", {"valor": "falla", "na": True, "na_motivo": "no aplica"})], True)
        self.assertTrue(ev[1]["hecha"] and ev[2]["hecha"])

    def test_el_estricto_es_el_de_la_constante_de_la_plantilla(self):
        """Sin pasar el 3.er argumento, manda OTD_FOTO_HALLAZGO_ESTRICTA (la plantilla la apaga con la OT firmada o cerrada)."""
        def con(constante):
            antes = ("const OTD_FOTO_HALLAZGO_ESTRICTA = %s;\n" % constante) if constante is not None else ""
            return _node("\n".join(_js_funcion(n, D) for n in FUNCS) + "\n" + antes +
                         "const T = " + json.dumps(OT359[:1], ensure_ascii=False) + ";\n"
                         "console.log(JSON.stringify(otdChkEvaluar(T).por_tarea));")
        self.assertEqual(con("true")["29153"]["motivo"], "foto_hallazgo")
        self.assertIsNone(con("false")["29153"]["motivo"])
        self.assertIsNone(con(None)["29153"]["motivo"], "sin la constante (otras pantallas, pruebas) se comporta como siempre")


@unittest.skipUnless(HAY_NODE, "node no está instalado")
class TestMismaDefinicionQueElServidor(unittest.TestCase):
    """La pantalla y el candado de la firma dicen lo mismo: una tarea sin foto es pendiente en la pantalla (modo estricto)
    si y solo si ot_hallazgos.evaluar_tarea la considera un hallazgo (la definición que usa _ot_hallazgos_sin_foto_razones)."""

    def _casos(self):
        casos, n = [], 100
        for v in ("aprobado", "alerta", "falla", "", "Falla", " alerta "):
            for na in (False, True):
                n += 1
                vj = {"valor": v} if v else {}
                if na:
                    vj = dict(vj, na=True)
                casos.append(_tarea(n, "verificacion", "Verificación %d" % n, vj or None))
        for opcion in OPC_RESULTADO + ["Sin resultado", "No llegó el producto", "No apto", "Baja", "Preparar para la venta",
                                       "Puede seguir trabajando", "Otra cosa", ""]:
            for na in (False, True):
                n += 1
                vj = {"opcion": opcion} if opcion else {}
                if na:
                    vj = dict(vj, na=True)
                casos.append(_tarea(n, "lista", "Resultado %d" % n, vj or None, ops=OPC_RESULTADO))
        for opcion in ("Reparar", "Bueno", "Malo"):          # una lista cualquiera NO es un hallazgo
            n += 1
            casos.append(_tarea(n, "lista", "Lista %d" % n, {"opcion": opcion}, ops=["Bueno", "Regular", "Malo", "Reparar"]))
        for tipo, vj in (("texto", {"texto": "ok"}), ("numero", {"valor": 3}), ("sino", {"valor": "si"}), ("foto", {})):
            n += 1
            casos.append(_tarea(n, tipo, "Otro %d" % n, vj))
        return casos

    def test_la_pantalla_pide_foto_si_y_solo_si_el_servidor_la_pide(self):
        casos = self._casos()
        ev = _evaluar(casos, True)
        mal = []
        for t in casos:
            servidor = OH.evaluar_tarea(dict(t, valor_json=json.dumps(t["valor_json"]) if t["valor_json"] is not None else None)) is not None
            pantalla = ev[t["id"]]["motivo"] == "foto_hallazgo"
            if servidor != pantalla:
                mal.append((t["titulo"], t["valor_json"], "servidor" if servidor else "pantalla"))
        self.assertEqual(mal, [], "la pantalla y el candado de la firma no coinciden en: %r" % (mal,))
        self.assertGreater(sum(1 for t in casos if ev[t["id"]]["motivo"] == "foto_hallazgo"), 10, "la matriz debe incluir hallazgos de verdad")


class TestServidor(unittest.TestCase):
    @staticmethod
    def _fila(tid, titulo, maquina, nombre, valor="falla"):
        return {"id": tid, "titulo": titulo, "maquina_id": maquina, "maquina_nombre": nombre, "tipo_respuesta": "verificacion",
                "valor_json": json.dumps({"valor": valor}), "opciones_lista_json": None}

    def _amb(self, filas):
        return FA.cargar(["_ot_hallazgos_sin_foto_tareas", "_ot_hallazgos_sin_foto_razones"], extra={
            "mysql_fetchall": lambda *a, **k: filas, "_oh": OH, "_ot_tarea_no_trabajable_sql": lambda p: ""})

    def test_el_texto_de_la_firma_dice_en_que_equipo_esta_cada_tarea(self):
        filas = [self._fila(29153, "Prueba de funcionamiento del motor y consola", 6451, "Escaladora ILUS X1"),
                 self._fila(29811, "Verificar tapas y protecciones.", 8233, "ILUS Dynamis Seated Leg Press"),
                 self._fila(29839, "• Verificar ruedas y frenos en modelos moviles (si aplica).", 6463, "ILUS Versa Half Rack")]
        r = self._amb(filas)["_ot_hallazgos_sin_foto_razones"](536)
        self.assertEqual(len(r), 1)
        self.assertIn("3 hallazgo(s)", r[0])
        self.assertIn("Escaladora ILUS X1 — Prueba de funcionamiento del motor y consola", r[0])
        self.assertIn("ILUS Dynamis Seated Leg Press — Verificar tapas y protecciones.", r[0])
        self.assertIn("ILUS Versa Half Rack — • Verificar ruedas y frenos", r[0])
        self.assertIn("; ", r[0], "cada «equipo — tarea» va separado con punto y coma (los títulos ya traen comas)")

    def test_sin_nombre_de_equipo_queda_solo_el_titulo(self):
        r = self._amb([self._fila(1, "Cable", None, None)])["_ot_hallazgos_sin_foto_razones"](7)
        self.assertIn("Cable", r[0])
        self.assertNotIn(" — ", r[0])

    def test_el_listado_trae_equipo_y_tarea_y_respeta_los_excluidos(self):
        filas = [self._fila(1, "Cable", 9, "Trotadora A"), self._fila(2, "Poleas", 10, "Rack B"),
                 self._fila(3, "Aprobado", 11, "Banco C", valor="aprobado")]
        amb = self._amb(filas)
        sin = amb["_ot_hallazgos_sin_foto_tareas"](7, {9})
        self.assertEqual([(t["id"], t["maquina_nombre"]) for t in sin], [(2, "Rack B")])
        self.assertEqual(len(amb["_ot_hallazgos_sin_foto_tareas"](7)), 2)

    def test_puede_firmar_dice_donde_esta_cada_hallazgo(self):
        src = FA.fuente_funcion("mant_ot_puede_firmar")
        self.assertIn("_ot_hallazgos_sin_foto_tareas(vid, excluir_maquinas)", src)
        self.assertIn('"pendientes": _pend_foto', src)
        self.assertIn('"primer_equipo_id": (_pend_foto[0]["maquina_id"] if _pend_foto else None)', src)
        self.assertIn("except Exception as _e_pf", src, "fail-open: si no se puede armar el detalle queda el texto de siempre")

    def test_la_consulta_nueva_no_rompe_el_group_by(self):
        """El nombre del equipo va con MAX(): sigue siendo válida con ONLY_FULL_GROUP_BY (si fallara, el candado se abriría en silencio)."""
        src = FA.fuente_funcion("_ot_hallazgos_sin_foto_tareas")
        self.assertIn("MAX(m.nombre) AS maquina_nombre", src)
        self.assertIn("GROUP BY t.id HAVING COUNT(f.id) = 0", src)
        self.assertIn("LEFT JOIN mant_maquinas m ON m.id = t.maquina_id", src)


class TestPantalla(unittest.TestCase):
    def test_la_constante_solo_vale_con_la_ot_sin_firmar(self):
        m = re.search(r"const OTD_FOTO_HALLAZGO_ESTRICTA = (\{\{.*?\}\});", D)
        self.assertIsNotNone(m, "falta OTD_FOTO_HALLAZGO_ESTRICTA en detalle.html")
        from jinja2 import Environment
        plantilla = Environment().from_string(m.group(1))

        def val(estado, solo_lectura=False):
            return plantilla.render(v={"estado": estado}, ot_solo_lectura=solo_lectura)
        self.assertEqual(val("en_ejecucion"), "true")
        self.assertEqual(val("programada"), "true")
        for cerrada in ("firmada_tecnico", "pendiente_aprobacion", "completada", "cerrada", "cancelada", "anulada", "CERRADA"):
            self.assertEqual(val(cerrada), "false", cerrada)
        self.assertEqual(val("en_ejecucion", solo_lectura=True), "false")
        self.assertEqual(plantilla.render(v={"estado": None}, ot_solo_lectura=False), "true")

    def test_la_plantilla_sigue_siendo_jinja_valido(self):
        from jinja2 import Environment
        Environment().parse(D)

    def test_el_foto_se_ofrece_en_la_tarea_del_hallazgo(self):
        """Sin la marca de la plantilla la tarea no tenía botón de foto: ahora «Falta foto del hallazgo» sale por t._ev."""
        btn = _js_funcion("otdChkFotoBtnHtml", D)
        self.assertIn("const _pideHallazgo = !!(t._ev && t._ev.motivo === 'foto_hallazgo');", btn)
        self.assertIn("'Falta foto del hallazgo'", btn)
        self.assertIn("otdChkEsHallazgoFirma(t)", btn, "con foto, el hallazgo deja «Agregar otra»")

    def test_el_aviso_previo_lleva_a_la_tarea(self):
        f = _js_funcion("otdPreflightFirma", D)
        self.assertIn("d.pendientes", f)
        self.assertIn("await ilusConfirm({", f)
        self.assertIn("okLabel: 'Ir a la primera'", f)
        self.assertIn("otdIrATarea(pend[0].tarea_id, pend[0].maquina_id)", f)
        self.assertIn("d.primer_equipo_id", f, "el servidor ya mandaba el primer equipo pendiente y la pantalla no lo usaba")
        self.assertNotIn("alert(", f.replace("ilusAlert(", ""), "REGLA #1: nada de alert nativo")

    def test_ir_a_la_tarea_abre_el_equipo_y_la_resalta(self):
        if not HAY_NODE:
            self.skipTest("node no está instalado")
        cuerpo = r"""
        const abiertos = [], resaltados = [];
        const eq = {dataset: {mid: '6451'}};
        const fila = {style: {}, scrollIntoView(){ resaltados.push('scroll'); }};
        global.window = {};
        global.document = {
          getElementById(id){ return id === 'otdChkItem-29153' ? fila : (id === 'otdWkBuscar' ? {value: 'lector'} : null); },
          querySelector(sel){ return sel === '#otdWkCard .otd-wk-item[data-mid="6451"]' ? eq : null; },
        };
        global.setTimeout = function(f){ f(); return 0; };
        global._otdWkAbrir = function(el){ abiertos.push(el.dataset.mid); };
        window.otdWkFiltrar = function(q){ abiertos.push('filtro:' + q); };
        const ok = otdIrATarea(29153, 6451), noEsta = otdIrATarea(null, 99999), sinTarea = otdIrATarea(null, 6451);
        console.log(JSON.stringify({ok, noEsta, sinTarea, abiertos, resaltados, sombra: fila.style.boxShadow === ''}));
        """
        out = _node(_js_funcion("otdIrATarea", D) + "\n" + cuerpo)
        self.assertTrue(out["ok"])
        self.assertFalse(out["noEsta"], "si no encuentra la tarjeta lo dice (la pantalla cae a «Siguiente pendiente»)")
        self.assertTrue(out["sinTarea"])
        self.assertEqual(out["abiertos"][0], "filtro:", "limpia la búsqueda para que el equipo no quede escondido")
        self.assertIn("6451", out["abiertos"])
        self.assertEqual(out["resaltados"], ["scroll"])


if __name__ == "__main__":
    unittest.main()
