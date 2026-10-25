"""🤝 Asistente «Nueva OT» · paso Costos amigable (Daniel, 25-oct-2026, mirando la OT con valor de referencia $109.817).

Daniel: «lo valoricé en 109, pero abajo me dice cuánto nos cuesta; está en rojo y no me deja avanzar. Esto debe ser amigable… no un
instrumento que retrase la toma de decisiones. Que la información recopilada sea cuánto nos cobran, cuánto cobré, el margen y ya. No
hay que repetir opciones».

Qué se vigila:
  · La tarjeta ¿Cuánto nos cuesta? ya no dice «Opcional» en una garantía mientras el paso la exige (completo('finanzas') pide el costo
    del servicio SIEMPRE desde el 23-oct): una etiqueta que mentía y no dejaba avanzar sin decir por qué.
  · Lo que falta se dice en ÁMBAR, no en rojo (nadie se equivocó: solo falta un dato).
  · Con un técnico propio, un toque declara $0 («Es técnico propio»); con uno externo no se ofrece.
  · En un $0 el valor es solo referencia: queda plegado en una línea (los campos siguen en la página: nada se quita) y se abre con un toque.
  · Técnicos y responsable son requisitos que se resuelven al elegir: quedan en verde (sel-ok), no en rojo.
Correr con:  py -m unittest tests.test_ot_asistente_costos_amigable
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(rel):
    with open(os.path.join(RAIZ, rel.replace("/", os.sep)), encoding="utf-8", newline="") as f:
        return f.read().replace("\r\n", "\n")


H = _leer("templates/ot2/_modal_crear.html")


def _js_funcion(nombre):
    i = H.index("function %s(" % nombre)
    prof, j = 0, H.index("{", i)
    while True:
        if H[j] == "{":
            prof += 1
        elif H[j] == "}":
            prof -= 1
            if prof == 0:
                return H[i:j + 1]
        j += 1


def _correr(programa):
    f = os.path.join(tempfile.mkdtemp(), "prog.js")
    with open(f, "w", encoding="utf-8") as fh:
        fh.write(programa)
    r = subprocess.run(["node", f], capture_output=True, text=True, encoding="utf-8", timeout=60)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestElCostoYaNoMiente(unittest.TestCase):
    def _estado(self, **s):
        base = {"fin_centro": "comercial", "fin_garantia": False, "fin_aut": False, "fin_costo_proveedor": "", "fin_zz_monto": None,
                "fin_zz_motivo_manual": "", "fin_doc": None}
        base.update(s)
        prog = "\n".join([
            "var S = %s;" % json.dumps(base),
            "function esInterno(){ return false; }",
            "function _o2mFinEsManual(){ return false; } function _o2mFinPideMotivo(){ return false; }",
            "function _o2mFinEnvioEsManual(){ return false; } function _o2mFinObsFalta(){ return false; }",
            _js_funcion("_o2mFinSubEstado"),
            "console.log(JSON.stringify(_o2mFinSubEstado('costo')));"])
        return _correr(prog)

    def test_en_garantia_sin_costo_dice_falta_y_no_opcional(self):
        e = self._estado(fin_garantia=True)
        self.assertEqual(e["cls"], "falta")
        self.assertEqual(e["txt"], "Falta este dato")

    def test_con_el_costo_declarado_queda_listo_tambien_en_cero(self):
        for g in (True, False):
            e = self._estado(fin_garantia=g, fin_costo_proveedor="0")
            self.assertEqual((e["cls"], e["txt"]), ("listo", "Listo"), g)
            e = self._estado(fin_garantia=g, fin_costo_proveedor="109817")
            self.assertEqual(e["cls"], "listo", g)

    def test_fuera_de_garantia_sigue_pidiendolo(self):
        self.assertEqual(self._estado(fin_costo_proveedor="")["cls"], "falta")

    def test_la_tarjeta_y_el_candado_piden_lo_mismo(self):
        """El paso se bloquea por completo('finanzas'): la tarjeta debe pedir exactamente lo mismo (el costo, siempre)."""
        comp = _js_funcion("completo")
        self.assertIn("if (String(S.fin_costo_proveedor||'').trim() === '') return false;", comp)
        sub = _js_funcion("_o2mFinSubEstado")
        costo = sub[sub.index("case 'costo':"):sub.index("return {cls:'', txt:''};\n  }")]
        self.assertNotIn("'opcional'", costo, "el costo del servicio ya no es opcional en ningún caso")
        self.assertNotIn("S.fin_garantia", costo, "garantía ya no cambia lo que se exige del costo")

    def test_el_asterisco_ya_no_se_oculta_en_garantia(self):
        f = _js_funcion("pintarFinanzas")
        self.assertNotIn("(S.fin_garantia?'':' <span class=\"text-danger\">*</span>')", f)
        self.assertIn("Costo de '+(S.tipo==='instalacion'?'la instalación':'el servicio')+' <span style=\"color:#b45309\">*</span>", f)


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestUnToqueParaElTecnicoPropio(unittest.TestCase):
    def _chip(self, interno=False, tecnicos=(11,), externo=False, tarifa=None, costo=""):
        prog = "\n".join([
            "var S = %s;" % json.dumps({"tecnicos": list(tecnicos), "fin_costo_proveedor": costo}),
            "var el = {innerHTML: 'previo'};",
            "var document = {getElementById: function(id){ return id === 'o2mFinCostoChip' ? el : null; }};",
            "function esInterno(){ return %s; }" % json.dumps(interno),
            "function _primerTecnicoExterno(){ return %s; }" % ("{id:21,nombre:'Rafael',es_externo:true}" if externo else "null"),
            "function _o2mFinTecExtTarifa(){ return %s; }" % (json.dumps({"nombre": "Rafael", "tarifa": tarifa}) if tarifa else "null"),
            "function _o2mLimpiarNumero(v){ return String(v==null?'':v).replace(/\\D/g,''); }",
            "function esc(s){ return String(s==null?'':s); }",
            "function $o2mPesos(n){ return '$'+n; }",
            _js_funcion("_pintarFinCostoChipDom"),
            "_pintarFinCostoChipDom();",
            "console.log(JSON.stringify({html: el.innerHTML}));"])
        return _correr(prog)["html"]

    def test_con_tecnico_propio_ofrece_declarar_cero(self):
        h = self._chip()
        self.assertIn('onclick="OT2C.usarCostoCero()"', h)
        self.assertIn("Técnico propio: sin costo extra", h)
        self.assertIn("toca para declarar $0", h)
        self.assertNotIn("is-sel", h)

    def test_ya_declarado_en_cero_queda_marcado(self):
        self.assertIn("o2m-fchip is-sel", self._chip(costo="0"))

    def test_con_externo_o_interno_o_sin_tecnico_no_se_ofrece(self):
        self.assertEqual(self._chip(externo=True), "")
        self.assertEqual(self._chip(interno=True), "")
        self.assertEqual(self._chip(tecnicos=()), "")

    def test_con_la_tarifa_del_externo_sigue_el_chip_de_siempre(self):
        h = self._chip(externo=True, tarifa=45000)
        self.assertIn("OT2C.usarTarifaProveedor()", h)
        self.assertNotIn("usarCostoCero", h)

    def test_el_toque_escribe_cero_y_repinta(self):
        prog = "\n".join([
            "var S = {fin_costo_proveedor: ''};",
            "var inp = {value: ''}; var llamadas = [];",
            "var document = {getElementById: function(id){ return id === 'o2mFinCostoProv' ? inp : null; }};",
            "function _pintarFinCostoChipDom(){ llamadas.push('chip'); } function _pintarFinMargenDom(){ llamadas.push('margen'); }",
            "function _o2mFinEstadosDom(){ llamadas.push('estados'); } function _o2mAnexoSeguirCostos(){ llamadas.push('anexo'); }",
            "function refrescarAnclas(){ llamadas.push('anclas'); }",
            _js_funcion("usarCostoCero"),
            "usarCostoCero();",
            "console.log(JSON.stringify({costo: S.fin_costo_proveedor, input: inp.value, llamadas: llamadas}));"])
        r = _correr(prog)
        self.assertEqual((r["costo"], r["input"]), ("0", "0"))
        self.assertEqual(sorted(r["llamadas"]), sorted(["chip", "margen", "estados", "anexo", "anclas"]))

    def test_cambiar_de_tecnico_actualiza_el_chip(self):
        self.assertIn("_pintarFinCostoChipDom();", _js_funcion("tecTog"))

    def test_esta_expuesto_para_el_html(self):
        self.assertIn("usarCostoCero:usarCostoCero", H)


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestElValorDeReferenciaQuedaPlegado(unittest.TestCase):
    def _paso(self, plegable, abierto=False):
        prog = "\n".join([
            "var S = {_fpAbierto: %s};" % json.dumps({"valor": True} if abierto else {}),
            _js_funcion("_o2mFpasoHtml"),
            "console.log(JSON.stringify({html: _o2mFpasoHtml('valor', 3, 'Valor de referencia (opcional)', '<input id=\"o2mFinValorInput\">', 'pista', %s)}));"
            % json.dumps(plegable)])
        return _correr(prog)["html"]

    def test_plegado_esconde_el_cuerpo_pero_deja_los_campos_en_la_pagina(self):
        h = self._paso(True)
        self.assertIn("o2m-fpaso plegable plegado", h)
        self.assertIn('aria-expanded="false"', h)
        self.assertIn('id="o2mFinValorInput"', h, "los campos siguen en la página: nada se quita")
        self.assertIn('onclick="OT2C.fpasoTog(\'valor\')"', h)
        self.assertIn('role="button" tabindex="0"', h)

    def test_abierto_no_lleva_la_marca_de_plegado(self):
        h = self._paso(True, abierto=True)
        self.assertIn("o2m-fpaso plegable", h)
        self.assertNotIn("plegado", h)
        self.assertIn('aria-expanded="true"', h)

    def test_un_sub_paso_normal_no_cambia(self):
        h = self._paso(False)
        self.assertNotIn("plegable", h)
        self.assertNotIn("role=\"button\"", h)

    def test_el_toque_abre_y_cierra(self):
        prog = "\n".join([
            "var S = {_fpAbierto: {}};",
            "var cls = {s: {}}; var hd = {v: '', setAttribute: function(k, v){ this.v = v; }};",
            "var el = {classList: {toggle: function(c, on){ cls.s[c] = on; }}, querySelector: function(){ return hd; }};",
            "var document = {getElementById: function(){ return el; }};",
            _js_funcion("fpasoTog"),
            "fpasoTog('valor'); var a = {abierto: S._fpAbierto.valor, plegado: cls.s.plegado, aria: hd.v};",
            "fpasoTog('valor'); var b = {abierto: S._fpAbierto.valor, plegado: cls.s.plegado, aria: hd.v};",
            "console.log(JSON.stringify({a: a, b: b}));"])
        r = _correr(prog)
        self.assertEqual(r["a"], {"abierto": True, "plegado": False, "aria": "true"})
        self.assertEqual(r["b"], {"abierto": False, "plegado": True, "aria": "false"})

    def test_en_un_cero_el_valor_va_plegado_salvo_que_pida_algo(self):
        f = _js_funcion("pintarFinanzas")
        self.assertIn("'Valor de referencia (opcional)'", f)
        self.assertIn("!interno && !!S.fin_garantia && _o2mFinSubEstado('valor').cls !== 'falta')", f)

    def test_el_estado_inicial_trae_el_registro_de_abiertos(self):
        self.assertIn("_fpAbierto:{},", H)
        self.assertIn("fpasoTog:fpasoTog", H)


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestElCostoDelTecnicoPropioVieneSugerido(unittest.TestCase):
    """Con un técnico propio el costo es $0 y queda sugerido ya escrito (visible y editable): el caso más común no pide ni un toque."""

    def _auto(self, estado, externo=False, interno=False):
        prog = "\n".join([
            "var S = %s;" % json.dumps(estado),
            "function esInterno(){ return %s; }" % json.dumps(interno),
            "function _primerTecnicoExterno(){ return %s; }" % ("{id:21}" if externo else "null"),
            _js_funcion("_o2mFinCostoAuto"),
            "var cambio = _o2mFinCostoAuto();",
            "console.log(JSON.stringify({cambio: cambio, costo: S.fin_costo_proveedor, auto: S._finCostoAuto}));"])
        return _correr(prog)

    def test_propio_sin_valor_queda_en_cero_sugerido(self):
        r = self._auto({"tecnicos": [11], "fin_costo_proveedor": "", "_finCostoAuto": False})
        self.assertEqual((r["cambio"], r["costo"], r["auto"]), (True, "0", True))

    def test_lo_que_la_persona_escribio_nunca_se_pisa(self):
        r = self._auto({"tecnicos": [11], "fin_costo_proveedor": "35000", "_finCostoAuto": False})
        self.assertEqual((r["cambio"], r["costo"], r["auto"]), (False, "35000", False))
        r = self._auto({"tecnicos": [11], "fin_costo_proveedor": "0", "_finCostoAuto": False})
        self.assertEqual((r["costo"], r["auto"]), ("0", False), "un 0 escrito a mano sigue siendo de la persona")

    def test_al_pasar_a_un_externo_se_suelta_solo_lo_que_puso_el_sistema(self):
        r = self._auto({"tecnicos": [21], "fin_costo_proveedor": "0", "_finCostoAuto": True}, externo=True)
        self.assertEqual((r["cambio"], r["costo"], r["auto"]), (True, "", False))
        r = self._auto({"tecnicos": [21], "fin_costo_proveedor": "0", "_finCostoAuto": False}, externo=True)
        self.assertEqual((r["cambio"], r["costo"]), (False, "0"), "lo escrito a mano se respeta aunque el técnico sea externo")

    def test_con_externo_se_deja_vacio_para_que_lo_escriba(self):
        r = self._auto({"tecnicos": [21], "fin_costo_proveedor": "", "_finCostoAuto": False}, externo=True)
        self.assertEqual((r["cambio"], r["costo"], r["auto"]), (False, "", False))

    def test_sin_tecnico_o_en_trabajo_interno_no_hace_nada(self):
        self.assertFalse(self._auto({"tecnicos": [], "fin_costo_proveedor": "", "_finCostoAuto": False})["cambio"])
        self.assertFalse(self._auto({"tecnicos": [11], "fin_costo_proveedor": "", "_finCostoAuto": False}, interno=True)["cambio"])

    def test_escribir_el_campo_apaga_la_sugerencia(self):
        self.assertIn("S._finCostoAuto = false; _o2mInputDineroCambio(el, 'fin_costo_proveedor');", H)

    def test_se_aplica_al_pintar_y_al_cambiar_de_tecnico(self):
        self.assertIn("_o2mFinCostoAuto();", _js_funcion("pintarFinanzas"))
        tec = _js_funcion("tecTog")
        self.assertIn("if (_o2mFinCostoAuto()){", tec)
        self.assertIn("_pintarFinCostoChipDom();", tec)

    def test_el_chip_dice_que_es_sugerido(self):
        f = _js_funcion("_pintarFinCostoChipDom")
        self.assertIn("sugerido · puedes escribir otro valor", f)


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestMenosCamposQueRepitenLaPlata(unittest.TestCase):
    """«Dan pereza rellenar valores duplicados o similares»: lo opcional queda detrás de un enlace; el anexo no vuelve a pedir los montos."""

    def test_despacho_solo_se_ve_si_hace_falta(self):
        def vis(estado):
            prog = "\n".join(["var S = %s;" % json.dumps(dict({"_fpAbierto": {}, "fin_costo_despacho": "", "fin_zz_envio": None}, **estado)),
                              _js_funcion("_o2mFinDespachoVisible"),
                              "console.log(JSON.stringify(_o2mFinDespachoVisible()));"])
            return _correr(prog)
        self.assertFalse(vis({}))
        self.assertTrue(vis({"fin_costo_despacho": "20000"}), "si ya trae un valor se ve")
        self.assertTrue(vis({"fin_zz_envio": {"monto": 50000}}), "si al cliente se le cobra despacho se ve")
        self.assertFalse(vis({"fin_zz_envio": {"monto": 0}}))
        self.assertTrue(vis({"_fpAbierto": {"despacho": True}}), "o si la persona lo pidió")

    def test_el_costo_del_despacho_sigue_en_la_pagina_con_su_enlace(self):
        f = _js_funcion("pintarFinanzas")
        self.assertIn("id=\"o2mFinDespWrap\"'+(_o2mFinDespachoVisible() ? '' : ' hidden')", f)
        self.assertIn('id="o2mFinCostoDesp"', f, "el campo no se quitó: solo se esconde")
        self.assertIn("OT2C.finDespachoAbrir()", f)
        self.assertIn("if (me.falta_despacho) _o2mFinDespachoMostrar(false);", _js_funcion("_pintarFinMargenDom"))

    def test_valorizar_queda_detras_de_un_enlace_salvo_que_ya_tenga_valor(self):
        def html(valor="", abierto=False):
            prog = "\n".join(["var S = %s;" % json.dumps({"fin_valorizado": valor, "_fpAbierto": {"valorizar": True} if abierto else {}}),
                              "function esc(s){ return String(s==null?'':s); } function _o2mFormatearMiles(v){ return String(v==null?'':v); }",
                              _js_funcion("_o2mFinValorizarHtml"),
                              "console.log(JSON.stringify({h: _o2mFinValorizarHtml()}));"])
            return _correr(prog)["h"]
        h = html()
        self.assertIn('id="o2mFinValorizarBtn" onclick="OT2C.finValorizarAbrir()">', h)
        self.assertIn('<div id="o2mFinValorizarBox" hidden>', h)
        self.assertIn('id="o2mFinValorizadoInput"', h, "el campo sigue en la página")
        con_valor = html("109817")
        self.assertIn('id="o2mFinValorizarBtn" onclick="OT2C.finValorizarAbrir()" hidden>', con_valor)
        self.assertIn('<div id="o2mFinValorizarBox">', con_valor)
        self.assertIn('<div id="o2mFinValorizarBox">', html(abierto=True))

    def _anexo(self, items, editados=False, abierto=False, garantia=False):
        prog = "\n".join([
            "var S = %s;" % json.dumps({"anexo": {"items": items, "items_editados": editados}, "_fpAbierto": {"anexoItems": True} if abierto else {}, "fin_garantia": garantia}),
            "function esc(s){ return String(s==null?'':s); } function _o2mFormatearMiles(v){ return String(v==null?'':v); }",
            "function $o2mPesos(n){ return '$'+Math.round(n).toLocaleString('es-CL'); }",
            _js_funcion("_o2mAnexoTotal"), _js_funcion("_o2mAnexoItemsHtml"),
            "console.log(JSON.stringify({h: _o2mAnexoItemsHtml()}));"])
        return _correr(prog)["h"]

    def test_el_anexo_no_vuelve_a_pedir_los_montos_de_costos(self):
        h = self._anexo([{"concepto": "Instalación", "monto": 1100000}, {"concepto": "Despacho", "monto": 650000}])
        self.assertIn("o2m-anx-res", h)
        self.assertIn("Instalación <b>$1.100.000</b>", h)
        self.assertIn("Despacho <b>$650.000</b>", h)
        self.assertIn('<b id="o2mAnexoTotal">$1.750.000</b>', h)
        self.assertIn("Vienen de Costos: no se vuelven a escribir.", h)
        self.assertIn("OT2C.anexoPartidasAbrir()", h)
        self.assertNotIn('class="o2m-anx-row"', h, "ninguna fila editable mientras sigan siendo las de Costos")

    def test_si_se_editan_o_se_abre_la_tabla_aparece_la_de_siempre(self):
        items = [{"concepto": "Instalación", "monto": 1100000}]
        self.assertIn('class="o2m-anx-row"', self._anexo(items, editados=True))
        self.assertIn('class="o2m-anx-row"', self._anexo(items, abierto=True))
        self.assertIn("OT2C.anexoAgregarItem()", self._anexo(items, abierto=True))

    def test_sin_partidas_se_muestra_la_tabla_para_poder_agregar(self):
        h = self._anexo([])
        self.assertIn("Sin conceptos todavía", h)
        self.assertIn("OT2C.anexoAgregarItem()", h)

    def test_estan_expuestos_para_el_html(self):
        for n in ("finDespachoAbrir", "finValorizarAbrir", "anexoPartidasAbrir"):
            self.assertIn("%s:%s" % (n, n), H, n)


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestElChecklistDeLaSalidaSeEligeEnEquipos(unittest.TestCase):
    """Daniel: "cuando escojo el checklist de instalación y el de desinstalación, en el paso 7… no me ofrece la opción de escoger"."""

    PLANTS = [{"id": 301, "nombre": "Desinstalación — Selectorizados"}, {"id": 302, "nombre": "Desinstalación — Poleas"}]

    def _salida(self, desinst=None, hija=True, plants=None, cargando=False, sug=None, mid=1001):
        d = {"eq_fuera": {}, "eq_plant": {}, "previo": ({"checklists_sugeridos": sug} if sug else None)}
        d.update(desinst or {})
        prog = "\n".join([
            "var S = %s;" % json.dumps({"desinst": d}),
            "var _evPlantillas = %s, _evPlantCargando = %s;" % (json.dumps(plants), json.dumps(cargando)),
            "function _o2mDiTieneHija(){ return %s; }" % json.dumps(hija),
            "function esc(s){ return String(s==null?'':s); }",
            "function _diEqNombre(m){ return 'Equipo '+m; }",
            _js_funcion("_diSug"), _js_funcion("_diPlantEleg"), _js_funcion("_o2mEqSalidaHtml"),
            "console.log(JSON.stringify({h: _o2mEqSalidaHtml(%d)}));" % mid])
        return _correr(prog)["h"]

    def test_muestra_el_checklist_sugerido_elegido_y_la_lista_para_cambiarlo(self):
        h = self._salida(plants=self.PLANTS, sug=[{"maquina_id": 1001, "plantilla_id": 302, "plantilla_nombre": "Desinstalación — Poleas"}])
        self.assertIn('class="o2m-eq-sal ok"', h)
        self.assertIn('<option value="302" selected>Desinstalación — Poleas</option>', h)
        self.assertIn('<option value="301">Desinstalación — Selectorizados</option>', h)
        self.assertIn('onchange="OT2C.diEqPlant(1001, this.value)"', h)
        self.assertIn("Al desinstalar", h)
        self.assertIn('OT2C.diEqTog(1001)', h)

    def test_lo_que_la_persona_elige_manda_sobre_la_sugerencia(self):
        h = self._salida(desinst={"eq_plant": {"1001": 301}}, plants=self.PLANTS,
                         sug=[{"maquina_id": 1001, "plantilla_id": 302, "plantilla_nombre": "Desinstalación — Poleas"}])
        self.assertIn('<option value="301" selected>', h)

    def test_sin_checklist_elegido_lo_pide_en_ambar(self):
        h = self._salida(plants=self.PLANTS)
        self.assertIn('class="o2m-eq-sal falta"', h)
        self.assertIn("Elige el checklist de la salida…", h)

    def test_un_equipo_que_no_se_retira_lo_dice_y_se_puede_incluir(self):
        h = self._salida(desinst={"eq_fuera": {"1001": True}}, plants=self.PLANTS)
        self.assertIn("No se retira", h)
        self.assertIn('OT2C.diEqTog(1001)">Incluir<', h)
        self.assertNotIn("<select", h)

    def test_sin_desinstalacion_no_agrega_nada_a_la_fila(self):
        self.assertEqual(self._salida(hija=False, plants=self.PLANTS), "")

    def test_mientras_carga_y_si_no_hay_ninguno_lo_dice(self):
        self.assertIn("Cargando checklists…", self._salida(plants=[], cargando=True))
        self.assertIn("Cargando checklists…", self._salida(plants=None))
        self.assertIn("Sin checklist configurado para desinstalaciones", self._salida(plants=[]))

    def _resumen(self, conDesinst, equipos, desinst):
        prog = "\n".join([
            "var S = %s;" % json.dumps({"conDesinst": conDesinst, "equipos": equipos, "desinst": dict({"lleva": "si", "eq_fuera": {}, "eq_plant": {}, "previo": None}, **desinst)}),
            "var _evPlantillas = [];",
            "var el = {innerHTML: ''};",
            "function esc(s){ return String(s==null?'':s); } function _diEsc(s){ return String(s==null?'':s); }",
            "function _diEqNombre(m){ return 'Equipo '+m; } function _diEqInfo(m){ return null; }",
            _js_funcion("_diSug"), _js_funcion("_diPlantEleg"), _js_funcion("_diEqIncluidos"), _js_funcion("_diRegEq"),
            "_diRegEq(el); console.log(JSON.stringify({h: el.innerHTML}));"])
        return _correr(prog)["h"]

    def test_el_paso_desinstalacion_solo_resume_y_manda_a_equipos(self):
        h = self._resumen(True, [{"maquina_id": 1}, {"maquina_id": 2}], {"eq_plant": {"1": 301}})
        self.assertIn("o2m-di-sub falta", h)
        self.assertIn("Falta el checklist de la salida de <b>Equipo 2</b>", h)
        self.assertIn("OT2C.irAncla('equipos')", h)
        self.assertNotIn("<select", h, "el selector ya no se repite acá")
        ok = self._resumen(True, [{"maquina_id": 1}, {"maquina_id": 2}], {"eq_plant": {"1": 301, "2": 302}})
        self.assertIn("o2m-di-sub ok", ok)
        self.assertIn("2 de 2 equipos con su checklist de desinstalación", ok)

    def test_ningun_equipo_marcado_para_retirar_lo_avisa(self):
        h = self._resumen(True, [{"maquina_id": 1}], {"eq_fuera": {"1": True}})
        self.assertIn("Ningún equipo se retira", h)

    def test_la_desinstalacion_clasica_conserva_su_selector(self):
        self.assertIn("if (S.conDesinst){", _js_funcion("_diRegEq"))
        self.assertIn("OT2C.diEqPlant(", _js_funcion("_diRegEq"), "fuera de «Instalación con desinstalación» el selector sigue en su paso")

    def test_cableado(self):
        self.assertIn("_o2mEqSalidaRefrescar();", _js_funcion("_diPintarRegiones"))
        self.assertIn("_evPlantCargando = true;", _js_funcion("_diCargarPlantillas"))
        self.assertIn("if (_o2mDiAplica()) _diCargarPlantillas();", _js_funcion("pintarEquipos"))
        self.assertIn("_o2mEqSalidaHtml(m.id)", _js_funcion("_o2mFilaEquipoHTML"))
        self.assertIn("(_o2mDiTieneHija() ? 'Checklist de entrada y de salida' : 'Plantillas extra')", _js_funcion("pintarListaEquipos"))
        self.assertIn("OT2C.diEqTodo()", _js_funcion("pintarListaEquipos"))


@unittest.skipUnless(shutil.which("node"), "sin Node")
class TestElAvisoDeCorreosOcupaUnaLinea(unittest.TestCase):
    """Daniel: "aparece como un menú dando vueltas, no sé qué es": el aviso del pie queda en una línea y la lista se abre con un toque."""

    ITEMS = [{"nombre": "Milagros Salazar", "rol_txt": "Cliente", "correo": "m@x.cl", "saldra": False, "motivo": "El envío de correos de Mantenciones está apagado: no saldrá."},
             {"nombre": "Daniel Aguilar", "rol_txt": "Técnico", "correo": "d@x.cl", "saldra": True, "nota": "recibe el correo con el archivo de calendario"}]

    def _pintar(self, items, abierto=False):
        prog = "\n".join([
            "var _o2mAvisoAbierto = %s;" % json.dumps(abierto),
            "var box = {hidden: true, className: '', innerHTML: ''};",
            "function esc(s){ return String(s==null?'':s); }",
            _js_funcion("_o2mAvisoEnvioPintar"),
            "_o2mAvisoEnvioPintar(box, %s); console.log(JSON.stringify({h: box.innerHTML, hidden: box.hidden, cls: box.className}));" % json.dumps({"ok": True, "items": items})])
        return _correr(prog)

    def test_una_linea_con_el_resumen_y_la_lista_cerrada(self):
        r = self._pintar(self.ITEMS)
        self.assertIn("Al crear esta OT se enviará:", r["h"])
        self.assertIn("técnico · 1 correo · 1 no saldrá", r["h"])
        self.assertIn('aria-expanded="false"', r["h"])
        self.assertIn("Ver a quién", r["h"])
        self.assertIn("<ul hidden>", r["h"])
        self.assertIn("El envío de correos de Mantenciones está apagado", r["h"], "la lista completa sigue en la página")
        self.assertEqual(r["cls"], "o2m-aviso-envio alerta")

    def test_abierto_muestra_la_lista(self):
        r = self._pintar(self.ITEMS, abierto=True)
        self.assertIn('aria-expanded="true"', r["h"])
        self.assertIn("Ocultar", r["h"])
        self.assertNotIn("<ul hidden>", r["h"])

    def test_sin_destinatarios_no_aparece(self):
        self.assertTrue(self._pintar([])["hidden"])

    def test_el_toque_abre_y_cierra(self):
        prog = "\n".join([
            "var _o2mAvisoAbierto = false;",
            "var ul = {hidden: true}; var ver = {innerHTML: ''}; var b = {attrs: {}, setAttribute: function(k, v){ this.attrs[k] = v; }, querySelector: function(){ return ver; }};",
            "var box = {querySelector: function(s){ return s === 'ul' ? ul : b; }};",
            "var document = {getElementById: function(){ return box; }};",
            _js_funcion("avisoEnvioTog"),
            "avisoEnvioTog(); var a = {ul: ul.hidden, aria: b.attrs['aria-expanded'], ver: ver.innerHTML};",
            "avisoEnvioTog(); var c = {ul: ul.hidden, aria: b.attrs['aria-expanded']};",
            "console.log(JSON.stringify({a: a, c: c}));"])
        r = _correr(prog)
        self.assertEqual((r["a"]["ul"], r["a"]["aria"]), (False, "true"))
        self.assertIn("Ocultar", r["a"]["ver"])
        self.assertEqual((r["c"]["ul"], r["c"]["aria"]), (True, "false"))

    def test_cableado_y_44px(self):
        self.assertIn("avisoEnvioTog:avisoEnvioTog", H)
        self.assertIn(".o2m-avenvio-t{display:flex;align-items:center;gap:8px;width:100%;min-height:44px", H)
        self.assertIn(".o2m-aviso-envio ul[hidden]{display:none}", H)


class TestAmbarYVerde(unittest.TestCase):
    def test_lo_que_falta_en_costos_es_ambar_no_rojo(self):
        m = re.search(r"\.o2m-fpaso\.falta\{border-color:([^}]+)\}\n\.o2m-fpaso\.falta \.hd\{background:([^;]+);color:([^}]+)\}\n"
                      r"\.o2m-fpaso\.falta \.hd \.est\{color:([^}]+)\}", H)
        self.assertIsNotNone(m)
        self.assertEqual(m.groups(), ("#fde68a", "#fffbeb", "#92400e", "#b45309"))
        self.assertNotIn("#fecaca", H[H.index(".o2m-fpaso.falta{"):H.index(".o2m-fpaso.opcional")])

    def test_el_sub_paso_plegable_mide_44px_en_el_encabezado(self):
        self.assertIn(".o2m-fpaso.plegable .hd{cursor:pointer;min-height:44px}", H)
        self.assertIn(".o2m-fpaso.plegado .bd{display:none}", H)

    def test_tecnicos_y_responsable_elegidos_quedan_en_verde(self):
        for nombre in ("_o2mPintarTecGrupos", "pintarResponsable"):
            f = _js_funcion(nombre)
            self.assertIn("' sel-ok'", f, nombre)
            self.assertNotIn("(sel ? ' sel' :", f, nombre)

    def test_el_verde_del_requisito_resuelto_ya_existia(self):
        self.assertIn(".o2m-op.sel-ok{border-color:#16a34a;background:#f0fdf4;", H)


if __name__ == "__main__":
    unittest.main()
