"""El paso de valor del asistente de crear OT (templates/ot2/_modal_crear.html, namespace OT2C) es PLANO -- 08-oct-2026.

Daniel: «limpia el paso de valor del asistente». A lo largo de varios días el asistente siguió poniendo montos que nadie
escribió: en la OT 281 la persona escribió $0, el campo se vació y se envió un estimado de $10.034; en la OT 282 el candado
">0" de las OT internas obligó a escribir $1; una OT de $20.068 era un estimado guardado como cobro.

La regla ÚNICA que fijan estas pruebas:
  1. Lo que se COBRA sale de DOS fuentes y de ninguna otra: (a) la línea de servicio del documento de Random que la persona
     ELIGE (clic en el chip o en la línea) o (b) un monto que la persona ESCRIBE, con motivo. El sistema nunca pone un monto
     de cobro por su cuenta: sin auto-selección por prioridad, sin respaldos con cotización o estimado, sin count-up. El 0
     escrito es 0.
  2. Las referencias (cotización, contrato, estimado, total del documento) se MUESTRAN como sugerencias con su botón
     «Usar este monto»; solo entran al cobro si la persona lo aprieta. Un estimado aceptado en una OT que se cobra pide
     motivo, igual que lo escrito a mano.
  3. El VALORIZADO es aparte y opcional; nunca se guarda como cobro.
  4. OT interna: no hay cobro; vacío o $0 pasan. Garantía/regalía/arriendo: cobro $0 con motivo y autorización (REGLA #24).
  5. La vista previa del margen usa EXACTAMENTE lo que se va a enviar.

Sin BD ni Flask: texto/lexer sobre la plantilla, node para ejecutar las funciones puras (se salta si no hay node) y las
funciones del servidor extraídas de app.py con ast (mismo arnés que tests/test_ot_finanzas_creacion.py).
Correr con:  py -m unittest tests.test_asistente_valor_plano
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


M = _leer("templates/ot2/_modal_crear.html")


# ── Mini analizador de JS: lo justo para saber en QUÉ función ocurre cada cosa ────────────────────────────────────────
def _enmascarar(src):
    """Copia de `src` con el contenido de comentarios, strings y regex en blanco (mismas posiciones y saltos de línea):
    sobre ella se puede buscar código real sin que un texto de HTML o un comentario cuente como llamada o asignación."""
    out = list(src)
    n, i, prev = len(src), 0, ""

    def blanco(a, b):
        for k in range(a, b):
            if out[k] != "\n":
                out[k] = " "

    while i < n:
        c = src[i]
        d = src[i + 1] if i + 1 < n else ""
        if c == "/" and d == "/":
            j = src.find("\n", i)
            j = n if j < 0 else j
            blanco(i, j)
            i = j
            continue
        if c == "/" and d == "*":
            j = src.index("*/", i + 2) + 2
            blanco(i, j)
            i = j
            continue
        if c in "'\"`":
            j = i + 1
            while j < n and src[j] != c:
                if src[j] == "\\":
                    j += 1
                j += 1
            blanco(i + 1, j)
            i, prev = j + 1, c
            continue
        if c == "/" and (prev == "" or prev in "(,=:[!&|?{};+-*%<>~^" or re.search(r"\b(return|typeof)\s*$", src[max(0, i - 8):i])):
            j, en_clase = i + 1, False
            while j < n:
                ch = src[j]
                if ch == "\\":
                    j += 2
                    continue
                if ch == "[":
                    en_clase = True
                elif ch == "]":
                    en_clase = False
                elif ch == "/" and not en_clase:
                    break
                j += 1
            blanco(i + 1, j)
            i, prev = j + 1, "/"
            continue
        if not c.isspace():
            prev = c
        i += 1
    return "".join(out)


# Solo el <script> inline: el HTML y el CSS de la plantilla traen apostrofes y comillas que desordenarían el lexer.
_I0 = M.index("<script>\n")
_I1 = M.index("</script>", _I0)
MM = " " * _I0 + _enmascarar(M[_I0:_I1]) + " " * (len(M) - _I1)
assert len(MM) == len(M)


def _cierre(masked, i_abre, abre, cierra):
    depth = 0
    for k in range(i_abre, len(masked)):
        if masked[k] == abre:
            depth += 1
        elif masked[k] == cierra:
            depth -= 1
            if depth == 0:
                return k
    raise AssertionError("llave sin cerrar")


def _funciones():
    """[(nombre, ini_declaracion, fin_cuerpo)] de TODA función con nombre del script."""
    res = []
    for m in re.finditer(r"\bfunction\s+([A-Za-z_$][\w$]*)\s*\(", MM):
        i_par = m.end() - 1
        f_par = _cierre(MM, i_par, "(", ")")
        i_llave = MM.index("{", f_par)
        res.append((m.group(1), m.start(), _cierre(MM, i_llave, "{", "}")))
    return res


FUNCS = _funciones()


def _fn(nombre):
    """Texto completo de `function nombre(...) {...}` (debe existir una sola vez)."""
    c = [f for f in FUNCS if f[0] == nombre]
    assert len(c) == 1, (nombre, len(c))
    _, ini, fin = c[0]
    return M[ini:fin + 1]


def _enclosing(pos):
    """Función con nombre más interna que contiene la posición `pos`."""
    c = [f for f in FUNCS if f[1] <= pos <= f[2]]
    return max(c, key=lambda f: f[1])[0] if c else None


def _quien(patron, excluir_def=None):
    """Conjunto de funciones donde el código real (sin strings ni comentarios) coincide con `patron`."""
    res = set()
    for m in re.finditer(patron, MM):
        f = _enclosing(m.start())
        if f != excluir_def:
            res.add(f)
    return res


class TestUnaSolaRegla(unittest.TestCase):
    """Texto/lexer sobre la plantilla."""

    def test_el_lexer_entiende_la_plantilla(self):
        for nombre in ("crear", "setFinZZ", "usarFuenteValor", "_o2mFinMontoServicio", "_pintarFinValorDom", "completo"):
            self.assertTrue(_fn(nombre).startswith("function " + nombre))
        self.assertTrue(_fn("crear").rstrip().endswith("}"))

    # ── 1. Nadie más que la persona escribe S.fin_zz_monto ────────────────────────────────────────────────────────────
    def test_fin_zz_monto_solo_lo_escribe_la_persona(self):
        """Toda asignación de un valor (no null) a S.fin_zz_monto ocurre en una función que es un gesto de la persona:
        teclear (_o2mFinMontoServicioCambio), elegir una línea (setFinZZ), el total del documento
        (usarTotalDocumentoComoServicio) o el botón «Usar este monto» (usarFuenteValor). Las demás solo lo vacían."""
        permitidas = {"_o2mFinMontoServicioCambio", "setFinZZ", "usarTotalDocumentoComoServicio", "usarFuenteValor"}
        con_valor = _quien(r"S\.fin_zz_monto\s*(?:[-+*/]=|=(?!=)(?!\s*null\b))")
        self.assertTrue(con_valor, "el patrón no encontró nada: el lexer o la regex se rompieron")
        self.assertLessEqual(con_valor, permitidas, "algo rellena el cobro por su cuenta: " + str(con_valor - permitidas))
        # y los cuatro gestos sí lo escriben (si no, la prueba no vigila nada)
        self.assertEqual(con_valor, permitidas)

    def test_el_transporte_del_documento_tampoco_entra_solo_al_cobro(self):
        """El transporte (ZZENVIO) se reconoce y se muestra como referencia; su monto solo entra al cobro del despacho
        si la persona lo escribe (_o2mFinMontoEnvioCambio) o aprieta «Usar este monto» (usarEnvioDocumento); «Tomar solo
        el saldo» (saldoAccion) solo BAJA un monto que la persona ya había puesto."""
        con_valor = _quien(r"S\.fin_zz_envio\.monto\s*(?:[-+*/]=|=(?!=)(?!\s*null\b))")
        self.assertEqual(con_valor, {"_o2mFinMontoEnvioCambio", "usarEnvioDocumento", "saldoAccion"})
        c = _enmascarar(_fn("_o2mFinCargarZZ"))
        self.assertNotRegex(c, r"monto:\s*envioLn\.monto", "las líneas cargadas no escriben el despacho")
        self.assertEqual(_quien(r"\busarEnvioDocumento\s*\(", "usarEnvioDocumento"), set(), "solo se llega por su botón")
        self.assertIn("OT2C.usarEnvioDocumento()", _fn("_o2mFinEnvioSugHtml"))
        self.assertIn("usarEnvioDocumento:usarEnvioDocumento", M)

    def test_quien_llama_a_los_gestos(self):
        """setFinZZ / usarFuenteValor / usarTotalDocumentoComoServicio solo se invocan desde un botón (onclick, que
        vive en strings) o desde la función de otro gesto de la persona."""
        self.assertEqual(_quien(r"\bsetFinZZ\s*\(", "setFinZZ"), {"usarFuenteValor", "saldoAccion"})   # «Tomar solo el saldo»
        self.assertEqual(_quien(r"\busarFuenteValor\s*\(", "usarFuenteValor"), {"_o2mFinCotRevisarAlVolver"})  # «Sí, usarla»
        self.assertEqual(_quien(r"\busarTotalDocumentoComoServicio\s*\(", "usarTotalDocumentoComoServicio"), {"usarFuenteValor"})
        # el botón «Usar este monto» del chip existe y llama a usarFuenteValor
        self.assertIn("OT2C.usarFuenteValor(", M)
        self.assertIn("Usar este monto", _fn("_pintarFinValorDom"))
        self.assertIn("OT2C.setFinZZ(", _fn("_pintarFinZZDom"))

    # ── 2. Sin auto-selección, sin prioridad, sin respaldos ──────────────────────────────────────────────────────────
    def test_no_hay_auto_seleccion(self):
        cuerpo = _fn("_o2mFinAutoSeleccionar")
        self.assertEqual(cuerpo.replace(" ", "").replace("\n", ""), "function_o2mFinAutoSeleccionar(){}",
                         "debe quedar como NO-OP documentado")
        self.assertEqual(_quien(r"\b_o2mFinAutoSeleccionar\s*\(", "_o2mFinAutoSeleccionar"), set(), "nadie la llama")
        codigo = MM
        for muerto in ("_o2mFinMejorFuente", "_O2M_FUENTE_PRIORIDAD", "_o2mFinAplicarFuente", "_o2mFinMarcarOfrecidas",
                       "_o2mFinToastOfrecido", "fin_valor_tocado"):
            self.assertNotIn(muerto, codigo, muerto + " era parte de la auto-selección y ya no existe")

    def test_ninguna_carga_de_referencias_escribe_el_cobro(self):
        """Contrato, estimado y líneas del documento llegan por fetch: solo pintan, nunca escriben el monto."""
        for nombre in ("_o2mFinCargarContrato", "_o2mFinEstimar", "_o2mFinCargarZZ", "_o2mFinCargarSaldo"):
            c = re.sub(r"[ \t]+", " ", _enmascarar(_fn(nombre)))
            self.assertNotRegex(c, r"S\.fin_zz_monto\s*=(?!=)(?!\s*null\b)", nombre)
            self.assertNotRegex(c, r"S\.fin_zz_codigo\s*=(?!=)(?!\s*null\b)", nombre)
            self.assertNotIn("setFinZZ(", c)
            self.assertNotIn("usarFuenteValor(", c)

    def test_cargar_zz_deja_las_lineas_a_la_vista_sin_elegir(self):
        c = _fn("_o2mFinCargarZZ")
        self.assertNotIn("candidatas", c, "ya no se elige la línea sugerida")
        self.assertIn("nada se elige solo", c)
        # el aviso de RUT sigue funcionando sin haber elegido la línea
        self.assertIn("S.fin_zz_lineas.some(function(l){ return l !== envioLn && l.sugerida; })", c)

    def test_crear_y_servicio_sin_respaldos(self):
        for nombre in ("crear", "_o2mFinMontoServicio", "_o2mFinMontosEnvio", "_ventaActual", "_o2mFinVistaPrevia"):
            c = _enmascarar(_fn(nombre))
            for prohibido in ("fin_cotizacion", "fin_costo_estimado", "total_neto"):
                self.assertNotIn(prohibido, c, f"{nombre} no puede caer a {prohibido}")
        self.assertIn("Object.assign(body.finanzas, _o2mFinMontosEnvio());", M)
        self.assertIn("if (S.fin_zz_monto == null) return null;", _fn("_o2mFinMontoServicio"))
        # los textos de respaldo viejos ya no se mandan al servidor
        self.assertNotIn("Estimado automático por clasificación del producto", M)
        self.assertNotIn("Valorizado con cotización", M)

    def test_el_input_no_cuenta_numeros_ni_se_vacia_solo(self):
        c = _fn("_pintarFinValorDom")
        self.assertNotIn("_o2mCountUp", _enmascarar(c), "el cobro ya no se anima (count-up)")
        self.assertIn("if (inp.value !== _txt) inp.value = _txt;", c)
        self.assertEqual(_quien(r"\b_o2mCountUp\s*\(", "_o2mCountUp"), {"usarCostoInternoAuto"},
                         "el count-up solo queda en el chip de horas × tarifa del trabajo interno (lo aprieta la persona)")
        self.assertIn("String(v == null ? '' : v).replace(/[^0-9]/g,'')", _fn("_o2mLimpiarNumero"))   # el 0 se conserva

    def test_cotizacion_elegida_es_sugerencia_hasta_que_se_usa(self):
        self.assertNotIn("usarFuenteValor(", _enmascarar(_fn("_o2mFinCotizacionSeleccionada")))

    # ── 3. Interna: sin cobro, vacío o $0 pasan ──────────────────────────────────────────────────────────────────────
    def test_interna_sin_valor_pasa_y_su_rotulo_es_opcional(self):
        self.assertIn("if (esInterno()) return true;", M)
        self.assertIn("Esta OT vale '+", M)
        self.assertNotIn("Esta OT vale *", M)
        self.assertIn("esInterno() && String(S.costo_interno == null ? '' : S.costo_interno).trim() !== ''", M)

    def test_el_estimado_en_una_ot_que_se_cobra_pide_motivo(self):
        self.assertIn("if (_o2mFinPideMotivo() && !(S.fin_zz_motivo_manual||'').trim()) return false;", M)
        self.assertIn("function _o2mFinPideMotivo(){ return _o2mFinEsManual() || _o2mFinEstimadoComoCobro(); }", M)


# ── Ejecución real de las funciones puras (node) ────────────────────────────────────────────────────────────────────
_FUNCIONES_NODE = (
    "_o2mLimpiarNumero", "_o2mFormatearMiles", "_o2mFinNumCampo", "_o2mFinEsManual", "_o2mFinEnvioEsManual",
    "_o2mFinEstimadoComoCobro", "_o2mFinPideMotivo", "_o2mFinMontoServicio", "_o2mFinValorizado", "_o2mFinMontosEnvio",
    "_o2mFinVistaPrevia", "_o2mFinMontoServicioCambio", "_o2mFinHayReferencia", "usarFuenteValor", "setFinZZ",
    "usarTotalDocumentoComoServicio", "_o2mFinFuentes", "_o2mFinDocsTodos", "_o2mDocOrigenNorm", "_o2mFinDocTxt",
    "_o2mFinLineaZZActual", "_o2mFinContratoAplica", "_o2mFinContratoEsReal", "_o2mRutCuerpo",
    "usarEnvioDocumento", "_o2mFinEnvioSugHtml",
)

_BASE = {
    "tipo": "preventiva", "cliente": {"id": 7, "rut": "76.111.222-3"}, "equipos": [{"maquina_id": 1}],
    "fin_garantia": False, "fin_doc": None, "doc_origen": None, "doc_extra": [], "fin_zz_cargando": False,
    "fin_zz_lineas": [], "fin_zz_codigo": None, "fin_zz_monto": None, "fin_zz_monto_original": None,
    "fin_zz_motivo_manual": "", "fin_zz_envio": None, "fin_zz_envio_original": None, "fin_zz_envio_motivo_manual": "",
    "fin_valor_origen": None, "fin_valor_origen_base": None, "fin_valorizado": "", "fin_costo_proveedor": "",
    "fin_costo_despacho": "", "fin_cotizacion": None, "fin_costo_estimado": None, "fin_contrato": None,
    "fin_contrato_cargando": False, "fin_estimado_cargando": False, "fin_doc_total_sugerido": 0, "fin_zz_docs_n": 1,
    "fin_doc_rut_difiere": False,
}
# Todas las referencias a la vista a la vez (el caso de la OT 281): línea ZZ del documento, cotización, contrato y estimado.
_TODAS = dict(_BASE, fin_doc={"tido": "FCV", "nudo": "11439", "rut": "76.111.222-3"},
              fin_zz_lineas=[{"sku": "ZZMANTENCION", "descripcion": "Mantención", "monto": 100000, "sugerida": True, "origenes": []}],
              fin_cotizacion={"id": 3, "numero": "COT-3", "total": 55000},
              fin_contrato={"cid": 7, "neto": 70000, "origen_precio": "contrato"},
              fin_costo_estimado={"total_neto": 10034, "n_equipos_sin_clasificar": 0})
_SIN_REFERENCIAS = dict(_BASE)


_VAR_VALORIZADO = re.search(r"var _O2M_VALORIZADO_FUENTE = \{[^}]*\};", M).group(0)


def _correr_node(escenarios):
    """escenarios: [(S inicial, [acciones], opciones)] -> [resultado]. Acciones: ["escribir", "0"] | ["usar", "estimado"]
    | ["linea", sku, monto]. opciones: {"interno": bool}."""
    funcs = _VAR_VALORIZADO + "\n" + "\n".join(_fn(n) for n in _FUNCIONES_NODE)
    js = r"""
const FUNCS = %s;
const ESCENARIOS = %s;
function correr(esc){
  const f = new Function('S0', 'OPC', `
    var S = S0;
    var window = { ilusOtFinanzas: function(a){ window._ult = a; return {}; } };
    function esInterno(){ return !!OPC.interno; }
    function _liderEsExterno(){ return false; }
    function _pintarFinZZDom(){} function _pintarFinValorDom(){} function _pintarFinGarResumenDom(){}
    function _pintarFinMargenDom(){} function _o2mFinEstadosDom(){} function refrescarAnclas(){}
    function $o2mPesos(n){ return '$' + n; }
    ` + FUNCS + `
    return {
      S: S,
      escribir: function(txt){
        _o2mFinMontoServicioCambio({value: txt, selectionStart: txt.length, setSelectionRange: function(){}});
      },
      usar: usarFuenteValor,
      linea: setFinZZ,
      usarEnvio: usarEnvioDocumento,
      sug: _o2mFinEnvioSugHtml,
      envio: _o2mFinMontosEnvio,
      pideMotivo: _o2mFinPideMotivo,
      fuentes: _o2mFinFuentes,
      previa: function(){ _o2mFinVistaPrevia(); return window._ult; },
    };`);
  const api = f(JSON.parse(JSON.stringify(esc[0])), esc[2] || {});
  const antes = {monto: api.S.fin_zz_monto, envio: api.envio()};
  esc[1].forEach(function(a){
    if (a[0] === 'escribir') api.escribir(a[1]);
    else if (a[0] === 'usar') api.usar(a[1]);
    else if (a[0] === 'linea') api.linea(a[1], a[2]);
    else if (a[0] === 'envio_doc') api.usarEnvio();
  });
  return {antes: antes, monto: api.S.fin_zz_monto, origen: api.S.fin_valor_origen, envio: api.envio(),
          pideMotivo: api.pideMotivo(),
          fuentes: api.fuentes().map(function(x){ return [x.src, x.estado, x.monto === undefined ? null : x.monto]; }),
          sug: api.sug(),
          previa: api.previa()};
}
process.stdout.write(JSON.stringify(ESCENARIOS.map(correr)));
""" % (json.dumps(funcs), json.dumps(escenarios))
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as fh:
        fh.write(js)
        ruta = fh.name
    try:
        r = subprocess.run(["node", ruta], capture_output=True, text=True, encoding="utf-8", timeout=60)
    finally:
        os.unlink(ruta)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


@unittest.skipUnless(shutil.which("node"), "node no está instalado")
class TestComportamiento(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        garantia = dict(_TODAS, fin_garantia=True)
        cls.res = _correr_node([
            (_TODAS, [], {}),                                                    # 0 nada se tocó, todo a la vista
            (_TODAS, [["escribir", "0"]], {}),                                   # 1 OT 281: la persona escribe $0
            (_TODAS, [["usar", "zz"]], {}),                                      # 2 elige la línea del documento
            (_TODAS, [["usar", "estimado"]], {}),                                # 3 acepta el estimado (OT que se cobra)
            (_TODAS, [["usar", "cotizacion"]], {}),                              # 4 acepta la cotización
            (_TODAS, [["usar", "contrato"]], {}),                                # 5 acepta el contrato
            (garantia, [["usar", "estimado"]], {}),                              # 6 garantía: el estimado es el valorizado
            (_SIN_REFERENCIAS, [["escribir", "5000"]], {}),                      # 7 sin referencias: supuesto
            (_TODAS, [["escribir", "1000"], ["escribir", ""]], {}),              # 8 escribe y borra: no queda nada
            (_TODAS, [["usar", "estimado"], ["escribir", "0"]], {}),             # 9 acepta el estimado y lo cambia a $0
            (_TODAS, [["linea", "ZZMANTENCION", 100000]], {}),                   # 10 elige una línea de la lista
            (dict(_TODAS, fin_zz_envio={"sku": "ZZENVIO", "monto": 30000}, fin_zz_envio_original=30000,
                  fin_costo_proveedor="80000", fin_costo_despacho="0"),
             [["usar", "zz"]], {}),                                              # 11 vista previa = lo que se envía
            (dict(_TODAS, fin_zz_envio={"sku": "ZZENVIO", "monto": 30000}, fin_zz_envio_original=30000,
                  fin_costo_proveedor="80000", fin_garantia=True),
             [["escribir", "90000"]], {}),                                       # 12 ídem en garantía
            (dict(_TODAS, fin_zz_envio={"sku": "ZZENVIO", "monto": None}, fin_zz_envio_original=30000),
             [], {}),                                                            # 13 el documento trae transporte: referencia
            (dict(_TODAS, fin_zz_envio={"sku": "ZZENVIO", "monto": None}, fin_zz_envio_original=30000),
             [["envio_doc"]], {}),                                               # 14 la persona aprieta «Usar este monto»
        ])

    def test_con_todo_a_la_vista_y_nada_tocado_no_viaja_ningun_monto(self):
        r = self.res[0]
        self.assertIsNone(r["monto"], "ninguna referencia se aplica sola")
        self.assertNotIn("zz_monto", r["envio"])
        self.assertNotIn("valor_origen", r["envio"])
        self.assertNotIn("valorizado_clp", r["envio"])
        # …pero las sugerencias SÍ están a la vista, con su monto (ningún chip desaparece)
        f = {x[0]: x for x in r["fuentes"]}
        self.assertEqual((f["zz"][1], f["zz"][2]), ("ok", 100000))
        self.assertEqual((f["cotizacion"][1], f["cotizacion"][2]), ("ok", 55000))
        self.assertEqual((f["contrato"][1], f["contrato"][2]), ("ok", 70000))
        self.assertEqual((f["estimado"][1], f["estimado"][2]), ("ok", 10034))

    def test_el_cero_escrito_es_cero_y_se_envia_como_cero(self):
        r = self.res[1]
        self.assertEqual(r["monto"], 0)
        self.assertEqual(r["envio"]["zz_monto"], 0, "OT 281: el $0 escrito no se reemplaza por el estimado de $10.034")
        self.assertNotEqual(r["envio"]["zz_monto"], 10034)
        self.assertEqual(r["envio"]["valor_origen"], "manual", "lo escrito con referencias a la vista es «manual»")
        self.assertTrue(r["pideMotivo"], "lo escrito lleva su motivo")

    def test_linea_del_documento_solo_si_la_persona_la_elige(self):
        for i in (2, 10):
            r = self.res[i]
            self.assertEqual(r["monto"], 100000)
            self.assertEqual((r["envio"]["zz_monto"], r["envio"]["zz_codigo"], r["envio"]["valor_origen"]),
                             (100000, "ZZMANTENCION", "zz"))
            self.assertFalse(r["pideMotivo"], "una línea del documento no pide motivo")

    def test_estimado_aceptado_en_ot_que_se_cobra_viaja_manual_y_pide_motivo(self):
        r = self.res[3]
        self.assertEqual(r["envio"]["zz_monto"], 10034)
        self.assertEqual(r["envio"]["valor_origen"], "manual", "el servidor no acepta 'estimado' como cobro")
        self.assertEqual(r["envio"]["zz_motivo_manual"], "", "el motivo lo escribe la persona; se exige antes de crear")
        self.assertTrue(r["pideMotivo"])

    def test_cotizacion_y_contrato_aceptados_son_referencias_reales(self):
        for i, origen, monto in ((4, "cotizacion", 55000), (5, "contrato", 70000)):
            r = self.res[i]
            self.assertEqual((r["envio"]["zz_monto"], r["envio"]["valor_origen"]), (monto, origen))
            self.assertFalse(r["pideMotivo"])

    def test_en_garantia_el_estimado_es_solo_valorizado(self):
        r = self.res[6]
        self.assertEqual(r["envio"]["valorizado_clp"], 10034)
        self.assertEqual(r["envio"]["valorizado_fuente"], "estimado")
        self.assertFalse(r["pideMotivo"])
        self.assertIsNone(r["previa"]["zz_monto"], "en garantía no se cobra nada")
        self.assertEqual(r["previa"]["valorizado_clp"], 10034)

    def test_sin_referencias_lo_escrito_es_supuesto(self):
        r = self.res[7]
        self.assertEqual((r["envio"]["zz_monto"], r["envio"]["valor_origen"]), (5000, "supuesto"))

    def test_borrar_lo_escrito_deja_el_casillero_vacio_sin_rellenar(self):
        r = self.res[8]
        self.assertIsNone(r["monto"])
        self.assertNotIn("zz_monto", r["envio"], "vaciar el campo no hace que vuelva una cotización ni un estimado")

    def test_cambiar_un_estimado_a_cero_envia_cero(self):
        r = self.res[9]
        self.assertEqual((r["envio"]["zz_monto"], r["envio"]["valor_origen"]), (0, "manual"))

    def test_el_transporte_del_documento_es_referencia_hasta_que_se_usa(self):
        r = self.res[13]
        self.assertNotIn("zz_envio_monto", r["envio"], "el transporte reconocido no viaja solo")
        self.assertNotIn("zz_envio_codigo", r["envio"])
        self.assertIn("El documento trae transporte por", r["sug"])
        self.assertIn("Usar este monto", r["sug"])
        self.assertIn("30000", r["sug"].replace(".", "").replace("$", ""))
        r = self.res[14]
        self.assertEqual((r["envio"]["zz_envio_monto"], r["envio"]["zz_envio_codigo"]), (30000, "ZZENVIO"))
        self.assertNotIn("zz_envio_motivo_manual", r["envio"], "lo que trae el documento no pide motivo")
        self.assertIn("En uso", r["sug"])
        self.assertNotIn("Usar este monto", r["sug"])

    def test_la_vista_previa_usa_exactamente_lo_que_se_envia(self):
        for i in range(15):
            r = self.res[i]
            e, p = r["envio"], r["previa"]
            gar = i in (6, 12)
            self.assertEqual(p["zz_monto"], None if gar else e.get("zz_monto"), f"escenario {i}: zz_monto")
            self.assertEqual(p["zz_codigo"], None if gar else (e.get("zz_codigo") or None), f"escenario {i}: zz_codigo")
            for k in ("valor_origen", "zz_envio_monto", "costo_proveedor", "costo_despacho", "valorizado_clp",
                      "valorizado_fuente"):
                self.assertEqual(p[k], e.get(k), f"escenario {i}: {k}")
        # el escenario 11 trae despacho y costos: viajan Y se ven en la vista previa
        e = self.res[11]["envio"]
        self.assertEqual((e["zz_envio_monto"], e["costo_proveedor"], e["costo_despacho"]), (30000, 80000, 0))


@unittest.skipUnless(shutil.which("node"), "node no está instalado")
class TestGateDelPasoCostos(unittest.TestCase):
    """completo('finanzas') ejecutado de verdad: una interna sin valor pasa; un cobro de $0 no avanza como monto."""

    @classmethod
    def setUpClass(cls):
        i = M.index("case 'finanzas':")
        j = M.index("case 'anexo':", i)
        cuerpo = M[i + len("case 'finanzas':"):j]
        gate = "function gate(){" + cuerpo + "\n}"
        casos = {
            "interna_sin_valor": [dict(fin_centro="sstt", costo_interno=""), True],
            "interna_cero_numero": [dict(fin_centro="sstt", costo_interno=0), True],
            "interna_cero_texto": [dict(fin_centro="sstt", costo_interno="0"), True],
            "interna_con_valor": [dict(fin_centro="sstt", costo_interno=45000), True],
            "interna_sin_centro": [dict(fin_centro=None, costo_interno=""), False],
            "cobra_con_cero": [dict(fin_centro="sstt", fin_doc={"tido": "FCV", "nudo": "1"}, fin_zz_monto=0,
                                    fin_costo_proveedor="0"), False],
            "cobra_sin_monto": [dict(fin_centro="sstt", fin_doc={"tido": "FCV", "nudo": "1"}, fin_zz_monto=None,
                                     fin_costo_proveedor="0"), False],
            "cobra_con_monto": [dict(fin_centro="sstt", fin_doc={"tido": "FCV", "nudo": "1"}, fin_zz_monto=90000,
                                     fin_costo_proveedor="0"), True],
            "garantia_sin_monto": [dict(fin_centro="sstt", fin_garantia=True, fin_garantia_motivo="x" * 30,
                                        fin_costo_proveedor=""), True],
        }
        interna = {k for k in casos if k.startswith("interna")}
        js = r"""
const GATE = %s; const CASOS = %s; const INTERNA = %s;
const out = {};
Object.keys(CASOS).forEach(function(k){
  const f = new Function('S0', 'INT', `
    var S = Object.assign({fin_centro:null, fin_garantia:false, fin_aut:false, fin_doc:null, fin_zz_monto:null,
      fin_zz_motivo_manual:'', fin_garantia_motivo:'', fin_costo_proveedor:'', fin_zz_envio:null, fin_zz_envio_original:null,
      fin_zz_envio_motivo_manual:'', costo_interno:''}, S0);
    function esInterno(){ return INT; }
    function _o2mFinPideMotivo(){ return false; }
    function _o2mFinEnvioEsManual(){ return false; }
    ` + GATE + `
    return gate();`);
  out[k] = f(CASOS[k][0], INTERNA.indexOf(k) >= 0);
});
process.stdout.write(JSON.stringify(out));
""" % (json.dumps(gate), json.dumps(casos), json.dumps(sorted(interna)))
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as fh:
            fh.write(js)
            ruta = fh.name
        try:
            r = subprocess.run(["node", ruta], capture_output=True, text=True, encoding="utf-8", timeout=60)
        finally:
            os.unlink(ruta)
        assert r.returncode == 0, r.stderr
        cls.out, cls.casos = json.loads(r.stdout), casos

    def test_cada_caso(self):
        for k, (_, esperado) in self.casos.items():
            self.assertEqual(self.out[k], esperado, k)


# ── Servidor: acepta exactamente la regla (sin rellenar nada por su cuenta) ─────────────────────────────────────────
MOTIVOS_CERO = {"garantia": "Garantía", "regalia": "Regalía", "arriendo_leasing": "Arriendo o leasing"}


class TestServidorAceptaLaRegla(unittest.TestCase):
    DOC = {"centro_costo": "sstt", "factura_tido": "FCV", "factura_nudo": "11439", "costo_proveedor": 0}

    @staticmethod
    def N(*a, **k):
        from tests.test_ot_finanzas_creacion import N, _amb
        # El arnés de esa prueba no trae esta constante (solo la usa el camino del $0 declarado): se inyecta la MISMA
        # que tiene app.py (se vigila abajo que el texto no haya cambiado).
        _amb().setdefault("_OT_COBRO_CERO_MOTIVOS", MOTIVOS_CERO)
        return N(*a, **k)

    def test_lo_cobrado_viene_de_la_linea_elegida_o_de_lo_escrito_con_motivo(self):
        err, c = self.N(dict(self.DOC, zz_monto=100000, valor_origen="zz", zz_codigo="ZZMANTENCION"), tipo="preventiva")
        self.assertIsNone(err)
        self.assertEqual((c["zz_monto"], c["cobertura"]), (100000, "cobra"))
        err, c = self.N(dict(self.DOC, zz_monto=80000, valor_origen="manual", zz_motivo_manual="se acordó esta tarifa"))
        self.assertIsNone(err)
        self.assertEqual(c["zz_monto"], 80000)
        err, _ = self.N(dict(self.DOC, zz_monto=80000, valor_origen="manual"))
        self.assertEqual(err["error_codigo"], "FINANZAS_SUPUESTO_SIN_MOTIVO", "lo escrito lleva su motivo")
        err, _ = self.N(dict(self.DOC, zz_monto=80000, valor_origen="supuesto"))
        self.assertEqual(err["error_codigo"], "FINANZAS_SUPUESTO_SIN_MOTIVO")

    def test_una_referencia_aceptada_con_el_boton_entra_como_cotizacion_o_contrato(self):
        for origen in ("cotizacion", "contrato"):
            err, c = self.N(dict(self.DOC, zz_monto=55000, valor_origen=origen))
            self.assertIsNone(err, origen)
            self.assertEqual((c["zz_monto"], c["valor_origen"]), (55000, origen))

    def test_el_estimado_nunca_es_lo_cobrado_en_una_ot_que_se_cobra(self):
        err, _ = self.N(dict(self.DOC, zz_monto=10034, valor_origen="estimado"))
        self.assertEqual(err["error_codigo"], "FINANZAS_ESTIMADO_NO_ES_COBRO")
        # el asistente lo manda 'manual' con motivo cuando la persona lo acepta
        err, c = self.N(dict(self.DOC, zz_monto=10034, valor_origen="manual", zz_motivo_manual="tarifa acordada"))
        self.assertIsNone(err)
        self.assertEqual((c["zz_monto"], c["valor_origen"]), (10034, "manual"))

    def test_motivos_del_cero_son_los_de_app(self):
        self.assertIn('_OT_COBRO_CERO_MOTIVOS = {"garantia": "Garantía", "regalia": "Regalía", '
                      '"arriendo_leasing": "Arriendo o leasing"}', _leer("app.py"))

    def test_un_cero_escrito_se_respeta_y_se_declara_no_se_reemplaza(self):
        err, _ = self.N(dict(self.DOC, zz_monto=0, valor_origen="manual", zz_motivo_manual="cortesía"))
        self.assertEqual(err["error_codigo"], "FINANZAS_CERO_SIN_DECLARAR")
        # …y como regalía con su argumento el $0 se guarda tal cual, sin monto cobrado ni valorizado inventados
        err, c = self.N({"centro_costo": "sstt", "cobro_cero_motivo": "regalia", "cobro_cero_argumento": "x" * 40,
                         "zz_monto": 0, "valor_origen": "manual", "zz_motivo_manual": "regalía", "costo_proveedor": 0})
        self.assertIsNone(err)
        self.assertEqual(c["cobertura"], "regalia")
        self.assertIsNone(c["valorizado_clp"])
        self.assertIsNone(c["costo_cliente"])

    def test_sin_monto_no_se_inventa_ninguno(self):
        err, _ = self.N(dict(self.DOC))
        self.assertEqual(err["error_codigo"], "FINANZAS_SIN_MONTO", "el servidor no rellena el cobro con una cotización ni un estimado")
        err, c = self.N({"centro_costo": "sstt", "garantia_aplica": True, "garantia_motivo": "falla de fábrica del motor"})
        self.assertIsNone(err)
        self.assertEqual((c["zz_monto"], c["valorizado_clp"], c["costo_cliente"]), (None, None, None))

    def test_el_espejo_costo_solo_copia_lo_cobrado(self):
        err, c = self.N(dict(self.DOC, zz_monto=100000, valor_origen="zz", zz_envio_monto=20000, zz_envio_codigo="ZZENVIO"))
        self.assertIsNone(err)
        self.assertEqual(c["costo_cliente"], 120000, "= servicio + despacho cobrados, nada más")
        err, c = self.N(dict(self.DOC, zz_monto=100000, valor_origen="cotizacion"))
        self.assertEqual(c["costo_cliente"], 100000)

    def test_interna_sin_valor_o_en_cero_pasa(self):
        for fin in ({}, {"costo_interno": 0}, {"costo_interno": ""}, {"costo_interno": 0, "valor_origen": "manual"}):
            err, c = self.N(fin, tipo="revision_interna", interna=True)
            self.assertIsNone(err, fin)
            self.assertEqual(c["cobertura"], "interno")
            self.assertIsNone(c["valorizado_clp"], "sin valor escrito no se inventa un valorizado")
            self.assertIsNone(c["zz_monto"])
            self.assertIsNone(c["costo_cliente"])

    def test_interna_con_valor_es_solo_valorizado(self):
        err, c = self.N({"costo_interno": 45000, "valor_origen": "interno"}, tipo="revision_interna", interna=True)
        self.assertIsNone(err)
        self.assertEqual((c["valorizado_clp"], c["zz_monto"], c["costo_cliente"]), (45000.0, None, None))


if __name__ == "__main__":
    unittest.main()
