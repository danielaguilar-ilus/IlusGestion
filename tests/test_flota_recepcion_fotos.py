# -*- coding: utf-8 -*-
"""🚚 FLOTA · recepción de la llave confirmada por la gestión y fotos más rápidas con barra de progreso (Daniel, 05-nov-2026):
«cuando el chofer gestione la entrega de llaves a Aarón, este las pueda confirmar y que los datos son reales, y ahí sí quedaría cerrado, al igual
que cuando solicitan la agenda Aarón debe confirmar la agenda» y «tarda mucho subiendo las fotos, colócale una barra de proceso».

Decisiones que se vigilan:
  1. Una solicitud devuelta queda «Devuelta · por confirmar» hasta que la gestión confirma la recepción (marcando que los datos son reales); ahí queda
     «Cerrada». Una sola vez, solo la gestión, y queda en la bitácora con quién y cuándo. No manda correos nuevos.
  2. Las devueltas sin confirmar se ven siempre (aunque su horario ya pasó) y se cuentan («Por cerrar»).
  3. Las fotos se reducen en el celular, suben con su avance (preparar, subir, guardar) y el servidor guarda las cuatro a la vez.
Correr:  python -m pytest tests/test_flota_recepcion_fotos.py -q -p no:warnings
"""
import datetime as dt
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import flota_reglas as R  # noqa: E402
from test_flota_solicitudes import (  # noqa: E402
    SUPER, Falsa, _app, _datos_chofer, _leer, _llave_cualquier_dia)
from test_flota_vehiculos import BucketFalso  # noqa: E402

HAY_NODE = shutil.which("node") is not None
SOLO_PEDIR = {"X-Rol": "ejecutivo", "X-Flota": "flota_solicitar"}
JPG = b"\xff\xd8\xff\xe0" + b"\x00" * 64


class _BlobRoto:
    cache_control = None

    def upload_from_string(self, *a, **k):
        raise RuntimeError("GCS caído")


class _BucketRoto:
    def blob(self, clave):
        return _BlobRoto()


class BaseRecepcion(unittest.TestCase):
    """Una reserva con la llave ya entregada, lista para devolver."""

    def setUp(self):
        _llave_cualquier_dia(self)
        self.enviados = []
        self.base = Falsa()
        self.bucket = BucketFalso()
        aviso = lambda clave, uids, titulo, cuerpo, url, etiqueta="", ref=None: self.enviados.append((clave, titulo, cuerpo)) or 1
        self.app = _app(self.base, self.bucket, extra={"_aviso_interno_correo": aviso})
        self.c = self.app.test_client()
        self.vid = self.c.post("/flota/api/vehiculos", json={"patente": "PHVD21-7", "tipo": "Camión", "licencia_clases": ["B"], "km_actual": 56000},
                               headers=SUPER).get_json()["vehiculo"]["id"]
        cid = self.c.post("/flota/api/choferes", json=dict(_datos_chofer(1), usuario_id=1), headers=SUPER).get_json()["chofer"]["id"]
        dia = (dt.date.today() + dt.timedelta(days=5)).isoformat()
        self.sid = self.c.post("/flota/api/solicitudes", json={"vehiculo_id": self.vid, "chofer_id": cid, "area": "Logística",
                               "motivo": "Reparto zona norte", "fecha": dia, "hora_desde": "09:00", "hora_hasta": "11:00"},
                               headers=SUPER).get_json()["solicitud"]["id"]
        self.c.post(f"/flota/api/solicitudes/{self.sid}/confirmar", json={}, headers=SUPER)
        self.c.post(f"/flota/api/solicitudes/{self.sid}/llave", json={"nota": ""}, headers=SUPER)
        self.enviados.clear()

    def devolver(self, quitar=(), fotos=None, headers=SUPER, **campos):
        data = {"km": "56.240", "combustible": "1_2", "observaciones": "", "novedad": ""}
        data.update(campos)
        for tipo, _n in R.FOTOS_DEVOLUCION:
            if tipo not in quitar:
                data[tipo] = (io.BytesIO((fotos or {}).get(tipo, JPG)), tipo + ".jpg", "image/jpeg")
        return self.c.post(f"/flota/api/solicitudes/{self.sid}/devolucion", data=data, content_type="multipart/form-data", headers=headers)

    def solicitud(self, headers=SUPER):
        return self.c.get(f"/flota/api/solicitudes/{self.sid}", headers=headers).get_json()

    def recepcion(self, cuerpo=None, headers=SUPER):
        return self.c.post(f"/flota/api/solicitudes/{self.sid}/recepcion", json={"datos_reales": True} if cuerpo is None else cuerpo, headers=headers)


# ══════════════════════════════════════════════════════════════════
#  1. La gestión confirma la recepción y ahí se cierra
# ══════════════════════════════════════════════════════════════════
class TestConfirmarRecepcion(BaseRecepcion):
    def test_recien_devuelta_queda_por_confirmar(self):
        r = self.devolver()
        self.assertEqual(r.status_code, 200, r.get_json())
        s = r.get_json()["solicitud"]
        self.assertEqual((s["estado"], s["cierre"], s["estado_texto"]), ("devuelta", "por_confirmar", "Devuelta · por confirmar"))
        self.assertTrue(s["puede"]["confirmar_recepcion"])
        self.assertIsNone(s["devolucion"]["confirmada_por"])

    def test_antes_de_devolver_no_hay_nada_que_confirmar(self):
        s = self.solicitud()["solicitud"]
        self.assertIsNone(s["cierre"])
        self.assertFalse(s["puede"]["confirmar_recepcion"])
        r = self.recepcion()
        self.assertEqual(r.status_code, 409)
        self.assertIn("ya devuelto", r.get_json()["error"])

    def test_confirmar_sin_marcar_que_son_reales_no_cierra(self):
        self.devolver()
        for cuerpo in ({}, {"datos_reales": False}, {"nota": "ok"}):
            r = self.recepcion(cuerpo)
            self.assertEqual(r.status_code, 400, cuerpo)
            self.assertIn("datos_reales", r.get_json()["errores"])
        self.assertEqual(self.solicitud()["solicitud"]["cierre"], "por_confirmar")

    def test_confirmar_cierra_con_quien_y_cuando(self):
        self.devolver()
        r = self.recepcion({"datos_reales": True, "nota": "  La llave quedó en la oficina  "})
        self.assertEqual(r.status_code, 200, r.get_json())
        s = r.get_json()["solicitud"]
        self.assertEqual((s["estado"], s["cierre"], s["estado_texto"]), ("devuelta", "cerrada", "Cerrada"))
        self.assertEqual(s["devolucion"]["confirmada_por"], "Daniel Aguilar")
        self.assertTrue(s["devolucion"]["confirmada_fmt"])
        self.assertEqual(s["devolucion"]["confirmada_nota"], "La llave quedó en la oficina")
        self.assertFalse(s["puede"]["confirmar_recepcion"])

    def test_queda_en_la_bitacora_y_no_manda_correos_nuevos(self):
        self.devolver()
        self.enviados.clear()
        self.recepcion({"datos_reales": True, "nota": "Todo en orden"})
        self.assertEqual(self.enviados, [])
        ev = self.solicitud()["eventos"][0]
        self.assertEqual(ev["titulo"], "Recepción confirmada · solicitud cerrada")
        self.assertEqual(ev["usuario"], "Daniel Aguilar")
        self.assertTrue(any("Todo en orden" in x for x in ev["lineas"]))

    def test_se_confirma_una_sola_vez(self):
        self.devolver()
        self.assertEqual(self.recepcion().status_code, 200)
        r = self.recepcion()
        self.assertEqual(r.status_code, 409)
        self.assertIn("ya la confirmó", r.get_json()["error"])
        self.assertEqual(len([e for e in self.solicitud()["eventos"] if e["accion"] == "recepcion_confirmada"]), 1)

    def test_solo_la_gestion_confirma(self):
        self.devolver()
        r = self.recepcion(headers=SOLO_PEDIR)
        self.assertEqual(r.status_code, 403)
        self.assertEqual(self.solicitud()["solicitud"]["cierre"], "por_confirmar")
        self.assertFalse(self.solicitud(SOLO_PEDIR)["solicitud"]["puede"]["confirmar_recepcion"])       # ni el botón se le ofrece

    def test_la_nota_tiene_tope(self):
        self.devolver()
        r = self.recepcion({"datos_reales": True, "nota": "x" * 501})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(self.solicitud()["solicitud"]["cierre"], "por_confirmar")

    def test_el_aviso_de_la_devolucion_le_pide_a_la_gestion_cerrarla(self):
        self.devolver()
        self.assertEqual([e[0] for e in self.enviados], ["flota_devolucion"])
        self.assertIn("Confirma la recepción", self.enviados[0][2])

    def test_una_devuelta_antigua_sin_confirmar_se_ve_y_se_cuenta(self):
        self.devolver()
        self.base.sol[self.sid]["inicio"] -= dt.timedelta(days=30)
        self.base.sol[self.sid]["fin"] -= dt.timedelta(days=30)
        j = self.c.get("/flota/api/solicitudes", headers=SUPER).get_json()
        self.assertIn(self.sid, [s["id"] for s in j["solicitudes"]])             # aunque su horario ya pasó, sigue a la vista
        self.assertEqual(j["resumen"]["por_cerrar"], 1)
        self.recepcion()
        j = self.c.get("/flota/api/solicitudes", headers=SUPER).get_json()
        self.assertNotIn(self.sid, [s["id"] for s in j["solicitudes"]])          # ya cerrada y pasada: sale de «Próximas»
        self.assertEqual(j["resumen"]["por_cerrar"], 0)
        j = self.c.get("/flota/api/solicitudes?vista=pasadas", headers=SUPER).get_json()
        self.assertIn(self.sid, [s["id"] for s in j["solicitudes"]])


# ══════════════════════════════════════════════════════════════════
#  2. Las fotos suben a la vez y el resultado es el mismo de antes
# ══════════════════════════════════════════════════════════════════
class TestFotosEnParalelo(BaseRecepcion):
    def fotos_guardadas(self):
        return [a for a in self.base.arch.values() if a["entidad"] == "solicitud" and a["entidad_id"] == self.sid and a["activo"]]

    def test_las_cuatro_se_guardan_con_su_registro(self):
        r = self.devolver()
        self.assertEqual(r.status_code, 200, r.get_json())
        self.assertEqual(sorted(a["tipo"] for a in self.fotos_guardadas()), sorted(t for t, _n in R.FOTOS_DEVOLUCION))
        for a in self.fotos_guardadas():
            self.assertTrue(a["gcs_key"].startswith(f"flota_privado/solicitud/{self.sid}/"), a["gcs_key"])
            self.assertIn(a["gcs_key"], self.bucket.objetos)
        self.assertEqual(len([k for k in self.bucket.objetos if k.startswith('flota_privado/solicitud/')]), 4)       # (la licencia del chofer también vive ahí)

    def test_una_foto_que_no_es_imagen_frena_todo_sin_registrar_ninguna(self):
        r = self.devolver(fotos={"dev_interior": b"MZ\x90 virus"})
        self.assertEqual(r.status_code, 400)
        self.assertIn("dev_interior", r.get_json()["errores"])
        self.assertEqual(self.fotos_guardadas(), [])
        self.assertEqual(self.base.sol[self.sid]["estado"], "llave_entregada")              # la devolución no se registró

    def test_sin_almacenamiento_no_se_devuelve(self):
        app = _app(self.base, None, extra={"_aviso_interno_correo": lambda *a, **k: 1})
        r = app.test_client().post(f"/flota/api/solicitudes/{self.sid}/devolucion", content_type="multipart/form-data", headers=SUPER, data={
            "km": "56.240", "combustible": "1_2", "observaciones": "", "novedad": "",
            **{t: (io.BytesIO(JPG), t + ".jpg", "image/jpeg") for t, _n in R.FOTOS_DEVOLUCION}})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(self.base.sol[self.sid]["estado"], "llave_entregada")

    def test_si_falla_la_subida_a_la_nube_no_se_registra_nada(self):
        app = _app(self.base, _BucketRoto(), extra={"_aviso_interno_correo": lambda *a, **k: 1})
        r = app.test_client().post(f"/flota/api/solicitudes/{self.sid}/devolucion", content_type="multipart/form-data", headers=SUPER, data={
            "km": "56.240", "combustible": "1_2", "observaciones": "", "novedad": "",
            **{t: (io.BytesIO(JPG), t + ".jpg", "image/jpeg") for t, _n in R.FOTOS_DEVOLUCION}})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(self.fotos_guardadas(), [])
        self.assertEqual(self.base.sol[self.sid]["estado"], "llave_entregada")

    def test_las_cuatro_suben_a_la_vez(self):
        """El almacenamiento de mentira solo deja avanzar cuando las cuatro subidas están en curso al mismo tiempo: en fila, se rompe."""
        import threading
        barrera = threading.Barrier(4, timeout=4)

        class _Blob:
            cache_control = None

            def __init__(self, bucket, clave):
                self.b, self.k = bucket, clave

            def upload_from_string(self, data, content_type=None):
                barrera.wait()
                self.b.objetos[self.k] = (bytes(data), content_type)

        class _BucketJuntas(BucketFalso):
            def blob(self, clave):
                return _Blob(self, clave)

        bucket = _BucketJuntas()
        app = _app(self.base, bucket, extra={"_aviso_interno_correo": lambda *a, **k: 1})
        r = app.test_client().post(f"/flota/api/solicitudes/{self.sid}/devolucion", content_type="multipart/form-data", headers=SUPER, data={
            "km": "56.240", "combustible": "1_2", "observaciones": "", "novedad": "",
            **{t: (io.BytesIO(JPG), t + ".jpg", "image/jpeg") for t, _n in R.FOTOS_DEVOLUCION}})
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(len([k for k in bucket.objetos if k.startswith("flota_privado/solicitud/")]), 4)

    def test_el_codigo_sube_en_hilos_y_registra_despues_en_orden(self):
        src = _leer("flota_module.py")
        i = src.index("def flota_api_solicitud_devolucion(sid):")
        cuerpo = src[i:i + 6000]
        self.assertIn("ThreadPoolExecutor", cuerpo)
        self.assertLess(cuerpo.index("_subir_imagen_solicitud"), cuerpo.index("_registrar_imagen_solicitud"))
        # la función que corre en el hilo no toca la base ni lee la petición
        j = src.index("def _subir_imagen_solicitud(")
        hilo = src[j:src.index("def _registrar_imagen_solicitud(", j)]
        for prohibido in ("mysql_execute", "request.", "current_username"):
            self.assertNotIn(prohibido, hilo, prohibido)


# ══════════════════════════════════════════════════════════════════
#  3. La pantalla
# ══════════════════════════════════════════════════════════════════
class TestPantallaRecepcion(unittest.TestCase):
    HTML = _leer("templates", "flota", "index.html")
    JS = _leer("static", "flota_uso.js")
    CSS = _leer("static", "flota.css")

    def test_el_modal_de_la_recepcion_existe_y_exige_marcar_la_casilla(self):
        for id_ in ("flModalRecep", "flRecReal", "flRecNota", "flRecGuardar", "flRecResumen"):
            self.assertIn(f'id="{id_}"', self.HTML, id_)
        self.assertRegex(self.HTML, r'id="flRecGuardar" disabled')                    # nace apagado hasta marcar que los datos son reales
        self.assertIn("Recibí la llave y confirmo que los datos son reales", self.HTML)

    def test_la_guia_tiene_un_quinto_paso_y_el_boton(self):
        self.assertIn("paso(5, 'cierre', 'Recepción confirmada · cierre'", self.JS)
        self.assertIn('data-dacc="recepcion"', self.JS)
        self.assertIn('data-acc="recepcion"', self.JS)
        self.assertIn("a === 'recepcion'", self.JS)

    def test_confirma_por_el_servidor_marcando_que_son_reales(self):
        i = self.JS.index("function guardarRecep()")
        cuerpo = self.JS[i:i + 1500]
        self.assertIn("'/recepcion'", cuerpo)
        self.assertIn("datos_reales: true", cuerpo)

    def test_la_devuelta_se_ve_por_confirmar_o_cerrada(self):
        self.assertIn("return s.cierre === 'cerrada' ? 'cerrada' : 'por_cerrar'", self.JS)
        self.assertIn(".fl-est--cerrada{", self.CSS)
        self.assertIn(".fl-est--por_cerrar{", self.CSS)

    def test_hay_un_filtro_por_cerrar_con_su_contador(self):
        self.assertIn('data-sest="por_cerrar"', self.HTML)
        self.assertIn('id="flChipPorCerrarN"', self.HTML)
        self.assertIn("s.cierre !== 'por_confirmar'", self.JS)

    def test_la_casilla_se_ve_bien_en_el_celular(self):
        self.assertIn("#flModalRecep .modal-dialog{margin:0;max-width:none}", self.CSS)
        self.assertRegex(self.CSS, r"\.fl-recep-chk input\{width:26px;height:26px")      # blanco táctil grande


class TestPantallaFotos(unittest.TestCase):
    HTML = _leer("templates", "flota", "index.html")
    JS = _leer("static", "flota_uso.js")
    SUB = _leer("static", "flota_subida.js")
    CSS = _leer("static", "flota.css")

    def test_la_barra_esta_en_la_devolucion_y_en_el_registro_del_chofer(self):
        self.assertIn('id="flDevProgreso"', self.HTML)
        self.assertIn('id="flChProgreso"', self.HTML)
        self.assertLess(self.HTML.index("flota_subida.js"), self.HTML.index("static', filename='flota.js'"))        # se carga ANTES que los demás

    def test_la_devolucion_reduce_sube_con_avance_y_guarda(self):
        i = self.JS.index("function guardarDev()")
        cuerpo = self.JS[i:i + 3600]
        self.assertLess(cuerpo.index("flotaSubida.comprimir"), cuerpo.index("flotaSubida.enviar"))
        for texto in ("Preparando las fotos", "Subiendo las fotos", "Guardando la devolución"):
            self.assertIn(texto, cuerpo, texto)
        self.assertNotIn("fetch(", cuerpo)                                           # el fetch no da avance de la subida
        self.assertIn("ponerCierresDev(true)", cuerpo)                               # no se puede cerrar la ventana mientras sube

    def test_se_avisa_si_se_intenta_salir_a_medio_subir(self):
        self.assertIn("beforeunload", self.JS)

    def test_la_barra_ocupa_todo_el_ancho_y_respeta_el_movimiento_reducido(self):
        self.assertRegex(self.CSS, r"\.fl-prog-wrap\{flex:1 1 100%")
        self.assertIn("prefers-reduced-motion", self.CSS[self.CSS.index(".fl-prog-bar"):])
        self.assertIn('role="progressbar"', self.SUB)
        self.assertIn("aria-valuenow", self.SUB)

    def test_el_envio_manda_el_token_csrf(self):
        self.assertIn("X-CSRF-Token", self.SUB)
        self.assertIn("window.__CSRF_TOKEN", self.SUB)


# ── La lógica de flota_subida.js EJECUTADA con Node (canvas, imagen y XMLHttpRequest de mentira) ──
_PRUEBA_JS = r'''
const fs = require('fs');
const src = fs.readFileSync(process.argv[2], 'utf8');
const resultados = {};
let canvasDims = null, blobSize = 0, imgDim = [4000, 3000], imgFalla = false, creadas = [];
class FakeImage {
  set src(v) { setTimeout(() => { if (imgFalla) { this.onerror && this.onerror(); } else { this.naturalWidth = imgDim[0]; this.naturalHeight = imgDim[1]; this.onload && this.onload(); } }, 0); }
}
class FakeFile { constructor(partes, nombre, o) { this.size = partes[0].size; this.name = nombre; this.type = (o || {}).type; creadas.push(this); } }
const documento = { createElement: () => { const cv = { width: 0, height: 0, getContext: () => ({ fillRect() {}, drawImage() { canvasDims = [cv.width, cv.height]; } }),
  toBlob(cb, tipo, q) { cb(blobSize ? { size: blobSize, type: tipo, calidad: q } : null); } }; return cv; } };
const ventana = { URL: { createObjectURL: () => 'blob:x', revokeObjectURL() {} }, __CSRF_TOKEN: 'tok123' };
class FakeXHR {
  constructor() { this.cab = {}; this.upload = {}; FakeXHR.ultimo = this; }
  open(m, u) { this.m = m; this.u = u; }
  setRequestHeader(k, v) { this.cab[k] = v; }
  send(f) { this.enviado = f; }
}
new Function('window', 'document', 'Image', 'URL', 'File', 'XMLHttpRequest', src)(ventana, documento, FakeImage, ventana.URL, FakeFile, FakeXHR);
const S = ventana.flotaSubida;
const dormir = () => new Promise(r => setTimeout(r, 5));
(async () => {
  const grande = { name: 'IMG_0001.JPG', type: 'image/jpeg', size: 5 * 1024 * 1024 };
  // 1. una imagen chica no se toca (aunque el canvas pudiera achicarla)
  blobSize = 100 * 1024;
  const chica = { name: 'a.jpg', type: 'image/jpeg', size: 200 * 1024 };
  resultados.chica_igual = (await S.comprimir(chica)) === chica;
  // 2. un PDF no se toca
  const pdf = { name: 'l.pdf', type: 'application/pdf', size: 5 * 1024 * 1024 };
  resultados.pdf_igual = (await S.comprimir(pdf)) === pdf;
  blobSize = 0;
  // 3. una foto grande horizontal se reduce a 1600 px de lado mayor y queda mucho más liviana
  blobSize = 400 * 1024; imgDim = [4000, 3000];
  const r3 = await S.comprimir(grande);
  resultados.horizontal = { dims: canvasDims, nombre: r3.name, tipo: r3.type, peso: r3.size, esOtra: r3 !== grande };
  // 4. vertical
  imgDim = [3000, 4000];
  await S.comprimir(grande);
  resultados.vertical = canvasDims;
  // 5. si no ahorra nada, queda la original
  blobSize = 6 * 1024 * 1024;
  resultados.sin_ahorro_igual = (await S.comprimir(grande)) === grande;
  // 6. si el navegador no puede abrirla, queda la original
  blobSize = 400 * 1024; imgFalla = true;
  resultados.error_igual = (await S.comprimir(grande)) === grande;
  imgFalla = false;
  // 7. si el canvas no entrega nada, queda la original
  blobSize = 0;
  resultados.sin_blob_igual = (await S.comprimir(grande)) === grande;
  // 8. enviar: avance, token CSRF y respuesta
  const eventos = [];
  const p = S.enviar({ url: '/x', method: 'POST', form: { f: 1 }, onProgreso: (f, a, b) => eventos.push(['p', f, a, b]), onSubido: () => eventos.push(['s']) });
  const x = FakeXHR.ultimo;
  x.upload.onprogress({ lengthComputable: true, loaded: 50, total: 100 });
  x.upload.onprogress({ lengthComputable: false, loaded: 1, total: 0 });
  x.upload.onload();
  x.status = 200; x.responseText = '{"ok":true,"n":1}'; x.onload();
  const d = await p;
  resultados.enviar = { eventos, ok: d.ok, status: d.status, n: d.n, cab: x.cab, metodo: x.m, url: x.u, conCreds: x.withCredentials === true };
  // 9. respuesta que no es JSON
  const p2 = S.enviar({ url: '/x', form: {} }); const x2 = FakeXHR.ultimo; x2.status = 502; x2.responseText = '<html>'; x2.onload();
  const d2 = await p2; resultados.no_json = { ok: d2.ok, status: d2.status, hayError: !!d2.error };
  // 10. sin conexión y tiempo agotado
  const p3 = S.enviar({ url: '/x', form: {} }); FakeXHR.ultimo.onerror(); const d3 = await p3;
  const p4 = S.enviar({ url: '/x', form: {} }); FakeXHR.ultimo.ontimeout(); const d4 = await p4;
  resultados.fallas = { sinConexion: [d3.ok, d3.status, /conexión/.test(d3.error)], timeout: [d4.ok, d4.status, /tardó demasiado/.test(d4.error)] };
  // 11. formato del peso
  resultados.mb = [S.mb(1572864), S.mb(0), S.mb(undefined)];
  console.log(JSON.stringify(resultados));
})();
'''


@unittest.skipUnless(HAY_NODE, "hace falta Node para ejecutar el JavaScript")
class TestSubidaEjecutada(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ruta = os.path.join(tempfile.mkdtemp(prefix="flota_subida_"), "prueba.js")
        with open(ruta, "w", encoding="utf-8") as f:
            f.write(_PRUEBA_JS)
        p = subprocess.run(["node", ruta, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static", "flota_subida.js")],
                           capture_output=True, text=True, timeout=60)
        if p.returncode != 0:
            raise AssertionError("Node falló: " + p.stderr[-1500:])
        cls.r = json.loads(p.stdout.strip().splitlines()[-1])

    def test_lo_chico_y_los_pdf_no_se_tocan(self):
        self.assertTrue(self.r["chica_igual"])
        self.assertTrue(self.r["pdf_igual"])

    def test_una_foto_grande_se_reduce_a_1600_px_y_pesa_mucho_menos(self):
        h = self.r["horizontal"]
        self.assertEqual(h["dims"], [1600, 1200])
        self.assertEqual((h["nombre"], h["tipo"]), ("IMG_0001.jpg", "image/jpeg"))
        self.assertLess(h["peso"], 1024 * 1024)
        self.assertTrue(h["esOtra"])
        self.assertEqual(self.r["vertical"], [1200, 1600])

    def test_ante_cualquier_duda_se_sube_la_original(self):
        for clave in ("sin_ahorro_igual", "error_igual", "sin_blob_igual"):
            self.assertTrue(self.r[clave], clave)

    def test_el_envio_reporta_avance_manda_el_token_y_entrega_la_respuesta(self):
        e = self.r["enviar"]
        self.assertEqual(e["eventos"], [["p", 0.5, 50, 100], ["s"]])                 # solo con tamaño conocido; luego «subida completa»
        self.assertEqual((e["ok"], e["status"], e["n"]), (True, 200, 1))
        self.assertEqual(e["cab"].get("X-CSRF-Token"), "tok123")
        self.assertEqual(e["cab"].get("Accept"), "application/json")
        self.assertEqual((e["metodo"], e["url"], e["conCreds"]), ("POST", "/x", True))

    def test_los_errores_dicen_algo_claro(self):
        self.assertEqual(self.r["no_json"], {"ok": False, "status": 502, "hayError": True})
        self.assertEqual(self.r["fallas"]["sinConexion"], [False, 0, True])
        self.assertEqual(self.r["fallas"]["timeout"], [False, 0, True])

    def test_el_peso_se_escribe_en_megas_con_coma(self):
        self.assertEqual(self.r["mb"], ["1,5", "0,0", "0,0"])


if __name__ == "__main__":
    unittest.main()
