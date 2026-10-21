# -*- coding: utf-8 -*-
"""CONTROL DE CORREOS (Comunicaciones → «Control de correos», Daniel, 20-oct-2026).

Daniel: «una pestaña donde el superadmin ve TODOS los correos que el sistema envía y puede APAGAR cada uno (con motivo)».

Qué se prueba aquí (todo con correo de MENTIRA — REGLA #22: jamás un correo real ni a clientes):
  1. El REGISTRO (comm_correos_registro.py): completo, sin claves repetidas, cada llave existe, cada plantilla está en el editor,
     y las tablas de traducción de Retiros / Tickets / Accesos / Técnicos / Transporte caen en claves registradas.
  2. COBERTURA: TODA llamada de envío del proyecto (todos los .py, salvo tests) lleva `correo=` con una clave registrada, o está en
     la lista de excepciones con su razón; el `modulo=` de cada llamada es la llave del registro.
  3. La PUERTA ÚNICA (`_send_ilus_email` de app.py, función real): general > llave del módulo > interruptor del correo > cola;
     sin fila = encendido; clave desconocida sale; un correo protegido nunca se frena; base caída = último valor conocido.
  4. La COLA: la clave viaja en su columna (nunca en kwargs_json), se vuelve a revisar al drenar, y sin la columna igual se encola.
  5. La MIGRACIÓN: barata (REGLA #18): la segunda corrida no ejecuta ningún DDL.
  6. REINTENTAR respeta el interruptor.

Sin BD ni Flask: las funciones de app.py se extraen con `ast` (importar app.py exige BD y credenciales) y se ejecutan en un ambiente de
mentira. Correr con:  py -m unittest tests.test_comm_control_correos
"""
import ast
import json
import os
import re
import types
import unittest

import comm_correos_registro as R
import ot_correos_tecnicos as OTC
from tests import _fuente_codigo as FC

RAIZ = FC.RAIZ


def _leer(rel):
    return FC.texto(rel)


def _extraer(amb, funcs=(), consts=()):
    """Ejecuta en `amb` las funciones y constantes de nivel módulo de app.py indicadas (sin decoradores)."""
    return FC.extraer(amb, "app.py", funcs, consts)


def _llaves_validas():
    """`_MODULOS_KILLSWITCH_VALIDOS` leído del código de app.py."""
    return tuple(FC.valor_literal("app.py", "_MODULOS_KILLSWITCH_VALIDOS"))

# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  1) EL REGISTRO
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestRegistro(unittest.TestCase):
    def test_el_registro_se_valida_solo(self):
        self.assertEqual(R.validar(_llaves_validas()), [])

    def test_claves_unicas_y_campos_completos(self):
        claves = [c["clave"] for c in R.REGISTRO]
        self.assertEqual(len(claves), len(set(claves)))
        for c in R.REGISTRO:
            for campo in ("clave", "nombre", "modulo_ui", "llave", "destinatario", "disparo", "modo", "tipo"):
                self.assertTrue(str(c[campo]).strip(), f"{c['clave']}: falta {campo}")

    def test_la_llave_de_cada_correo_existe_incluida_ot_tecnicos(self):
        validas = _llaves_validas()
        self.assertIn("ot_tecnicos", validas)
        for c in R.REGISTRO:
            self.assertIn(c["llave"], validas, c["clave"])
        self.assertEqual({c["llave"] for c in R.REGISTRO if c["clave"].startswith("ot_tec_")}, {"ot_tecnicos"})

    def test_el_destinatario_es_un_valor_conocido_y_responsable_ot_es_valido(self):
        self.assertIn("responsable_ot", R.DESTINATARIOS)
        self.assertIn("bodega", R.DESTINATARIOS)
        self.assertEqual(set(R.DESTINATARIOS_ORDEN), set(R.DESTINATARIOS))
        for c in R.REGISTRO:
            self.assertIn(c["destinatario"], R.DESTINATARIOS, c["clave"])
        # el correo a bodega de Retiros se identifica como «bodega»
        self.assertEqual(R.por_clave("ret_aviso_bodega")["destinatario"], "bodega")

    def test_las_ordenes_de_trabajo_forman_su_propio_grupo_separado(self):
        grupos = {c["modulo_ui"] for c in R.listar()}
        self.assertIn("ot", grupos)
        self.assertEqual({"ot", "tickets", "mantenciones", "retiros", "transporte", "cotizaciones", "prospectos",
                          "repuestos", "catalogo", "accesos"}, grupos)
        for k in ("ot_creada_cliente", "ot_tec_asignada", "ot_tec_agenda_manana", "ot_pendiente_aprobacion",
                  "ot_informe_servicio", "ot_anexo_envio", "ot_acta_recepcion"):
            self.assertEqual(R.por_clave(k)["modulo_ui"], "ot", k)
        for k in ("tk_creacion", "tk_asignacion"):
            self.assertEqual(R.por_clave(k)["modulo_ui"], "tickets", k)
        for k in ("mant_plan_propuesto", "mant_recordatorio_visita"):
            self.assertEqual(R.por_clave(k)["modulo_ui"], "mantenciones", k)

    def test_los_tres_correos_de_seguridad_son_protegidos_y_solo_ellos(self):
        self.assertEqual({c["clave"] for c in R.REGISTRO if c["protegido"]},
                         {"acceso_olvido_clave", "acceso_invitacion", "acceso_cambio_clave"})

    def test_las_autorizaciones_son_sensibles(self):
        self.assertTrue(R.por_clave("ot_autorizacion_pendiente")["sensible"])
        self.assertTrue(R.por_clave("ot_pendiente_aprobacion")["sensible"])

    def test_los_cuatro_avisos_de_terceros_y_las_dos_herramientas(self):
        self.assertEqual(set(R.claves(tipo="externo")),
                         {"ext_fedex_notif_envio", "ext_fedex_notif_retiro", "ext_fedex_export_email", "ext_simpliroute_contacto"})
        self.assertEqual(set(R.HERRAMIENTAS), {"comm_prueba_manual", "comm_reenviar_historial"})
        # las herramientas no son tarjetas
        self.assertFalse(set(R.HERRAMIENTAS) & {c["clave"] for c in R.listar()})

    def test_cada_plantilla_del_registro_existe_en_el_editor_de_comunicaciones(self):
        js = _leer("static/comunicaciones_index.js")
        listas = {"transporte": "TPL_ESTADOS_TRANSPORTE", "transporte_cotizaciones": "TPL_ESTADOS_COTIZACIONES_TRANSPORTE",
                  "retiros": "TPL_ESTADOS_RETIROS", "mantenciones": "TPL_ESTADOS_MANTENCIONES",
                  "prospectos": "TPL_ESTADOS_PROSPECTOS", "comunicacion_interna": "TPL_ESTADOS_COMUNICACION_INTERNA",
                  "tickets": "TPL_ESTADOS_TICKETS", "catalogo": "TPL_ESTADOS_CATALOGO"}
        bloques = {}
        for modulo, nombre in listas.items():
            i = js.index(f"const {nombre} = [")
            bloques[modulo] = js[i:js.index("\n];", i)]
        for c in R.REGISTRO:
            if c["plantilla"] is None:
                continue
            modulo, estado = c["plantilla"]
            self.assertIn(modulo, bloques, f"{c['clave']}: módulo de plantilla {modulo}")
            self.assertRegex(bloques[modulo], r"key:\s*'" + re.escape(estado) + "'",
                             f"{c['clave']}: la plantilla {modulo}/{estado} no está en el editor")

    def test_clave_invalida_o_repetida_se_detecta(self):
        malo = R.REGISTRO + (dict(R.REGISTRO[0]),)
        original = R.REGISTRO
        try:
            R.REGISTRO = malo
            problemas = R.validar()
        finally:
            R.REGISTRO = original
        self.assertTrue(any("repetida" in p for p in problemas))


class TestTablasDeTraduccion(unittest.TestCase):
    """Cada tabla «evento/estado/kind → clave» del código cae en claves registradas, con la llave y la plantilla correctas."""

    def test_avisos_a_tecnicos(self):
        self.assertEqual(set(OTC.CORREO_DE), set(OTC.EVENTOS))
        for ev, clave in OTC.CORREO_DE.items():
            c = R.por_clave(clave)
            self.assertIsNotNone(c, clave)
            self.assertEqual(c["llave"], OTC.MODULO_LLAVE)
            self.assertEqual(c["plantilla"], (OTC.MODULO_PLANTILLAS, OTC.PLANTILLA_DE[ev]), clave)
        agenda = R.por_clave(OTC.CORREO_AGENDA)
        self.assertEqual(agenda["plantilla"], (OTC.MODULO_PLANTILLAS, OTC.PLANTILLA_AGENDA))
        self.assertEqual(agenda["llave"], OTC.MODULO_LLAVE)
        self.assertEqual(R.por_clave(OTC.CORREO_VISITA_COPIA)["llave"], OTC.MODULO_LLAVE)
        # una plantilla apagada en el editor hace que NO salga (así lo hace _ot_tec_plantilla_apagada)
        for clave in list(OTC.CORREO_DE.values()) + [OTC.CORREO_AGENDA]:
            self.assertEqual(R.por_clave(clave)["plantilla_apagada"], "no_sale", clave)

    def test_retiros_kind_a_clave(self):
        est = FC.valor_literal_anidado("pickups_module.py", "_KIND_TO_ESTADO")
        cor = FC.valor_literal_anidado("pickups_module.py", "_KIND_TO_CORREO")
        self.assertEqual(set(est), set(cor), "todo kind de notify() necesita su clave")
        for kind, clave in cor.items():
            c = R.por_clave(clave)
            self.assertIsNotNone(c, f"{kind}: {clave}")
            self.assertEqual(c["llave"], "retiros", clave)
            if est[kind]:
                self.assertEqual(c["plantilla"], ("retiros", est[kind]), f"{kind}: {clave}")
            else:
                self.assertIsNone(c["plantilla"], f"{kind}: {clave}")

    def test_tickets_slug_a_clave(self):
        defaults = set(FC.valor_literal_anidado("tickets_module.py", "_TK_LIFECYCLE_DEFAULTS", solo_claves=True))
        corr = FC.valor_literal_anidado("tickets_module.py", "_TK_CORREO_DE_SLUG")
        self.assertEqual(defaults, set(corr), "todo estado del ciclo de vida necesita su clave")
        for slug, clave in corr.items():
            c = R.por_clave(clave)
            self.assertIsNotNone(c, clave)
            self.assertEqual(c["llave"], "tickets")
            self.assertEqual(c["plantilla"], ("tickets", slug), clave)

    def test_accesos_plantilla_a_clave(self):
        tabla = FC.valor_literal("app.py", "_ACCESO_CORREO_DE_PLANTILLA")
        self.assertEqual(set(tabla), {"usuario_nuevo", "cambio_clave", "olvido_contrasena"})
        for estado, clave in tabla.items():
            c = R.por_clave(clave)
            self.assertTrue(c["protegido"], clave)
            self.assertEqual(c["plantilla"], ("comunicacion_interna", estado), clave)
            self.assertEqual(c["llave"], "comunicacion_interna")

    def test_transporte_los_seis_estados_se_resuelven_por_el_evento(self):
        """`_tr_notificar_cliente` NO se toca (otras 3 pruebas la comparan con origin/main): su evento `tracking_<estado>` se
        resuelve a la clave con `inferir()`. Se cruza con el `_slug` (plantilla) que usa esa misma función."""
        fn = ast.parse(FC.funcion_top("app.py", "_tr_notificar_cliente")).body[0]
        slug = None
        for n in ast.walk(fn):
            if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name) and n.targets[0].id == "_slug":
                # {...}.get(estado, 'entregado')  → el dict de estado → plantilla
                slug = ast.literal_eval(n.value.func.value)
        # el evento se arma exactamente así en el código (si cambia, esta prueba avisa)
        self.assertIn("evento=f\"tracking_{estado.lower().replace(' ','_')}\"", FC.funcion_top("app.py", "_tr_notificar_cliente"))
        self.assertEqual(set(slug), {"En preparación", "Entregado a transporte", "En ruta", "Entregado", "Entrega fallida",
                                     "Problema"})
        esperado = {"En preparación": "tr_en_preparacion", "Entregado a transporte": "tr_despachado", "En ruta": "tr_en_ruta",
                    "Entregado": "tr_entregado", "Entrega fallida": "tr_entrega_fallida", "Problema": "tr_entrega_fallida"}
        for estado, plantilla in slug.items():
            evento = f"tracking_{estado.lower().replace(' ', '_')}"
            clave = R.inferir("transporte", evento)
            self.assertEqual(clave, esperado[estado], estado)
            c = R.por_clave(clave)
            self.assertEqual(c["llave"], "transporte")
            self.assertEqual(c["plantilla"], ("transporte", plantilla), estado)
    def test_inferir_no_adivina_lo_ambiguo(self):
        # Todo Retiros sale con evento «pickup_created» y «visita_agendada» sirve a tres correos distintos: sin clave explícita no
        # se adivina nada (sale con el general y la llave, como hoy).
        self.assertIsNone(R.inferir("retiros", "pickup_created"))
        self.assertIsNone(R.inferir("mantenciones", "visita_agendada"))
        self.assertIsNone(R.inferir("comunicacion_interna", "password_access"))
        self.assertIsNone(R.inferir("general", None))
        self.assertEqual(R.inferir("tickets", "retry_ticket_resuelto"), "tk_resuelto")       # reintento del Historial
        self.assertEqual(R.inferir("tickets", "prueba_tk_creacion"), "tk_creacion")          # prueba de la pantalla
        self.assertIsNone(R.inferir("tickets", "prueba_clave_que_no_existe"))


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  2) COBERTURA: toda llamada de envío lleva una clave del registro
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
# Funciones que ENVUELVEN el envío y la llave de paso (`modulo=`) con que salen. `_send_ilus_email` toma su `modulo=` del argumento.
ENVOLTORIOS = {"_send_ilus_email": None, "_mant_send_email_multi": "mantenciones", "_send_pickup_email_multi": "retiros",
               "_prospecto_correo_por_ticket": "tickets", "_ot_tec_enviar": "ot_tecnicos"}

# Llamadas cuya clave NO es una constante en el sitio de la llamada. Cada una dice por qué es segura y qué otra prueba la cubre.
EXCEPCIONES_DINAMICAS = {
    ("app.py", "_ot_tec_despachar"):
        "correo=_otc.CORREO_DE[ev]: tabla de ot_correos_tecnicos (TestTablasDeTraduccion.test_avisos_a_tecnicos)",
    ("app.py", "_ot_tec_barrido_agenda"): "correo=_otc.CORREO_AGENDA: constante de ot_correos_tecnicos",
    ("app.py", "mant_visita_enviar_email"): "correo=_otc.CORREO_VISITA_COPIA: constante de ot_correos_tecnicos",
    ("app.py", "_send_password_access_email"):
        "correo=_clave_correo sale de _ACCESO_CORREO_DE_PLANTILLA (TestTablasDeTraduccion.test_accesos_plantilla_a_clave)",
    ("app.py", "comm_log_reintentar"):
        "Reintentar sale con la clave del correo ORIGINAL (email_log.correo_clave) — prueba TestReintentar",
    ("pickups_module.py", "notify"): "correo=_KIND_TO_CORREO.get(kind) (TestTablasDeTraduccion.test_retiros_kind_a_clave)",
    ("tickets_module.py", "_tk_notificar_lifecycle"):
        "correo=_TK_CORREO_DE_SLUG.get(estado_slug) (TestTablasDeTraduccion.test_tickets_slug_a_clave)",
}

# Llamadas que NO llevan `correo=` a propósito, con su razón.
EXCEPCIONES_SIN_CLAVE = {
    ("app.py", "_tr_notificar_cliente"):
        "su evento tracking_<estado> se resuelve con inferir() (test_transporte_los_seis_estados_se_resuelven_por_el_evento); "
        "la función no se toca: test_correo_cliente_no_miente_al_subir, test_manifiesto_sin_lineas_zz_confirmado y "
        "test_pantalla_despacho_semaforo la comparan con origin/main",
}


def _archivos_de_codigo():
    out = []
    for nombre in sorted(os.listdir(RAIZ)):
        if nombre.endswith(".py") and not nombre.startswith("test_"):
            out.append(nombre)
    return out


def _llamadas_de_envio():
    """[(archivo, linea, envoltorio, funcion_contenedora, params_de_la_funcion, kw {nombre: nodo})] de TODO el código.
    Se lee por TOKENS (tests/_fuente_codigo.py): el árbol completo de app.py pesa varios GB."""
    resultado = []
    for archivo in _archivos_de_codigo():
        for ll in FC.llamadas(archivo, set(ENVOLTORIOS)):
            kw = {}
            for nombre, valor in ll["kw"].items():
                try:
                    kw[nombre] = ast.parse(valor.strip(), mode="eval").body
                except SyntaxError:
                    kw[nombre] = ast.Name(id="<expresion no analizable: " + valor + ">", ctx=ast.Load())
            resultado.append((archivo, ll["linea"], ll["nombre"], ll["funcion"], ll["params"], kw))
    return resultado


class TestCobertura(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.llamadas = _llamadas_de_envio()

    def test_se_encontraron_las_llamadas_conocidas(self):
        # Si este número baja a casi cero el escáner dejó de ver el código y la prueba de abajo no estaría probando nada.
        self.assertGreater(len(self.llamadas), 55)
        archivos = {l[0] for l in self.llamadas}
        self.assertTrue({"app.py", "pickups_module.py", "tickets_module.py", "catalogo_module.py",
                         "logistica_cotizaciones.py"} <= archivos)

    def _constantes_de(self, nodo):
        """Todas las claves constantes que puede tomar `nodo` (str, o IfExp con str en ambas ramas); None si no es constante."""
        if isinstance(nodo, ast.Constant) and isinstance(nodo.value, str):
            return [nodo.value]
        if isinstance(nodo, ast.IfExp):
            a, b = self._constantes_de(nodo.body), self._constantes_de(nodo.orelse)
            return (a + b) if (a is not None and b is not None) else None
        return None

    def test_toda_llamada_lleva_una_clave_registrada_o_esta_en_las_excepciones(self):
        problemas = []
        for archivo, linea, envoltorio, fn, params, kw in self.llamadas:
            donde = f"{archivo}:{linea} {envoltorio}() en {fn}"
            if "correo" not in kw:
                if (archivo, fn) in EXCEPCIONES_SIN_CLAVE:
                    continue
                problemas.append(f"{donde}: falta correo= (agrégalo; REGLA #33 de CLAUDE.md)")
                continue
            valor = kw["correo"]
            claves = self._constantes_de(valor)
            if claves is not None:
                for k in claves:
                    if R.por_clave(k) is None:
                        problemas.append(f"{donde}: la clave {k!r} no está en el registro (comm_correos_registro.py)")
                continue
            if isinstance(valor, ast.Name) and valor.id in params:
                continue                                  # el envoltorio la recibe de quien lo llama
            if (archivo, fn) in EXCEPCIONES_DINAMICAS:
                continue
            problemas.append(f"{donde}: la clave no es una constante ({ast.unparse(valor)}); declárala en EXCEPCIONES_DINAMICAS "
                             f"con su razón o pásala como constante")
        self.assertEqual(problemas, [], "\n".join(problemas))

    def test_las_excepciones_siguen_existiendo(self):
        """Una excepción que ya no corresponde a ninguna llamada es una mentira en la lista."""
        vistas = {(a, fn) for a, _, _, fn, _, kw in self.llamadas}
        for (archivo, fn), razon in list(EXCEPCIONES_DINAMICAS.items()) + list(EXCEPCIONES_SIN_CLAVE.items()):
            self.assertIn((archivo, fn), vistas, f"{archivo}:{fn} ya no tiene llamadas de envío ({razon[:50]})")
            self.assertGreater(len(razon), 20)

    def test_el_modulo_de_cada_llamada_es_la_llave_del_registro(self):
        problemas = []
        for archivo, linea, envoltorio, fn, params, kw in self.llamadas:
            claves = self._constantes_de(kw["correo"]) if "correo" in kw else None
            if not claves:
                continue
            for k in claves:
                c = R.por_clave(k)
                if c is None:
                    continue
                if ENVOLTORIOS[envoltorio] is not None:
                    esperado = ENVOLTORIOS[envoltorio]
                elif "modulo" in kw and isinstance(kw["modulo"], ast.Constant):
                    esperado = kw["modulo"].value
                elif "modulo" not in kw and "**" in kw:
                    continue                         # el modulo viaja dentro de un **dict: no se ve en el sitio de la llamada
                elif "modulo" not in kw:
                    esperado = "general"             # sin modulo= ni evento conocido, la puerta lo manda a «general»
                else:
                    continue
                if c["llave"] != esperado:
                    problemas.append(f"{archivo}:{linea} {envoltorio}(): {k} sale por la llave {esperado!r} pero el registro dice "
                                     f"{c['llave']!r}")
        self.assertEqual(problemas, [], "\n".join(problemas))

    def test_ninguna_clave_ilus_del_registro_queda_sin_usar(self):
        """Un correo registrado que nada envía es una tarjeta que miente (se vería un interruptor que no apaga nada)."""
        fuentes = "\n".join(_leer(a) for a in _archivos_de_codigo() if a != "comm_correos_registro.py")
        inferibles = {"tr_en_preparacion", "tr_despachado", "tr_en_ruta", "tr_entregado", "tr_entrega_fallida"}   # por evento
        for c in R.listar("ilus"):
            k = c["clave"]
            if k in inferibles:
                continue
            self.assertRegex(fuentes, r'["\']' + re.escape(k) + r'["\']', f"{k}: ninguna llamada de envío usa esta clave")

    def test_el_envio_directo_no_se_hace_fuera_de_la_puerta(self):
        """Solo estos lugares llaman a `_send_ilus_email_real`: la puerta, el drenado de la cola y «Enviar prueba» (herramienta).
        Desde la fase 2 también «Enviarme una prueba» de Control de correos (`comm_correos_prueba`): un correo de muestra SOLO a quien está
        conectado; no puede pasar por la puerta porque la puerta lo frenaría con el interruptor del mismo correo que se está probando."""
        lugares = set()
        for archivo in _archivos_de_codigo():
            for ll in FC.llamadas(archivo, {"_send_ilus_email_real"}):
                lugares.add((archivo, ll["funcion"]))
        base = {("app.py", "_send_ilus_email"), ("app.py", "_email_queue_send_job"), ("app.py", "_bg_send_email")}
        self.assertLessEqual(base, lugares, lugares)
        self.assertLessEqual(lugares, base | {("app.py", "comm_correos_prueba")}, lugares)

# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  3) LA PUERTA ÚNICA (funciones REALES de app.py en un ambiente de mentira)
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
FUNCS_PUERTA = ("_correo_ctrl_invalidar", "_correo_ctrl_estado", "_correo_resolver_clave", "_correo_permitido",
                "_correo_bloqueo_previo", "_email_marcar_bloqueo", "_email_ultimo_bloqueo", "_email_motivo_bloqueo_txt",
                "_email_resultado", "_modulo_desde_evento", "_send_ilus_email", "_email_queue_send_job")
CONSTS_PUERTA = ("_CORREO_CTRL_CACHE", "_CORREO_CTRL_TTL", "_CORREO_AVISADOS", "_KS_MODULO_TXT")


class _Cursor:
    def __init__(self, mundo):
        self.m = mundo

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        self.m.sql.append((sql, params))
        if "INSERT INTO comm_email_jobs" in sql:
            if not self.m.columna_cola and "correo_clave" in sql:
                raise RuntimeError("(1054, \"Unknown column 'correo_clave' in 'field list'\")")
            if not self.m.columna_ref and "ref_json" in sql:
                raise RuntimeError("(1054, \"Unknown column 'ref_json' in 'field list'\")")
            self.m.cola.append((sql, params))


class _Conn:
    def __init__(self, mundo):
        self.m = mundo

    def cursor(self):
        return _Cursor(self.m)

    def commit(self):
        pass


class Mundo:
    """Base de mentira (comm_correo_control + cola) y correo de mentira. La puerta y la cola son las funciones REALES."""

    def __init__(self, filas=None, global_on=True, llaves_cerradas=(), columna_cola=True, columna_ref=True):
        self.filas = dict(filas or {})          # clave → activo (0/1), como en comm_correo_control
        self.global_on = global_on
        self.llaves_cerradas = set(llaves_cerradas)
        self.columna_cola = columna_cola
        self.columna_ref = columna_ref
        self.db_cae = False
        self.lecturas = 0
        self.salieron, self.logs, self.sql, self.cola, self.avisos = [], [], [], [], []
        self.jobs_estado = []
        amb = {"json": json, "print": lambda *a, **k: self.avisos.append(" ".join(str(x) for x in a)),
               "g": types.SimpleNamespace(), "_ccr": R,
               "mysql_fetchall": self._fetchall, "mysql_execute": lambda q, p=(): self.jobs_estado.append((q, p)),
               "comm_is_enabled": lambda canal: self.global_on,
               "_modulo_canal_bloqueado": lambda mod, canal: mod in self.llaves_cerradas,
               "_email_log": lambda *a, **k: self.logs.append((a, k)) or 1,
               "_email_log_norm_ref": lambda ref, asunto: (ref.get("tipo"), ref.get("id"), ref.get("codigo")),
               "get_db": lambda: _Conn(self), "_email_queue_kick": lambda: None,
               "_email_adjuntos_para_cola": lambda adj: None, "_email_adjuntos_de_cola": lambda adj: adj,
               "_send_ilus_email_real": self._real}
        _extraer(amb, FUNCS_PUERTA, CONSTS_PUERTA)
        self.amb = amb

    def _fetchall(self, sql, params=()):
        self.lecturas += 1
        if self.db_cae:
            raise RuntimeError("la base no responde")
        return [{"clave": k, "activo": a, "motivo": "", "actualizado_por": "Daniel", "actualizado_en": None}
                for k, a in self.filas.items()]

    def _real(self, to, asunto, html, **kw):
        self.salieron.append({"to": to, "asunto": asunto, **kw})
        return True

    def enviar(self, **kw):
        kw.setdefault("modulo", "retiros")
        return self.amb["_send_ilus_email"]("cliente@ejemplo.cl", "Asunto", "<p>x</p>", **kw)

    def estados_log(self):
        return [(k.get("correo"), a[3]) for a, k in self.logs]       # (clave, estado)


class TestOrdenDeRevision(unittest.TestCase):
    """general > llave del módulo > interruptor del correo > cola. Nada de esto llama jamás al envío real."""

    def test_correo_apagado_no_llama_al_envio_real_y_queda_bloqueado_con_su_motivo(self):
        m = Mundo(filas={"ret_cita_confirmada": 0})
        ok = m.enviar(evento="pickup_created", correo="ret_cita_confirmada")
        self.assertFalse(ok)
        self.assertEqual(m.salieron, [])
        (args, kw), = m.logs
        self.assertEqual(args[3], "bloqueado")
        self.assertEqual(kw["correo"], "ret_cita_confirmada")
        self.assertIn("Apagado en Control de correos", args[4] if len(args) > 4 else kw["error_msg"])
        self.assertIn("Cita de retiro confirmada", kw["error_msg"])
        self.assertEqual(kw["metadata"], {"modulo": "retiros", "bloqueo": "correo", "correo": "ret_cita_confirmada"})
        self.assertEqual(kw["html"], "<p>x</p>")            # el cuerpo se guarda, como con las llaves
        r = m.amb["_email_resultado"](ok)
        self.assertEqual((r["causa"], r["correo"]), ("apagado", "ret_cita_confirmada"))
        self.assertIn("«Cita de retiro confirmada» está apagado en Comunicaciones → Control de correos", r["mensaje"])

    def test_el_interruptor_general_manda_sobre_todo(self):
        m = Mundo(global_on=False, filas={"ret_cita_confirmada": 0})
        self.assertFalse(m.enviar(correo="ret_cita_confirmada"))
        self.assertEqual(m.salieron, [])
        (args, kw), = m.logs
        self.assertEqual(args[3], "bloqueado")
        self.assertIn("kill switch global", kw["error_msg"])
        self.assertEqual(kw["correo"], "ret_cita_confirmada")                 # igual cuenta como frenado de ese correo
        self.assertEqual(m.amb["_email_ultimo_bloqueo"]()["tipo"], "global")

    def test_la_llave_del_modulo_va_antes_que_el_interruptor_del_correo(self):
        m = Mundo(llaves_cerradas={"retiros"}, filas={"ret_cita_confirmada": 0})
        self.assertFalse(m.enviar(correo="ret_cita_confirmada"))
        self.assertEqual(m.salieron, [])
        self.assertEqual(m.amb["_email_ultimo_bloqueo"]()["tipo"], "modulo")
        self.assertIn("Llave de paso cerrada", m.logs[0][1]["error_msg"])

    def test_con_todo_abierto_sale_y_queda_enviado_con_su_clave(self):
        m = Mundo()
        self.assertTrue(m.enviar(correo="ret_cita_confirmada", evento="pickup_created"))
        self.assertEqual(len(m.salieron), 1)
        self.assertNotIn("correo", m.salieron[0])                 # la clave NUNCA llega al envío real (no la conoce)
        self.assertEqual(m.estados_log(), [("ret_cita_confirmada", "enviado")])
        self.assertIsNone(m.amb["_email_ultimo_bloqueo"]())

    def test_un_correo_apagado_no_frena_a_los_demas(self):
        m = Mundo(filas={"ret_cita_confirmada": 0})
        self.assertFalse(m.enviar(correo="ret_cita_confirmada"))
        self.assertTrue(m.enviar(correo="ret_propuesta"))
        self.assertTrue(m.enviar(correo="ret_en_preparacion"))
        self.assertEqual(len(m.salieron), 2)

    def test_el_bloqueo_de_un_correo_no_contamina_al_siguiente(self):
        m = Mundo(filas={"ret_cita_confirmada": 0})
        m.enviar(correo="ret_cita_confirmada")
        self.assertEqual(m.amb["_email_ultimo_bloqueo"]()["tipo"], "correo")
        m.enviar(correo="ret_propuesta")
        self.assertIsNone(m.amb["_email_ultimo_bloqueo"]())


class TestPorDefecto(unittest.TestCase):
    def test_sin_fila_en_la_tabla_todos_los_correos_salen(self):
        m = Mundo()
        for c in R.listar():
            self.assertTrue(m.enviar(modulo=c["llave"], correo=c["clave"]), c["clave"])
        self.assertEqual(len(m.salieron), len(R.listar()))
        self.assertEqual({l[1]["correo"] for l in m.logs}, {c["clave"] for c in R.listar()})

    def test_clave_desconocida_sale_y_deja_un_aviso(self):
        m = Mundo(filas={"clave_que_no_existe": 0})
        self.assertTrue(m.enviar(correo="clave_que_no_existe", evento="algo"))
        self.assertTrue(any("clave desconocida" in a and "clave_que_no_existe" in a for a in m.avisos), m.avisos)
        self.assertIsNone(m.logs[0][1]["correo"])                  # no se registra una clave que no existe

    def test_sin_clave_y_sin_evento_conocido_sale_con_las_llaves_de_siempre_y_avisa_una_vez(self):
        m = Mundo()
        self.assertTrue(m.enviar(evento="pickup_created"))
        self.assertTrue(m.enviar(evento="pickup_created"))
        self.assertEqual(sum("sin clave evento=pickup_created" in a for a in m.avisos), 1)       # un aviso por evento y proceso
        self.assertEqual(len(m.salieron), 2)

    def test_sin_clave_se_deduce_del_evento_cuando_hay_una_sola_lectura(self):
        """Así salen los seis estados de Transporte (`_tr_notificar_cliente` no se toca)."""
        m = Mundo(filas={"tr_entrega_fallida": 0})
        self.assertFalse(m.enviar(modulo="transporte", evento="tracking_problema"))
        self.assertFalse(m.enviar(modulo="transporte", evento="tracking_entrega_fallida"))
        self.assertTrue(m.enviar(modulo="transporte", evento="tracking_en_ruta"))
        self.assertEqual(m.estados_log(), [("tr_entrega_fallida", "bloqueado"), ("tr_entrega_fallida", "bloqueado"),
                                           ("tr_en_ruta", "enviado")])

    def test_un_correo_protegido_sale_aunque_su_fila_diga_apagado(self):
        m = Mundo(filas={"acceso_olvido_clave": 0, "acceso_invitacion": 0, "acceso_cambio_clave": 0})
        for k in ("acceso_olvido_clave", "acceso_invitacion", "acceso_cambio_clave"):
            self.assertTrue(m.enviar(modulo="comunicacion_interna", correo=k, evento="password_access"), k)
        self.assertEqual(len(m.salieron), 3)

    def test_un_correo_protegido_igual_respeta_el_interruptor_general_y_su_llave(self):
        """Hoy y mañana: la seguridad no se salta el general ni la llave (como hoy). La pantalla lo avisa en ámbar."""
        m = Mundo(global_on=False)
        self.assertFalse(m.enviar(modulo="comunicacion_interna", correo="acceso_olvido_clave"))
        m = Mundo(llaves_cerradas={"comunicacion_interna"})
        self.assertFalse(m.enviar(modulo="comunicacion_interna", correo="acceso_olvido_clave"))

    def test_la_clave_es_un_parametro_propio_y_nunca_viaja_en_los_kwargs(self):
        """`correo` es keyword-only de `_send_ilus_email` (si fuera a **kwargs se reenviaría a _send_ilus_email_real, que no lo
        conoce → TypeError) y la cola la guarda en su columna, no en kwargs_json."""
        fn = ast.parse(FC.funcion_top("app.py", "_send_ilus_email")).body[0]
        self.assertIn("correo", [a.arg for a in fn.args.kwonlyargs])
        self.assertEqual([a.arg for a in fn.args.args], ["to_addr", "subject", "html_body"])      # la firma posicional no cambió
        for n in ast.walk(fn):
            if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "_send_ilus_email_real":
                self.assertNotIn("correo", [k.arg for k in n.keywords])
            if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "kw_persistir" for t in n.targets):
                self.assertNotIn("correo", ast.unparse(n))              # la tupla de claves que sí viajan en kwargs_json


class TestBaseCaida(unittest.TestCase):
    def test_si_la_lectura_falla_se_usa_el_ultimo_valor_conocido(self):
        m = Mundo(filas={"ret_cita_confirmada": 0})
        self.assertFalse(m.enviar(correo="ret_cita_confirmada"))                 # lee y recuerda: apagado
        m.db_cae = True
        m.amb["_CORREO_CTRL_CACHE"]["ts"] -= 100                                 # vence el caché: tiene que volver a leer
        self.assertFalse(m.enviar(correo="ret_cita_confirmada"), "con la base caída debe seguir apagado")
        self.assertTrue(m.enviar(correo="ret_propuesta"))
        self.assertTrue(any("último valor conocido" in a for a in m.avisos))

    def test_si_nunca_se_leyo_y_la_base_esta_caida_el_correo_sale(self):
        m = Mundo(filas={"ret_cita_confirmada": 0})
        m.db_cae = True
        self.assertTrue(m.enviar(correo="ret_cita_confirmada"))                  # igual que las llaves: no se frena por un error técnico
        self.assertEqual(len(m.salieron), 1)

    def test_el_estado_se_guarda_30_segundos(self):
        m = Mundo(filas={"ret_cita_confirmada": 0})
        for _ in range(5):
            m.enviar(correo="ret_cita_confirmada")
        self.assertEqual(m.lecturas, 1)
        m.amb["_CORREO_CTRL_CACHE"]["ts"] -= 31
        m.enviar(correo="ret_cita_confirmada")
        self.assertEqual(m.lecturas, 2)
        m.amb["_correo_ctrl_invalidar"]()                                         # lo que hace el PUT de la pantalla
        m.filas["ret_cita_confirmada"] = 1
        self.assertTrue(m.enviar(correo="ret_cita_confirmada"))


class TestCola(unittest.TestCase):
    def test_se_encola_encendido_con_la_clave_en_su_columna_y_no_en_kwargs_json(self):
        m = Mundo()
        ok = m.enviar(correo="ot_tec_asignada", modulo="ot_tecnicos", asincrono=True, cc="a@b.cl", evento="ot_asignada")
        self.assertTrue(ok)
        self.assertEqual(m.salieron, [])                                          # no se envió síncrono
        (sql, params), = m.cola
        self.assertIn("correo_clave", sql)
        self.assertIn("ot_tec_asignada", params)
        kwargs_json = next(p for p in params if isinstance(p, str) and p.startswith("{"))
        self.assertNotIn("correo", json.loads(kwargs_json))                       # provocaría TypeError al drenar
        self.assertEqual(json.loads(kwargs_json), {"cc": "a@b.cl"})

    def test_apagado_antes_de_encolar_ni_se_encola(self):
        m = Mundo(filas={"ot_tec_asignada": 0})
        self.assertFalse(m.enviar(correo="ot_tec_asignada", modulo="ot_tecnicos", asincrono=True))
        self.assertEqual(m.cola, [])
        self.assertEqual(m.estados_log(), [("ot_tec_asignada", "bloqueado")])

    def test_se_apaga_mientras_esperaba_en_la_cola_y_al_sacarlo_queda_bloqueado(self):
        m = Mundo()
        self.assertTrue(m.enviar(correo="ot_tec_asignada", modulo="ot_tecnicos", asincrono=True))
        (sql, params), = m.cola
        m.filas["ot_tec_asignada"] = 0
        m.amb["_correo_ctrl_invalidar"]()
        job = {"id": 9, "destinatario": "cliente@ejemplo.cl", "asunto": "Asunto", "cuerpo": "<p>x</p>",
               "evento": "ot_asignada", "modulo": "ot_tecnicos", "intentos": 1, "max_intentos": 5,
               "kwargs_json": None, "correo_clave": "ot_tec_asignada"}
        m.amb["_email_queue_send_job"](job)
        self.assertEqual(m.salieron, [])
        self.assertTrue(any("failed" in q and "Control de correos" in str(p) for q, p in m.jobs_estado), m.jobs_estado)
        ult = m.logs[-1]
        self.assertEqual(ult[0][3], "bloqueado")
        self.assertEqual(ult[1]["metadata"]["bloqueo"], "correo")
        self.assertEqual(ult[1]["correo"], "ot_tec_asignada")
        self.assertEqual(ult[1]["job_id"], 9)

    def test_encendido_el_job_sale_y_queda_enviado_con_su_clave(self):
        m = Mundo()
        job = {"id": 3, "destinatario": "a@b.cl", "asunto": "A", "cuerpo": "x", "evento": "ot_asignada",
               "modulo": "ot_tecnicos", "intentos": 1, "max_intentos": 5, "kwargs_json": '{"cc": "z@b.cl"}',
               "correo_clave": "ot_tec_asignada"}
        m.amb["_email_queue_send_job"](job)
        self.assertEqual(len(m.salieron), 1)
        self.assertEqual(m.salieron[0]["cc"], "z@b.cl")
        self.assertNotIn("correo", m.salieron[0])
        self.assertEqual(m.estados_log(), [("ot_tec_asignada", "enviado")])

    def test_un_job_anterior_a_la_columna_se_deduce_del_evento_o_sale_como_siempre(self):
        m = Mundo(filas={"ot_tec_asignada": 0})
        job = {"id": 4, "destinatario": "a@b.cl", "asunto": "A", "cuerpo": "x", "evento": "ot_asignada",
               "modulo": "ot_tecnicos", "intentos": 1, "max_intentos": 5, "kwargs_json": None}      # sin correo_clave
        m.amb["_email_queue_send_job"](job)
        self.assertEqual(m.salieron, [])                                                            # evento ot_asignada → ot_tec_asignada
        m2 = Mundo(filas={"ot_tec_asignada": 0})
        job2 = dict(job, id=5, evento="evento_raro")
        m2.amb["_email_queue_send_job"](job2)
        self.assertEqual(len(m2.salieron), 1)                                                       # sin lectura única: como siempre

    def test_sin_la_columna_correo_clave_igual_se_encola(self):
        m = Mundo(columna_cola=False)
        self.assertTrue(m.enviar(correo="ot_tec_asignada", modulo="ot_tecnicos", asincrono=True))
        (sql, params), = m.cola
        self.assertNotIn("correo_clave", sql)
        self.assertEqual(m.salieron, [])

    def test_sin_ninguna_columna_opcional_igual_se_encola(self):
        m = Mundo(columna_cola=False, columna_ref=False)
        self.assertTrue(m.enviar(correo="ot_tec_asignada", modulo="ot_tecnicos", asincrono=True,
                                 ref={"tipo": "OT", "id": 3, "codigo": "OT-3"}))
        (sql, params), = m.cola
        self.assertNotIn("correo_clave", sql)
        self.assertNotIn("ref_json", sql)

    def test_con_referencia_y_clave_van_las_dos_columnas(self):
        m = Mundo()
        self.assertTrue(m.enviar(correo="ot_tec_asignada", modulo="ot_tecnicos", asincrono=True,
                                 ref={"tipo": "OT", "id": 3, "codigo": "OT-3"}))
        (sql, params), = m.cola
        self.assertIn("ref_json", sql)
        self.assertIn("correo_clave", sql)

    def test_sin_la_columna_correo_pero_con_ref_se_conserva_la_referencia(self):
        m = Mundo(columna_cola=False)
        self.assertTrue(m.enviar(correo="ot_tec_asignada", modulo="ot_tecnicos", asincrono=True,
                                 ref={"tipo": "OT", "id": 3, "codigo": "OT-3"}))
        (sql, params), = m.cola
        self.assertIn("ref_json", sql)
        self.assertNotIn("correo_clave", sql)


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  4) ESTADO INICIAL: el día que se sube, nada cambia
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestEstadoInicial(unittest.TestCase):
    def test_con_la_tabla_vacia_solo_no_salen_los_que_ya_estaban_apagados_por_configuracion_o_codigo(self):
        hoy_apagados_por_config = {"ret_comprobante_firma", "mant_recordatorio_visita", "mant_factura_pendiente",
                                   "prospecto_oferta_auto_1", "prospecto_oferta_auto_2"}
        hoy_apagados_por_codigo = {"acceso_alerta_login"}
        for c in R.listar():
            e = R.estado_efectivo(c, environ={})
            if c["clave"] in hoy_apagados_por_config:
                self.assertEqual(e["efectivo"], "config", c["clave"])
                self.assertFalse(e["puede_encender"], c["clave"])
            elif c["clave"] in hoy_apagados_por_codigo:
                self.assertEqual(e["efectivo"], "codigo", c["clave"])
            else:
                self.assertEqual(e["efectivo"], "sale", c["clave"])

    def test_sin_RETIROS_FIRMA_CORREO_el_comprobante_sigue_sin_salir_y_con_ella_sale(self):
        c = R.por_clave("ret_comprobante_firma")
        self.assertEqual(R.estado_efectivo(c, environ={})["efectivo"], "config")
        self.assertEqual(R.estado_efectivo(c, environ={"RETIROS_FIRMA_CORREO": "true"})["efectivo"], "config")   # exacto «1»
        self.assertEqual(R.estado_efectivo(c, environ={"RETIROS_FIRMA_CORREO": "1"})["efectivo"], "sale")

    def test_lectura_de_cada_variable_igual_que_el_codigo_que_la_usa(self):
        def sale(clave, **env):
            return R.estado_efectivo(R.por_clave(clave), environ=env)["efectivo"] == "sale"
        # MANT_RECORDATORIOS_EMAIL_ENABLED: 1/true/yes/on, por defecto apagada
        self.assertFalse(sale("mant_recordatorio_visita"))
        for v in ("1", "true", "YES", " on "):
            self.assertTrue(sale("mant_recordatorio_visita", MANT_RECORDATORIOS_EMAIL_ENABLED=v), v)
        self.assertFalse(sale("mant_recordatorio_visita", MANT_RECORDATORIOS_EMAIL_ENABLED="0"))
        # PROSPECTOS_OFERTA_AUTO: 1/true/si/yes
        self.assertFalse(sale("prospecto_oferta_auto_1"))
        self.assertTrue(sale("prospecto_oferta_auto_1", PROSPECTOS_OFERTA_AUTO="si"))
        self.assertFalse(sale("prospecto_oferta_auto_1", PROSPECTOS_OFERTA_AUTO="on"))

    def test_las_condiciones_de_solo_el_disparo_automatico_no_apagan_el_correo(self):
        """RETIROS_PREP_AUTO / RETIROS_RETIRO_AUTO solo condicionan el envío automático: el manual sigue funcionando."""
        e = R.estado_efectivo(R.por_clave("ret_en_preparacion"), environ={"RETIROS_PREP_AUTO": "0"})
        self.assertEqual(e["efectivo"], "sale")
        self.assertTrue(e["puede_encender"])
        self.assertTrue(any(x["variable"] == "RETIROS_PREP_AUTO" and not x["activa"] for x in e["condiciones"]))
        e = R.estado_efectivo(R.por_clave("ret_retiro_completado"), environ={})
        self.assertEqual(e["efectivo"], "sale")
        self.assertTrue(any(x["variable"] == "RETIROS_RETIRO_AUTO" and not x["activa"] for x in e["condiciones"]))

    def test_los_pasos_se_evaluan_en_el_orden_de_la_puerta(self):
        c = R.por_clave("ret_cita_confirmada")
        e = R.estado_efectivo(c, general_activo=False, llave_bloqueada=True, switch_activo=False)
        self.assertEqual((e["efectivo"], e["frena_en"]), ("general", 1))
        self.assertEqual([p["estado"] for p in e["pasos"]], ["apagado", "saltado", "saltado", "saltado"])
        e = R.estado_efectivo(c, llave_bloqueada=True, switch_activo=False)
        self.assertEqual((e["efectivo"], e["frena_en"]), ("llave", 2))
        e = R.estado_efectivo(c, switch_activo=False)
        self.assertEqual((e["efectivo"], e["frena_en"]), ("correo", 3))
        self.assertEqual([p["estado"] for p in e["pasos"]], ["ok", "ok", "apagado", "saltado"])
        e = R.estado_efectivo(c)
        self.assertEqual((e["efectivo"], e["frena_en"]), ("sale", 0))

    def test_un_protegido_muestra_candado_y_no_se_puede_apagar_ni_aunque_diga_apagado(self):
        c = R.por_clave("acceso_olvido_clave")
        e = R.estado_efectivo(c, switch_activo=False)           # una fila «apagada» NO lo frena
        self.assertEqual(e["efectivo"], "sale")
        self.assertEqual(e["pasos"][2]["estado"], "protegido")
        self.assertFalse(e["puede_apagar"])
        self.assertFalse(e["puede_encender"])


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  5) REINTENTAR
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
class TestReintentar(unittest.TestCase):
    def _mundo(self, fila_log, **kw):
        m = Mundo(**kw)
        _extraer(m.amb, ("comm_log_reintentar",))
        m.amb.update({"mysql_fetchone": lambda sql, p=(): dict(fila_log), "jsonify": lambda d: d,
                      "_email_log_cuerpo_leer": lambda lid: ("<p>guardado</p>", 0),
                      "_comm_render_email_document": lambda t, b, s="": b})
        return m

    FILA = {"id": 7, "destinatario": "cliente@ejemplo.cl", "asunto": "Cita confirmada", "evento": "pickup_created",
            "modulo": "retiros", "metadata": "{}", "correo_clave": "ret_cita_confirmada", "ref_tipo": None, "ref_codigo": None,
            "ref_id": None}

    def test_el_reintento_sale_con_la_clave_original_y_si_ese_correo_esta_apagado_no_sale(self):
        m = self._mundo(self.FILA, filas={"ret_cita_confirmada": 0})
        r = m.amb["comm_log_reintentar"](7)
        self.assertFalse(r["ok"])
        self.assertEqual(r["estado"], "bloqueado")
        self.assertEqual(r["bloqueo"], "correo")
        self.assertIn("Cita de retiro confirmada", r["motivo"])
        self.assertEqual(m.salieron, [])
        self.assertEqual(m.estados_log(), [("ret_cita_confirmada", "bloqueado")])

    def test_encendido_el_reintento_sale(self):
        m = self._mundo(self.FILA)
        r = m.amb["comm_log_reintentar"](7)
        self.assertTrue(r["ok"])
        self.assertEqual(r["estado"], "enviado")
        self.assertEqual(len(m.salieron), 1)
        self.assertEqual(m.estados_log(), [("ret_cita_confirmada", "enviado")])

    def test_una_fila_antigua_sin_clave_se_deduce_del_evento_retry(self):
        fila = dict(self.FILA, evento="ticket_resuelto", modulo="tickets", correo_clave=None)
        m = self._mundo(fila, filas={"tk_resuelto": 0})
        r = m.amb["comm_log_reintentar"](7)
        self.assertEqual(r["estado"], "bloqueado")
        self.assertEqual(r["bloqueo"], "correo")

    def test_la_llave_cerrada_sigue_bloqueando_el_reintento(self):
        m = self._mundo(self.FILA, llaves_cerradas={"retiros"})
        r = m.amb["comm_log_reintentar"](7)
        self.assertEqual((r["estado"], r["bloqueo"]), ("bloqueado", "modulo"))


# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  6) MIGRACIÓN BARATA (REGLA #18)
# ════════════════════════════════════════════════════════════════════════════════════════════════════════════
class _BaseSim:
    """information_schema de mentira: lo que existe se actualiza cuando un DDL se ejecuta."""

    def __init__(self, tablas=("email_log", "comm_email_jobs"), columnas=(), indices=()):
        self.tablas, self.columnas, self.indices = set(tablas), set(columnas), set(indices)
        self.ddl = []

    def fetchone(self, sql, params=()):
        if "information_schema.TABLES" in sql and "comm_correo_control" in sql:
            return {"x": 1} if "comm_correo_control" in self.tablas else None
        if "information_schema.STATISTICS" in sql:
            return {"x": 1} if "idx_correo_fecha" in self.indices else None
        return None

    def fetchall(self, sql, params=()):
        if "information_schema.COLUMNS" in sql:
            return [{"t": t} for t in ("email_log", "comm_email_jobs") if (t, "correo_clave") in self.columnas]
        if "information_schema.TABLES" in sql:
            return [{"t": t} for t in ("email_log", "comm_email_jobs") if t in self.tablas]
        return []

    def ejecutar(self, sql):
        self.ddl.append(sql)
        if sql.lstrip().upper().startswith("CREATE TABLE IF NOT EXISTS COMM_CORREO_CONTROL"):
            self.tablas.add("comm_correo_control")
        m = re.match(r"\s*ALTER TABLE (\w+) ADD COLUMN correo_clave", sql, re.I)
        if m:
            self.columnas.add((m.group(1), "correo_clave"))
        if re.match(r"\s*ALTER TABLE email_log ADD INDEX idx_correo_fecha", sql, re.I):
            self.indices.add("idx_correo_fecha")


class TestMigracion(unittest.TestCase):
    def _correr(self, sim):
        class Cur:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, sql, p=None):
                sim.ejecutar(sql)

        class Conexion:
            def cursor(self):
                return Cur()

            def commit(self):
                pass

            def close(self):
                pass

        amb = {"print": lambda *a, **k: None, "mysql_fetchone": sim.fetchone, "mysql_fetchall": sim.fetchall,
               "get_mysql": lambda: Conexion(), "_correo_ctrl_invalidar": lambda: None,
               "_EMAIL_LOG_TIENE_CORREO": {"v": True, "ts": 0}}
        _extraer(amb, ("_ensure_comm_correo_control",))
        return amb["_ensure_comm_correo_control"]()

    def test_la_primera_corrida_crea_lo_que_falta_y_la_segunda_no_ejecuta_ningun_ddl(self):
        sim = _BaseSim()
        hechos1 = self._correr(sim)
        self.assertEqual(len(sim.ddl), 4, sim.ddl)
        self.assertTrue(hechos1)
        antes = len(sim.ddl)
        hechos2 = self._correr(sim)
        self.assertEqual(len(sim.ddl), antes, "la segunda corrida ejecutó DDL: " + str(sim.ddl[antes:]))
        self.assertEqual(hechos2, [])

    def test_cada_sentencia_lleva_una_sola_clausula_y_la_tabla_no_se_siembra(self):
        sim = _BaseSim()
        self._correr(sim)
        for sql in sim.ddl:
            self.assertNotRegex(sql, r",\s*(ADD|MODIFY|CHANGE|DROP|ALTER|RENAME)\s", sql)       # REGLA #18: una sentencia, una cláusula
            self.assertNotRegex(sql, r"INSERT|UPDATE|DELETE", sql)                             # no hay relleno ni siembra
        crear = next(s for s in sim.ddl if s.upper().startswith("CREATE TABLE"))
        for col in ("clave VARCHAR(60) NOT NULL PRIMARY KEY", "activo TINYINT(1) NOT NULL DEFAULT 1", "motivo VARCHAR(300) NULL",
                    "actualizado_por VARCHAR(190) NULL", "actualizado_en DATETIME NULL"):
            self.assertIn(col, crear)

    def test_con_todo_aplicado_a_medias_solo_agrega_lo_que_falta(self):
        sim = _BaseSim(tablas=("email_log", "comm_email_jobs", "comm_correo_control"),
                       columnas=(("email_log", "correo_clave"),))
        self._correr(sim)
        self.assertEqual(len(sim.ddl), 2)
        self.assertTrue(any("comm_email_jobs ADD COLUMN correo_clave" in s for s in sim.ddl))
        self.assertTrue(any("ADD INDEX idx_correo_fecha (correo_clave, created_at)" in s for s in sim.ddl))

    def test_la_guardia_de_ddl_del_proyecto_tambien_reconoce_cada_sentencia_como_ya_aplicada(self):
        """Doble red: aunque la comprobación propia fallara, la guardia de `mysql_execute`/`get_mysql` salta lo ya hecho."""
        amb = {"re": re}
        _extraer(amb, ("_ddl_ya_aplicado",),
                 ("_DDL_RE", "_DDLG_MULTI", "_DDLG_CREATE_TBL", "_DDLG_ADD_IDX", "_DDLG_CREATE_IDX", "_DDLG_ADD_COL",
                  "_DDLG_MOD_ENUM", "_DDLG_ENUM_VAL", "_DDLG_NO_COLUMNA", "_ddl_guard_saltados"))
        sim = _BaseSim()
        self._correr(sim)
        for sql in sim.ddl:
            self.assertTrue(amb["_ddl_ya_aplicado"](lambda q, p: {"x": 1}, sql), f"la guardia no entiende: {sql}")


if __name__ == "__main__":
    unittest.main()
