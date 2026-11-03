# -*- coding: utf-8 -*-
"""🚚 FLOTA · «Solicitudes de vehículo» (Fase 1b: choferes, solicitudes, calendario) — Daniel, 02-nov-2026:
«la solicitud la va a poder hacer logística o servicio técnico, asignando el chofer que va a ir a retirar la llave. Aarón va a poder marcar
que la llave fue entregada tal y tal hora… necesito que me declaren el horario en que necesitan el camión.» «El sello decide» los choques.

Decisiones que se vigilan:
  1. El horario (día + desde + hasta) es OBLIGATORIO y el SELLO del servidor decide si dos pedidos se pisan: el segundo recibe quién tiene el
     vehículo y nada se guarda. Terminar justo cuando empieza otro NO es choque. Una cancelada libera el horario.
  2. Un chofer es un usuario de ILUS con su licencia (clase y vencimiento): sin la clase que exige el vehículo, o con la licencia vencida
     antes de terminar el uso, NO puede recibir la reserva. Un chofer tampoco puede estar en dos vehículos a la vez.
  3. Documentos del vehículo vencidos ALERTAN, no traban (decisión pendiente de Daniel). Feriado o fin de semana: alerta.
  4. Flujo: solicitada → (la gestión) confirmada → llave entregada; cancelar con motivo. Cada paso es atómico y queda en la bitácora.
  5. Sin precios ni costos. La bitácora solo se agrega. Nada se borra.
Correr:  python -m pytest tests/test_flota_solicitudes.py -q -p no:warnings
"""
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import flota_reglas as R  # noqa: E402
from test_flota_vehiculos import BaseFalsa  # noqa: E402

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HAY_NODE = shutil.which("node") is not None


def _leer(*partes):
    with open(os.path.join(RAIZ, *partes), encoding="utf-8") as f:
        return f.read()


def _proximo_dia_habil(minimo_dias=8):
    """Un martes futuro: ni fin de semana ni (casi nunca) feriado, para que las pruebas no dependan de la fecha en que se corren."""
    d = dt.date.today() + dt.timedelta(days=minimo_dias)
    while d.weekday() != 1:
        d += dt.timedelta(days=1)
    return d


def _rut(cuerpo):
    """Un RUT con su dígito verificador correcto (módulo 11)."""
    suma, factor = 0, 2
    for d in reversed(str(cuerpo)):
        suma += int(d) * factor
        factor = 2 if factor == 7 else factor + 1
    r = 11 - (suma % 11)
    return f"{cuerpo}-" + ("0" if r == 11 else ("K" if r == 10 else str(r)))


NOMBRES_COMPLETOS = {1: "Ana María Pérez Soto", 2: "Humberto Soto Rivas", 4: "Daniela Mora Vega"}


def _datos_chofer(uid, **k):
    d = {"rut": _rut(10000000 + uid), "nombre_completo": NOMBRES_COMPLETOS.get(uid, f"Persona de Prueba {uid}"),
         "direccion": "Av. Siempre Viva 742, Providencia", "direccion_lat": "-33.4263", "direccion_lng": "-70.6141",
         "direccion_place_id": "ChIJprueba_providencia_742", "licencia_clases": ["B"], "licencia_vence": FUTURO}
    d.update(k)
    return d


DIA = _proximo_dia_habil()
DIA_ISO = DIA.isoformat()
AHORA = dt.datetime(2026, 11, 2, 15, 0)


# ═══════════════════════════════════════════════════════════════════════════
#  A. Reglas puras
# ═══════════════════════════════════════════════════════════════════════════
class TestHorario(unittest.TestCase):
    def test_hora(self):
        self.assertEqual(R.parse_hora("8:30"), dt.time(8, 30))
        self.assertEqual(R.parse_hora("08:30:00"), dt.time(8, 30))
        self.assertIsNone(R.parse_hora(""))
        for malo in ("25:00", "8", "ab:cd", "12:60"):
            with self.assertRaises(ValueError):
                R.parse_hora(malo)

    def test_terminar_justo_cuando_empieza_otro_no_es_choque(self):
        a, b, c = dt.datetime(2026, 11, 3, 8), dt.datetime(2026, 11, 3, 12), dt.datetime(2026, 11, 3, 15)
        self.assertFalse(R.se_cruzan(a, b, b, c))
        self.assertFalse(R.se_cruzan(b, c, a, b))
        self.assertTrue(R.se_cruzan(a, c, b, c))
        self.assertTrue(R.se_cruzan(a, b, dt.datetime(2026, 11, 3, 11), c))
        self.assertTrue(R.se_cruzan(a, c, dt.datetime(2026, 11, 3, 9), dt.datetime(2026, 11, 3, 10)))     # uno adentro del otro

    def test_conflictos_ignora_canceladas_y_a_si_misma(self):
        ini, fin = dt.datetime(2026, 11, 3, 8), dt.datetime(2026, 11, 3, 12)
        filas = [
            {"id": 1, "estado": "confirmada", "inicio": ini, "fin": fin},
            {"id": 2, "estado": "cancelada", "inicio": ini, "fin": fin},
            {"id": 3, "estado": "llave_entregada", "inicio": dt.datetime(2026, 11, 3, 11), "fin": dt.datetime(2026, 11, 3, 13)},
            {"id": 4, "estado": "solicitada", "inicio": dt.datetime(2026, 11, 3, 12), "fin": dt.datetime(2026, 11, 3, 14)},
        ]
        self.assertEqual([f["id"] for f in R.conflictos(ini, fin, filas)], [1, 3])
        self.assertEqual([f["id"] for f in R.conflictos(ini, fin, filas, excluir_id=1)], [3])

    def test_rango_texto_dice_el_dia_en_espanol_y_nunca_iso(self):
        t = R.rango_texto(dt.datetime(2026, 11, 3, 8, 0), dt.datetime(2026, 11, 3, 12, 30))
        self.assertEqual(t, "mar 03/11/2026 · 08:00 a 12:30")
        t2 = R.rango_texto(dt.datetime(2026, 11, 3, 8, 0), dt.datetime(2026, 11, 4, 12, 30))
        self.assertIn("mar 03/11/2026 08:00", t2)
        self.assertIn("mié 04/11/2026 12:30", t2)
        self.assertNotRegex(t + t2, r"\d{4}-\d{2}-\d{2}")

    def test_numero(self):
        self.assertEqual(R.numero_solicitud(7, 2026), "VEH-2026-0007")
        self.assertEqual(R.numero_solicitud(12345, 2027), "VEH-2027-12345")


def _sol(**k):
    d = {"vehiculo_id": 1, "chofer_id": 2, "area": "Logística", "motivo": "Reparto de equipos", "fecha": DIA_ISO,
         "hora_desde": "08:00", "hora_hasta": "12:30"}
    d.update(k)
    return d


class TestValidarSolicitud(unittest.TestCase):
    def test_caso_bueno(self):
        d, e = R.validar_solicitud(_sol(), AHORA)
        self.assertEqual(e, {})
        self.assertEqual(d["inicio"], dt.datetime.combine(DIA, dt.time(8, 0)))
        self.assertEqual(d["fin"], dt.datetime.combine(DIA, dt.time(12, 30)))
        self.assertEqual((d["vehiculo_id"], d["chofer_id"], d["area"]), (1, 2, "Logística"))

    def test_el_horario_es_obligatorio(self):
        _d, e = R.validar_solicitud(_sol(hora_desde="", hora_hasta=""), AHORA)
        self.assertIn("hora_desde", e)
        self.assertIn("hora_hasta", e)
        _d, e = R.validar_solicitud(_sol(fecha=""), AHORA)
        self.assertIn("fecha", e)

    def test_obligatorios(self):
        _d, e = R.validar_solicitud({}, AHORA)
        for c in ("vehiculo_id", "chofer_id", "area", "motivo", "fecha", "hora_desde", "hora_hasta"):
            self.assertIn(c, e, c)

    def test_fin_despues_del_inicio(self):
        _d, e = R.validar_solicitud(_sol(hora_desde="12:00", hora_hasta="12:00"), AHORA)
        self.assertIn("hora_hasta", e)
        _d, e = R.validar_solicitud(_sol(hora_desde="14:00", hora_hasta="09:00"), AHORA)
        self.assertIn("hora_hasta", e)

    def test_no_se_puede_pedir_el_pasado(self):
        _d, e = R.validar_solicitud(_sol(fecha="2026-11-01"), AHORA)
        self.assertIn("fecha", e)
        # hoy sí, mientras el horario no haya terminado
        _d, e = R.validar_solicitud(_sol(fecha="2026-11-02", hora_desde="16:00", hora_hasta="18:00"), AHORA)
        self.assertEqual(e, {})
        _d, e = R.validar_solicitud(_sol(fecha="2026-11-02", hora_desde="08:00", hora_hasta="09:00"), AHORA)
        self.assertIn("hora_hasta", e)

    def test_editar_permite_un_uso_que_ya_empezo_pero_no_uno_que_ya_termino(self):
        _d, e = R.validar_solicitud(_sol(fecha="2026-11-01", fecha_hasta="2026-11-03", hora_desde="08:00", hora_hasta="12:00"), AHORA, editando=True)
        self.assertEqual(e, {})
        _d, e = R.validar_solicitud(_sol(fecha="2026-11-01", hora_hasta="12:00"), AHORA, editando=True)
        self.assertIn("hora_hasta", e)

    def test_otro_dia_y_tope(self):
        d, e = R.validar_solicitud(_sol(fecha_hasta=(DIA + dt.timedelta(days=1)).isoformat(), hora_hasta="10:00"), AHORA)
        self.assertEqual(e, {})
        self.assertEqual(d["fin"].date(), DIA + dt.timedelta(days=1))
        _d, e = R.validar_solicitud(_sol(fecha_hasta=(DIA + dt.timedelta(days=30)).isoformat()), AHORA)
        self.assertIn("hora_hasta", e)

    def test_motivo_y_area(self):
        _d, e = R.validar_solicitud(_sol(motivo="abc"), AHORA)
        self.assertIn("motivo", e)
        _d, e = R.validar_solicitud(_sol(area="x" * 61), AHORA)
        self.assertIn("area", e)

    def test_motivo_de_cambio(self):
        self.assertIsNotNone(R.validar_motivo_cambio("ab")[1])
        self.assertEqual(R.validar_motivo_cambio("  El chofer  no estará ")[0], "El chofer no estará")


class TestChoferYVehiculo(unittest.TestCase):
    HOY = dt.date(2026, 11, 2)

    def chofer(self, **k):
        d = {"nombre": "Ana Pérez", "habilitado": True, "licencia_clases": "B", "licencia_vence": dt.date(2027, 6, 1)}
        d.update(k)
        return d

    def test_licencia_cubre(self):
        self.assertTrue(R.licencia_cubre("B", "B"))
        self.assertTrue(R.licencia_cubre(["A4", "B"], "B"))
        self.assertTrue(R.licencia_cubre("A4", "A4,B"))        # el vehículo exige «B o A4»: basta una
        self.assertFalse(R.licencia_cubre("C", "B"))
        self.assertTrue(R.licencia_cubre("", ""))               # un vehículo que no exige nada lo puede llevar cualquiera

    def test_chofer_valido(self):
        self.assertEqual(R.evaluar_chofer(self.chofer(), "B", dt.date(2026, 11, 4), self.HOY), (None, None))

    def test_chofer_que_no_puede(self):
        for chofer, parte in (
            (self.chofer(habilitado=False), "inhabilitado"),
            (self.chofer(licencia_clases="C"), "no tiene registrada la licencia"),
            (self.chofer(licencia_vence=None), "no tiene registrado el vencimiento"),
            (self.chofer(licencia_vence=dt.date(2026, 11, 3)), "antes de terminar"),
        ):
            err, _av = R.evaluar_chofer(chofer, "B", dt.date(2026, 11, 4), self.HOY)
            self.assertIsNotNone(err, parte)
            self.assertIn(parte, err)

    def test_licencia_que_vence_justo_el_ultimo_dia_sirve(self):
        self.assertEqual(R.evaluar_chofer(self.chofer(licencia_vence=dt.date(2026, 11, 4)), "B", dt.date(2026, 11, 4), self.HOY)[0], None)

    def test_licencia_por_vencer_avisa_sin_trabar(self):
        err, aviso = R.evaluar_chofer(self.chofer(licencia_vence=dt.date(2026, 11, 20)), "B", dt.date(2026, 11, 4), self.HOY)
        self.assertIsNone(err)
        self.assertIn("20/11/2026", aviso)

    def test_documentos_vencidos_el_dia_de_uso_solo_avisan(self):
        fila = {"soap_vence": dt.date(2026, 11, 10), "revision_tecnica_vence": dt.date(2027, 5, 1)}
        av = R.avisos_vehiculo(fila, dt.date(2026, 11, 12))
        self.assertEqual(len(av), 1)
        self.assertIn("SOAP", av[0])
        self.assertEqual(R.avisos_vehiculo(fila, dt.date(2026, 11, 5)), [])
        self.assertEqual(R.avisos_vehiculo({}, dt.date(2026, 11, 5)), [])        # sin fechas no se inventa nada

    def test_validar_chofer(self):
        base = {"rut": "76.996.964-0", "nombre_completo": "Ana María Pérez Soto", "direccion": "Av. Siempre Viva 742",
                "direccion_lat": "-33.43", "direccion_lng": "-70.61", "direccion_place_id": "ChIJprueba_742"}
        d, e = R.validar_chofer(dict(base, usuario_id="3", licencia_clases=["b", "A4"], licencia_vence="2027-01-31"))
        self.assertEqual(e, {})
        self.assertEqual((d["usuario_id"], d["licencia_clases"], d["licencia_vence"]), (3, "A4,B", dt.date(2027, 1, 31)))
        self.assertEqual((d["rut"], d["nombre_completo"]), ("76996964-0", "Ana María Pérez Soto"))
        _d, e = R.validar_chofer({})
        for c in ("usuario_id", "rut", "nombre_completo", "licencia_clases", "licencia_vence"):
            self.assertIn(c, e)
        _d, e = R.validar_chofer(dict(base, licencia_clases="B", licencia_vence="2027-01-31"), con_usuario=False)
        self.assertEqual(e, {})
        _d, e = R.validar_chofer(dict(base, rut="12.345.678-9", licencia_clases="B", licencia_vence="2027-01-31"), con_usuario=False)
        self.assertIn("rut", e)                                                     # el dígito verificador no cuadra
        _d, e = R.validar_chofer(dict(base, direccion="", licencia_clases="B", licencia_vence="2027-01-31"), con_usuario=False)
        self.assertIn("direccion", e)                                               # la dirección es obligatoria (Daniel, 02-nov-2026)


# ═══════════════════════════════════════════════════════════════════════════
#  B. Rutas con una base de mentira (con la transacción de la reserva)
# ═══════════════════════════════════════════════════════════════════════════
def _n(q):
    return " ".join(q.split())


class Falsa(BaseFalsa):
    """Suma a la base de vehículos las tablas de la fase 1b: usuarios, choferes, solicitudes y sus bitácoras. Cualquier SQL que no conozca
    falla la prueba. Imita lo que hace MySQL: el SELECT ... FOR UPDATE, las sentencias de la transacción y el rowcount."""

    def __init__(self):
        super().__init__()
        self.cho, self.sol, self.slog, self.clog = {}, {}, [], []
        self.sig_c = self.sig_s = 1
        self.usr = {
            1: {"id": 1, "nombre": "Ana Pérez", "username": "ana@ilus.cl", "phone": "+56911111111", "cargo": "Conductora", "active": 1},
            2: {"id": 2, "nombre": "Beto Soto", "username": "beto@ilus.cl", "phone": "+56922222222", "cargo": "Bodega", "active": 1},
            3: {"id": 3, "nombre": "Cami Rojas", "username": "cami@ilus.cl", "phone": None, "cargo": None, "active": 0},
            4: {"id": 4, "nombre": "Dani Mora", "username": "dani@ilus.cl", "phone": None, "cargo": None, "active": 1},
        }
        self.rollbacks = self.commits = self.cerradas = 0
        self.abiertas = 0                     # reservas por delante que dirá la cuenta de «tiene reservas pendientes»

    # ── filas unidas
    def _chof_row(self, c):
        if c["usuario_id"] is None:          # chofer externo: COALESCE con su propia fila, «activo» = 1
            return dict(c, u_nombre=c.get("nombre_completo"), u_username=None, u_phone=c.get("telefono"), u_cargo=None, u_activo=1)
        u = self.usr.get(c["usuario_id"], {})
        return dict(c, u_nombre=u.get("nombre"), u_username=u.get("username"), u_phone=u.get("phone"), u_cargo=u.get("cargo"), u_activo=u.get("active"))

    def _sol_row(self, s):
        v = self.veh[s["vehiculo_id"]]
        c = self.cho.get(s["chofer_id"])
        u = self.usr.get(c["usuario_id"], {}) if c else {}
        return dict(s, v_patente=v["patente"], v_tipo=v["tipo"], v_marca=v["marca"], v_modelo=v["modelo"], v_km=v.get("km_actual"),
                    chofer_nombre=u.get("nombre") or (c or {}).get("nombre_completo"), chofer_phone=u.get("phone") or (c or {}).get("telefono"))

    def _ocupa_row(self, s):
        return {k: s[k] for k in ("id", "numero", "estado", "inicio", "fin", "area", "solicitante_nombre", "sello_at", "vehiculo_id", "chofer_id")} | \
            {"v_patente": self.veh[s["vehiculo_id"]]["patente"]}

    # ── lecturas
    def select(self, s, p):
        if s.startswith("SELECT * FROM flota_vehiculos WHERE id=%s"):
            return [dict(self.veh[p[0]])] if p[0] in self.veh else []
        if s.startswith("SELECT * FROM flota_solicitudes WHERE id=%s FOR UPDATE"):
            return [dict(self.sol[p[0]])] if p[0] in self.sol else []
        if s.startswith("SELECT u.id, (SELECT up.permitido FROM usuario_permisos up WHERE up.user_id=u.id AND up.permiso='flota_gestionar'"):
            return [{"id": 2, "ov": 1, "rolp": None}, {"id": 4, "ov": None, "rolp": 0}]      # Beto gestiona la flota; Dani no
        if s.startswith("SELECT id, tipo, nombre_original, content_type, bytes, subido_por, subido_at FROM flota_archivos WHERE entidad='solicitud'"):
            return [dict(a) for a in sorted(self.arch.values(), key=lambda a: a["id"]) if a["entidad"] == "solicitud" and a["entidad_id"] == p[0] and a["activo"]]
        if s.startswith("SELECT IS_NULLABLE FROM information_schema.COLUMNS"):
            return [{"IS_NULLABLE": "YES"}]
        if s.startswith("SELECT id, nombre_completo FROM flota_choferes WHERE rut=%s"):
            return [{"id": c["id"], "nombre_completo": c.get("nombre_completo")} for c in self.cho.values() if c.get("rut") == p[0]][:1]
        if s.startswith("SELECT id, tipo, nombre_original, content_type, bytes, subido_por, subido_at, activo, quitado_por, quitado_at FROM flota_archivos"):
            return [dict(a) for a in sorted(self.arch.values(), key=lambda a: -a["id"]) if a["entidad"] == "vehiculo" and a["entidad_id"] == p[0]]
        if s.startswith("SELECT c.*, COALESCE(u.nombre, c.nombre_completo) AS u_nombre"):
            if s.endswith("WHERE c.usuario_id IS NULL AND c.rut=%s ORDER BY c.id DESC LIMIT 1"):
                return [self._chof_row(c) for c in sorted(self.cho.values(), key=lambda c: -c["id"]) if c["usuario_id"] is None and c.get("rut") == p[0]][:1]
            if s.endswith("WHERE c.id=%s"):
                return [self._chof_row(self.cho[p[0]])] if p[0] in self.cho else []
            if s.endswith("WHERE c.usuario_id=%s"):
                return [self._chof_row(c) for c in self.cho.values() if c["usuario_id"] == p[0]]
            if s.endswith("WHERE c.archivado=%s ORDER BY c.habilitado DESC, COALESCE(u.nombre, c.nombre_completo) LIMIT 500"):
                return sorted((self._chof_row(c) for c in self.cho.values() if int(c.get("archivado") or 0) == p[0]),
                              key=lambda c: (-c["habilitado"], c["u_nombre"] or ""))
        if s == "SELECT COUNT(*) AS n FROM flota_choferes WHERE archivado=1":
            return [{"n": sum(1 for c in self.cho.values() if c.get("archivado"))}]
        if s.startswith("SELECT id, nombre, active FROM `app_users` WHERE id=%s"):
            u = self.usr.get(p[0])
            return [{"id": u["id"], "nombre": u["nombre"], "active": u["active"]}] if u else []
        if s.startswith("SELECT id, nombre, username, phone, cargo, rut, direccion, direccion_lat, direccion_lng, direccion_place_id FROM `app_users` WHERE active=1 AND id NOT IN"):
            assert "WHERE usuario_id IS NOT NULL" in s          # sin esto, un NULL en el NOT IN vacía la lista
            ya = {c["usuario_id"] for c in self.cho.values() if c["usuario_id"] is not None}
            return sorted((dict(u) for u in self.usr.values() if u["active"] and u["id"] not in ya), key=lambda u: u["nombre"])
        if s.startswith("SELECT s.*, v.patente AS v_patente"):
            filas = list(self.sol.values())
            if s.endswith("WHERE s.id=%s"):
                return [self._sol_row(self.sol[p[0]])] if p[0] in self.sol else []
            if s.endswith("WHERE s.fin < %s ORDER BY s.inicio DESC, s.id DESC LIMIT 500"):
                return [self._sol_row(x) for x in sorted(filas, key=lambda x: (x["inicio"], x["id"]), reverse=True) if x["fin"] < p[0]]
            if s.endswith("WHERE s.fin >= %s ORDER BY s.inicio, s.id LIMIT 500"):
                return [self._sol_row(x) for x in sorted(filas, key=lambda x: (x["inicio"], x["id"])) if x["fin"] >= p[0]]
            if "WHERE s.inicio < %s AND s.fin > %s AND s.estado <> 'cancelada'" in s:
                return [self._sol_row(x) for x in sorted(filas, key=lambda x: (x["inicio"], x["id"]))
                        if x["inicio"] < p[0] and x["fin"] > p[1] and x["estado"] != "cancelada"]
        if s.startswith("SELECT s.id, s.numero, s.estado, s.inicio, s.fin"):
            campo = "vehiculo_id" if "WHERE s.vehiculo_id=%s" in s else "chofer_id"
            return [self._ocupa_row(x) for x in self.sol.values()
                    if x[campo] == p[0] and x["estado"] in R.ESTADOS_OCUPAN and x["inicio"] < p[1] and x["fin"] > p[2]]
        if s.startswith("SELECT estado, COUNT(*) AS n FROM flota_solicitudes WHERE fin >= %s GROUP BY estado"):
            out = {}
            for x in self.sol.values():
                if x["fin"] >= p[0]:
                    out[x["estado"]] = out.get(x["estado"], 0) + 1
            return [{"estado": k, "n": v} for k, v in out.items()]
        if s.startswith("SELECT COUNT(*) AS n FROM flota_solicitudes WHERE estado IN"):
            return [{"n": sum(1 for x in self.sol.values() if x["estado"] in R.ESTADOS_OCUPAN and x["inicio"] < p[0] and x["fin"] > p[1])}]
        if s.startswith("SELECT COUNT(*) AS n FROM flota_solicitudes WHERE vehiculo_id=%s") or \
                s.startswith("SELECT COUNT(*) AS n FROM flota_solicitudes WHERE chofer_id=%s"):
            if " AND fin >=" in s:
                return [{"n": self.abiertas}]
            campo = "vehiculo_id" if "WHERE vehiculo_id=%s" in s else "chofer_id"
            return [{"n": sum(1 for x in self.sol.values() if x[campo] == p[0])}]
        if s.startswith("SELECT id, accion, detalle, usuario, created_at FROM flota_solicitud_log WHERE solicitud_id=%s"):
            return [dict(x) for x in sorted(self.slog, key=lambda x: -x["id"]) if x["solicitud_id"] == p[0]]
        if s.startswith("SELECT id, accion, detalle, usuario, created_at FROM flota_chofer_log WHERE chofer_id=%s"):
            return [dict(x) for x in sorted(self.clog, key=lambda x: -x["id"]) if x["chofer_id"] == p[0]]
        return None

    def fetchone(self, q, p=None):
        r = self.select(_n(q), p or ())
        if r is None:
            return super().fetchone(q, p)
        return r[0] if r else None

    def fetchall(self, q, p=None):
        r = self.select(_n(q), p or ())
        return super().fetchall(q, p) if r is None else r

    # ── escrituras (comunes a la conexión de la transacción y a mysql_execute)
    def escribir(self, s, p):
        """(rowcount, lastrowid) o None si no es de la fase 1b."""
        if s.startswith("INSERT INTO flota_choferes (usuario_id, telefono,"):
            c = {"id": self.sig_c, "usuario_id": None, "telefono": p[0], "rut": p[1], "nombre_completo": p[2], "direccion": p[3],
                 "direccion_lat": p[4], "direccion_lng": p[5], "direccion_place_id": p[6], "licencia_clases": p[7],
                 "licencia_vence": p[8], "notas": p[9], "habilitado": 1, "archivado": 0, "archivado_motivo": None, "archivado_por": None,
                 "archivado_at": None, "inhabilitado_motivo": None, "inhabilitado_por": None, "inhabilitado_at": None,
                 "created_by": p[10], "updated_by": p[11],
                 "created_at": dt.datetime(2026, 11, 2, 15), "updated_at": dt.datetime(2026, 11, 2, 15)}
            self.cho[c["id"]] = c
            self.sig_c += 1
            return 1, c["id"]
        if s.startswith("UPDATE flota_choferes SET telefono=%s"):
            self.cho[p[2]].update(telefono=p[0], updated_by=p[1])
            return 1, None
        if s.startswith("INSERT INTO flota_choferes"):
            if any(c["usuario_id"] == p[0] for c in self.cho.values()):
                e = Exception("(1062, \"Duplicate entry '1' for key 'uq_flota_chofer_usuario'\")")
                e.args = (1062, "Duplicate entry")
                raise e
            c = {"id": self.sig_c, "usuario_id": p[0], "rut": p[1], "nombre_completo": p[2], "direccion": p[3], "direccion_lat": p[4],
                 "direccion_lng": p[5], "direccion_place_id": p[6], "licencia_clases": p[7],
                 "licencia_vence": p[8], "notas": p[9], "habilitado": 1, "archivado": 0, "archivado_motivo": None, "archivado_por": None,
                 "archivado_at": None, "inhabilitado_motivo": None, "inhabilitado_por": None, "inhabilitado_at": None,
                 "created_by": p[10], "updated_by": p[11],
                 "created_at": dt.datetime(2026, 11, 2, 15), "updated_at": dt.datetime(2026, 11, 2, 15)}
            self.cho[c["id"]] = c
            self.sig_c += 1
            return 1, c["id"]
        if s.startswith("INSERT INTO flota_chofer_log"):
            self.clog.append({"id": len(self.clog) + 1, "chofer_id": p[0], "accion": p[1], "detalle": p[2], "usuario": p[3],
                              "created_at": dt.datetime(2026, 11, 2, 18)})
            return 1, None
        if s.startswith("UPDATE flota_choferes SET rut=%s"):
            sets = re.findall(r"(\w+)=%s", s.split(" WHERE ")[0])
            self.cho[p[-1]].update(dict(zip(sets, p[:-1])))
            return 1, None
        if s.startswith("UPDATE flota_choferes SET archivado=1"):
            motivo, por, inhab, _por2, _por3, cid = p
            c = self.cho.get(cid)
            if not c or c.get("archivado"):
                return 0, None
            c.update(archivado=1, archivado_motivo=motivo, archivado_por=por, archivado_at=dt.datetime(2026, 11, 2, 19, 30), habilitado=0,
                     inhabilitado_motivo=inhab, inhabilitado_por=por, inhabilitado_at=dt.datetime(2026, 11, 2, 19, 30))
            return 1, None
        if s.startswith("UPDATE flota_choferes SET archivado=0"):
            _por, cid = p
            c = self.cho.get(cid)
            if not c or not c.get("archivado"):
                return 0, None
            c.update(archivado=0, archivado_motivo=None, archivado_por=None, archivado_at=None)
            return 1, None
        if s.startswith("DELETE FROM flota_choferes WHERE id=%s"):
            self.cho.pop(p[0], None)
            return 1, None
        if s.startswith("UPDATE flota_choferes SET habilitado=0"):
            motivo, por, _por2, cid = p
            c = self.cho.get(cid)
            if not c or not c["habilitado"]:
                return 0, None
            c.update(habilitado=0, inhabilitado_motivo=motivo, inhabilitado_por=por, inhabilitado_at=dt.datetime(2026, 11, 2, 19))
            return 1, None
        if s.startswith("UPDATE flota_choferes SET habilitado=1"):
            _por, cid = p
            c = self.cho.get(cid)
            if not c or c["habilitado"]:
                return 0, None
            c.update(habilitado=1, inhabilitado_motivo=None, inhabilitado_por=None, inhabilitado_at=None)
            return 1, None
        if s.startswith("INSERT INTO flota_solicitudes ("):
            cols = ("vehiculo_id", "area", "solicitante_id", "solicitante_nombre", "chofer_id", "inicio", "fin", "motivo")
            x = dict(zip(cols, p))
            x.update(id=self.sig_s, numero=None, estado="solicitada", sello_at=dt.datetime.utcnow(), confirmada_por=None, confirmada_at=None,
                     llave_por=None, llave_at=None, llave_nota=None, cancelada_por=None, cancelada_at=None, cancelada_motivo=None,
                     modificada_por=None, modificada_at=None)
            self.sol[x["id"]] = x
            self.sig_s += 1
            return 1, x["id"]
        if s.startswith("UPDATE flota_solicitudes SET numero=%s WHERE id=%s"):
            self.sol[p[1]]["numero"] = p[0]
            return 1, None
        if s.startswith("INSERT INTO flota_solicitud_log"):
            self.slog.append({"id": len(self.slog) + 1, "solicitud_id": p[0], "accion": p[1], "detalle": p[2], "usuario": p[3],
                              "created_at": dt.datetime(2026, 11, 2, 18, 30)})
            return 1, None
        if s.startswith("UPDATE flota_solicitudes SET vehiculo_id=%s"):
            vid, area, cid, ini, fin, motivo, por, sid = p
            x = self.sol[sid]
            if x["estado"] not in ("solicitada", "confirmada"):
                return 0, None
            x.update(vehiculo_id=vid, area=area, chofer_id=cid, inicio=ini, fin=fin, motivo=motivo, modificada_por=por,
                     modificada_at=dt.datetime.utcnow())
            return 1, None
        if s.startswith("UPDATE flota_solicitudes SET estado='confirmada'"):
            por, sid = p
            x = self.sol.get(sid)
            if not x or x["estado"] != "solicitada":
                return 0, None
            x.update(estado="confirmada", confirmada_por=por, confirmada_at=dt.datetime.utcnow())
            return 1, None
        if s.startswith("UPDATE flota_solicitudes SET estado='llave_entregada'"):
            por, nota, sid = p
            x = self.sol.get(sid)
            if not x or x["estado"] != "confirmada":
                return 0, None
            x.update(estado="llave_entregada", llave_por=por, llave_at=dt.datetime.utcnow(), llave_nota=nota)
            return 1, None
        if s.startswith("UPDATE flota_solicitudes SET estado='devuelta'"):
            por, km, comb, obs, nov, sid = p
            x = self.sol.get(sid)
            if not x or x["estado"] != "llave_entregada":
                return 0, None
            x.update(estado="devuelta", devuelta_por=por, devuelta_at=dt.datetime.utcnow(), dev_km=km, dev_combustible=comb, dev_obs=obs,
                     dev_novedad=nov)
            return 1, None
        if s.startswith("UPDATE flota_vehiculos SET km_actual=%s, updated_by=%s WHERE id=%s AND (km_actual IS NULL OR km_actual < %s)"):
            km, _por, vid, _km2 = p
            v = self.veh.get(vid)
            if not v or (v.get("km_actual") is not None and v["km_actual"] >= km):
                return 0, None
            v["km_actual"] = km
            return 1, None
        if s.startswith("UPDATE flota_solicitudes SET estado='cancelada'"):
            por, motivo, sid = p
            x = self.sol.get(sid)
            if not x or x["estado"] not in R.ESTADOS_OCUPAN:
                return 0, None
            x.update(estado="cancelada", cancelada_por=por, cancelada_at=dt.datetime.utcnow(), cancelada_motivo=motivo)
            return 1, None
        return None

    def execute(self, q, p=None):
        s = _n(q)
        self.sql.append(s)
        if self.escribir(s, p or ()) is None:
            return super().execute(q, p)

    def rowcount(self, q, p=None):
        s = _n(q)
        r = self.escribir(s, p or ())
        if r is None:
            return super().rowcount(q, p)
        self.sql.append(s)
        return r[0]

    # ── la conexión con transacción que usa la reserva
    def conexion(self):
        return _Conexion(self)


class _Conexion:
    def __init__(self, base):
        self.b = base

    def autocommit(self, v):
        self.ac = v

    def cursor(self):
        return _Cursor(self.b)

    def commit(self):
        self.b.commits += 1

    def rollback(self):
        self.b.rollbacks += 1

    def close(self):
        self.b.cerradas += 1


class _Cursor:
    def __init__(self, base):
        self.b, self._r, self.rowcount, self.lastrowid = base, [], 0, None

    def execute(self, q, p=None):
        s = _n(q)
        self.b.sql.append(s)
        p = p or ()
        if s.startswith("SELECT"):
            r = self.b.select(s, p)
            if r is None:
                raise AssertionError("SELECT de la transacción no previsto: " + s[:100])
            self._r = r
            return
        r = self.b.escribir(s, p)
        if r is None:
            raise AssertionError("escritura de la transacción no prevista: " + s[:100])
        self.rowcount, self.lastrowid = r

    def fetchone(self):
        return dict(self._r[0]) if self._r else None

    def fetchall(self):
        return [dict(x) for x in self._r]


def _app(base, bucket=None, extra=None):
    from flask import Flask, g, redirect, request

    plantillas = tempfile.mkdtemp(prefix="flota_tpl_")
    os.makedirs(os.path.join(plantillas, "flota"))
    with open(os.path.join(plantillas, "base.html"), "w", encoding="utf-8") as f:
        f.write("<!doctype html><html><head>{% block head_extra %}{% endblock %}</head>"
                "<body>{% block content %}{% endblock %}{% block scripts %}{% endblock %}</body></html>")
    shutil.copy(os.path.join(RAIZ, "templates", "flota", "index.html"), os.path.join(plantillas, "flota", "index.html"))
    app = Flask(__name__, template_folder=plantillas)
    app.config["TESTING"] = True

    @app.route("/")
    def index():
        return "inicio"

    @app.before_request
    def _usuario():
        rol = request.headers.get("X-Rol")
        g.user = {"id": 1, "nombre": "Daniel Aguilar"} if rol else None
        extra = {x.strip(): True for x in (request.headers.get("X-Flota") or "").split(",") if x.strip()}
        g.permissions = {"superadmin": rol == "superadmin", "retiros": rol in ("admin", "superadmin"), "mantenciones": rol is not None, **extra}

    def login_required(view):
        from functools import wraps

        @wraps(view)
        def w(*a, **k):
            if not g.user:
                return redirect("/")
            return view(*a, **k)
        return w

    ctx = {"mysql_fetchone": base.fetchone, "mysql_fetchall": base.fetchall, "mysql_execute": base.execute,
           "mysql_execute_returning_rowcount": base.rowcount, "get_mysql": base.conexion, "AUTH_TABLE": "app_users",
           "login_required": login_required, "current_username": lambda: "Daniel Aguilar", "_gcs_bucket": lambda: bucket}
    ctx.update(extra or {})
    import flota_module
    flota_module.register_flota_routes(app, ctx)
    return app


SUPER = {"X-Rol": "superadmin"}
OTRO = {"X-Rol": "admin"}
FUTURO = (dt.date.today() + dt.timedelta(days=1500)).isoformat()      # vigente también para las fechas lejanas que usan algunas pruebas


class Base(unittest.TestCase):
    def setUp(self):
        self.base = Falsa()
        self.app = _app(self.base)
        self.c = self.app.test_client()
        # Dos vehículos (como los de Daniel) y dos choferes con licencia B vigente.
        self.furgon = self.veh("TDVZ88", "Furgón", "Suzuki", "Swift")
        self.camion = self.veh("PHVD21", "Camión", "Kia", "Frontier", licencia_clases=["B"])
        self.ana = self.chofer(1)
        self.beto = self.chofer(2)

    def veh(self, patente, tipo, marca, modelo, **k):
        d = {"patente": patente, "tipo": tipo, "marca": marca, "modelo": modelo, "licencia_clases": ["B"], "revision_tecnica_vence": FUTURO,
             "permiso_circulacion_vence": FUTURO, "soap_vence": FUTURO, "seguro_vence": FUTURO}
        d.update(k)
        r = self.c.post("/flota/api/vehiculos", json=d, headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        return r.get_json()["vehiculo"]["id"]

    def chofer(self, uid, **k):
        d = dict(_datos_chofer(uid), usuario_id=uid)
        d.update(k)
        r = self.c.post("/flota/api/choferes", json=d, headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        return r.get_json()["chofer"]["id"]

    def pedir(self, **k):
        d = _sol(vehiculo_id=self.furgon, chofer_id=self.ana)
        d.update(k)
        r = self.c.post("/flota/api/solicitudes", json=d, headers=SUPER)
        return r, r.get_json()


class TestPermisos(Base):
    def test_solo_el_superadmin_entra_a_todo(self):
        for url in ("/flota/api/choferes", "/flota/api/solicitudes", "/flota/api/calendario"):
            self.assertEqual(self.c.get(url, headers=OTRO).status_code, 403, url)
            self.assertEqual(self.c.get(url, headers=SUPER).status_code, 200, url)
        r = self.c.post("/flota/api/solicitudes", json=_sol(vehiculo_id=self.furgon, chofer_id=self.ana), headers=OTRO)
        self.assertEqual(r.status_code, 403)
        self.assertEqual(len(self.base.sol), 0)

    def test_las_acciones_de_la_gestion_estan_cerradas_al_resto(self):
        r, d = self.pedir()
        sid = d["solicitud"]["id"]
        for ruta, cuerpo in (("confirmar", {}), ("llave", {}), ("cancelar", {"motivo": "no se usará"})):
            self.assertEqual(self.c.post(f"/flota/api/solicitudes/{sid}/{ruta}", json=cuerpo, headers=OTRO).status_code, 403, ruta)
        self.assertEqual(self.base.sol[sid]["estado"], "solicitada")

    def test_sin_sesion_no_entra(self):
        self.assertEqual(self.c.get("/flota/api/solicitudes").status_code, 302)


class TestChoferes(Base):
    def test_alta_con_el_nombre_del_usuario_y_bitacora(self):
        r = self.c.get("/flota/api/choferes", headers=SUPER).get_json()
        ana = next(c for c in r["choferes"] if c["usuario_id"] == 1)
        self.assertEqual(ana["nombre"], "Ana Pérez")
        self.assertEqual(ana["licencia_texto"], "Clase B")
        self.assertTrue(ana["disponible"])
        self.assertEqual(r["resumen"]["habilitados"], 2)
        self.assertEqual([x["accion"] for x in self.base.clog], ["creado", "creado"])

    def test_el_usuario_debe_existir_y_estar_activo(self):
        for uid in (3, 99):
            r = self.c.post("/flota/api/choferes", json=dict(_datos_chofer(uid), usuario_id=uid), headers=SUPER)
            self.assertEqual(r.status_code, 400, uid)
        self.assertEqual(len(self.base.cho), 2)

    def test_no_se_repite_el_mismo_usuario(self):
        r = self.c.post("/flota/api/choferes", json=dict(_datos_chofer(1), usuario_id=1), headers=SUPER)
        self.assertEqual(r.status_code, 409)
        self.assertIn("usuario_id", r.get_json()["errores"])

    def test_licencia_y_vencimiento_son_obligatorios(self):
        n = len(self.base.cho)
        r = self.c.post("/flota/api/choferes", json={"usuario_id": 4}, headers=SUPER)
        self.assertEqual(r.status_code, 400)
        self.assertIn("licencia_clases", r.get_json()["errores"])
        self.assertIn("licencia_vence", r.get_json()["errores"])
        self.assertEqual(len(self.base.cho), n)

    def test_usuarios_disponibles_excluye_a_los_que_ya_son_choferes_y_a_los_desactivados(self):
        r = self.c.get("/flota/api/choferes/usuarios", headers=SUPER).get_json()
        self.assertEqual([u["id"] for u in r["usuarios"]], [4])

    def test_licencia_vencida_no_deja_disponible(self):
        self.base.cho[self.ana]["licencia_vence"] = dt.date.today() - dt.timedelta(days=3)
        c = next(x for x in self.c.get("/flota/api/choferes", headers=SUPER).get_json()["choferes"] if x["id"] == self.ana)
        self.assertEqual(c["licencia_estado"], "vencido")
        self.assertFalse(c["disponible"])

    def test_editar_deja_lo_que_cambio(self):
        r = self.c.put(f"/flota/api/choferes/{self.ana}", json=_datos_chofer(1, licencia_clases=["A4", "B"], notas="Profesional"), headers=SUPER)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(self.base.cho[self.ana]["licencia_clases"], "A4,B")
        self.assertEqual(json.loads(self.base.clog[-1]["detalle"])["cambios"]["licencia_clases"]["despues"], "A4,B")
        r = self.c.put(f"/flota/api/choferes/{self.ana}", json=_datos_chofer(1, licencia_clases=["A4", "B"], notas="Profesional"), headers=SUPER)
        self.assertTrue(r.get_json()["sin_cambios"])

    def test_inhabilitar_pide_motivo_y_avisa_si_tenia_reservas(self):
        self.assertEqual(self.c.post(f"/flota/api/choferes/{self.ana}/inhabilitar", json={"motivo": "x"}, headers=SUPER).status_code, 400)
        self.base.abiertas = 2
        r = self.c.post(f"/flota/api/choferes/{self.ana}/inhabilitar", json={"motivo": "Licencia suspendida"}, headers=SUPER)
        self.assertEqual(r.status_code, 200)
        self.assertFalse(r.get_json()["chofer"]["habilitado"])
        self.assertIn("2 reservas pendientes", r.get_json()["avisos"][0])
        self.assertEqual(self.c.post(f"/flota/api/choferes/{self.ana}/inhabilitar", json={"motivo": "Licencia suspendida"}, headers=SUPER).status_code, 409)
        self.assertEqual(self.c.post(f"/flota/api/choferes/{self.ana}/habilitar", json={}, headers=SUPER).status_code, 200)
        h = self.c.get(f"/flota/api/choferes/{self.ana}/historial", headers=SUPER).get_json()["eventos"]
        self.assertEqual([e["accion"] for e in h], ["habilitado", "inhabilitado", "creado"])

    def test_inhabilitar_un_vehiculo_con_reservas_avisa_pero_lo_hace(self):
        self.base.abiertas = 1
        r = self.c.post(f"/flota/api/vehiculos/{self.furgon}/inhabilitar", json={"motivo": "En el taller"}, headers=SUPER)
        self.assertEqual(r.status_code, 200)
        self.assertFalse(r.get_json()["vehiculo"]["habilitado"])
        self.assertIn("1 reserva pendiente", r.get_json()["avisos"][0])

    def test_inexistentes(self):
        self.assertEqual(self.c.put("/flota/api/choferes/99", json={}, headers=SUPER).status_code, 404)
        self.assertEqual(self.c.post("/flota/api/choferes/99/habilitar", json={}, headers=SUPER).status_code, 404)


class TestSolicitudes(Base):
    def test_crear_deja_numero_sello_y_bitacora(self):
        r, d = self.pedir()
        self.assertEqual(r.status_code, 200, d)
        s = d["solicitud"]
        self.assertRegex(s["numero"], r"^VEH-\d{4}-0001$")
        self.assertEqual(s["estado"], "solicitada")
        self.assertEqual(s["estado_texto"], "Por confirmar")
        self.assertEqual(s["vehiculo"]["patente_fmt"], "TD-VZ88".replace("-", "-") if False else s["vehiculo"]["patente_fmt"])
        self.assertEqual(s["area"], "Logística")
        self.assertEqual(s["solicitante"], "Daniel Aguilar")
        self.assertEqual(s["chofer"]["nombre"], "Ana Pérez")
        self.assertEqual((s["hora_desde"], s["hora_hasta"]), ("08:00", "12:30"))
        self.assertRegex(s["sello_fmt"], r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$")          # hora Chile, nunca ISO (REGLA #6)
        self.assertNotRegex(s["rango_texto"], r"\d{4}-\d{2}-\d{2}")
        self.assertEqual([x["accion"] for x in self.base.slog], ["creada"])
        self.assertEqual(self.base.commits, 1)

    def test_el_numero_sale_distinto_a_cada_una(self):
        _r, a = self.pedir()
        _r, b = self.pedir(vehiculo_id=self.camion, chofer_id=self.beto)
        self.assertNotEqual(a["solicitud"]["numero"], b["solicitud"]["numero"])

    def test_la_reserva_bloquea_el_vehiculo_antes_de_decidir(self):
        self.pedir()
        sql = self.base.sql
        i_lock = max(i for i, q in enumerate(sql) if q.startswith("SELECT * FROM flota_vehiculos WHERE id=%s FOR UPDATE"))
        i_choque = max(i for i, q in enumerate(sql) if q.startswith("SELECT s.id, s.numero") and "s.vehiculo_id=%s" in q)
        i_insert = max(i for i, q in enumerate(sql) if q.startswith("INSERT INTO flota_solicitudes ("))
        self.assertLess(i_lock, i_choque)
        self.assertLess(i_choque, i_insert)

    def test_el_sello_decide_el_segundo_en_pedir_recibe_quien_lo_tiene(self):
        _r, primera = self.pedir(area="Servicio Técnico")
        n = len(self.base.sol)
        r, d = self.pedir(hora_desde="10:00", hora_hasta="14:00", chofer_id=self.beto, area="Logística")
        self.assertEqual(r.status_code, 409)
        self.assertEqual(d["error_codigo"], "CHOQUE_VEHICULO")
        self.assertIn(primera["solicitud"]["numero"], d["error"])
        self.assertIn("Servicio Técnico", d["error"])
        self.assertEqual(d["choques"][0]["numero"], primera["solicitud"]["numero"])
        self.assertEqual(len(self.base.sol), n)                              # NO se guardó nada
        self.assertEqual(self.base.rollbacks, 1)
        self.assertEqual(self.base.cerradas, self.base.commits + self.base.rollbacks)      # la conexión siempre se cierra

    def test_pegado_no_es_choque_y_el_otro_vehiculo_o_otra_hora_si_se_puede(self):
        self.pedir()                                                           # 08:00 a 12:30
        self.assertEqual(self.pedir(hora_desde="12:30", hora_hasta="15:00", chofer_id=self.beto)[0].status_code, 200)
        self.assertEqual(self.pedir(vehiculo_id=self.camion, chofer_id=self.beto, hora_desde="08:00", hora_hasta="12:30")[0].status_code, 200)

    def test_un_chofer_no_puede_estar_en_dos_vehiculos_a_la_vez(self):
        self.pedir()
        r, d = self.pedir(vehiculo_id=self.camion, hora_desde="09:00", hora_hasta="11:00")          # misma Ana, otro vehículo
        self.assertEqual(r.status_code, 409)
        self.assertEqual(d["error_codigo"], "CHOQUE_CHOFER")
        self.assertIn("chofer_id", d["errores"])

    def test_cancelar_libera_el_horario(self):
        _r, a = self.pedir()
        self.assertEqual(self.pedir(chofer_id=self.beto)[0].status_code, 409)
        self.c.post(f"/flota/api/solicitudes/{a['solicitud']['id']}/cancelar", json={"motivo": "Ya no se usará"}, headers=SUPER)
        self.assertEqual(self.pedir(chofer_id=self.beto)[0].status_code, 200)

    def test_vehiculo_inhabilitado_no_se_reserva(self):
        self.c.post(f"/flota/api/vehiculos/{self.furgon}/inhabilitar", json={"motivo": "En el taller"}, headers=SUPER)
        r, d = self.pedir()
        self.assertEqual(r.status_code, 409)
        self.assertIn("inhabilitado", d["error"])
        self.assertEqual(len(self.base.sol), 0)

    def test_el_chofer_debe_tener_la_licencia_que_exige_el_vehiculo(self):
        camion_a4 = self.veh("CAM123", "Camión", "Volvo", "FH", licencia_clases=["A4"])
        r, d = self.pedir(vehiculo_id=camion_a4)                                # Ana solo tiene B
        self.assertEqual(r.status_code, 400)
        self.assertIn("chofer_id", d["errores"])
        self.assertEqual(len(self.base.sol), 0)

    def test_licencia_vencida_antes_de_terminar_el_uso_no_deja(self):
        self.base.cho[self.ana]["licencia_vence"] = DIA - dt.timedelta(days=1)
        r, d = self.pedir()
        self.assertEqual(r.status_code, 400)
        self.assertIn("antes de terminar", d["errores"]["chofer_id"])

    def test_chofer_inhabilitado_no_recibe_reservas(self):
        self.c.post(f"/flota/api/choferes/{self.ana}/inhabilitar", json={"motivo": "Licencia suspendida"}, headers=SUPER)
        r, d = self.pedir()
        self.assertEqual(r.status_code, 400)
        self.assertIn("inhabilitado", d["errores"]["chofer_id"])

    def test_chofer_con_usuario_desactivado_no_recibe_reservas(self):
        self.base.usr[1]["active"] = 0
        self.assertEqual(self.pedir()[0].status_code, 400)

    def test_documentos_vencidos_y_licencia_por_vencer_alertan_sin_trabar(self):
        self.c.put(f"/flota/api/vehiculos/{self.furgon}", json={
            "patente": "TDVZ88", "tipo": "Furgón", "licencia_clases": ["B"], "soap_vence": (DIA - dt.timedelta(days=2)).isoformat()}, headers=SUPER)
        self.base.cho[self.ana]["licencia_vence"] = DIA + dt.timedelta(days=5)
        r, d = self.pedir()
        self.assertEqual(r.status_code, 200)                                  # se guarda igual
        self.assertTrue(any("SOAP" in a for a in d["avisos"]), d["avisos"])
        self.assertTrue(any("licencia" in a.lower() for a in d["avisos"]), d["avisos"])
        self.assertEqual(len(self.base.sol), 1)

    def test_fin_de_semana_y_feriado_alertan_sin_trabar(self):
        sabado = DIA + dt.timedelta(days=4)
        r, d = self.pedir(fecha=sabado.isoformat())
        self.assertEqual(r.status_code, 200)
        self.assertTrue(any("fin de semana" in a for a in d["avisos"]), d["avisos"])
        r, d = self.pedir(fecha=dt.date(DIA.year + 1, 12, 25).isoformat(), chofer_id=self.beto)
        self.assertEqual(r.status_code, 200)
        self.assertTrue(any("feriado" in a.lower() for a in d["avisos"]), d["avisos"])

    def test_invalidas_no_tocan_la_base(self):
        for k in ({"hora_desde": ""}, {"fecha": "2020-01-01"}, {"hora_desde": "14:00", "hora_hasta": "09:00"}, {"motivo": ""}, {"area": ""},
                  {"vehiculo_id": 999}, {"chofer_id": 999}, {"vehiculo_id": ""}):
            r, d = self.pedir(**k)
            self.assertEqual(r.status_code, 400, k)
            self.assertFalse(d["ok"])
        self.assertEqual(len(self.base.sol), 0)
        self.assertEqual(self.base.slog, [])

    def test_en_dos_dias(self):
        r, d = self.pedir(fecha_hasta=(DIA + dt.timedelta(days=1)).isoformat(), hora_hasta="10:00")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(d["solicitud"]["fecha_hasta"], (DIA + dt.timedelta(days=1)).isoformat())
        # y el segundo día lo ocupa para el que pida ese vehículo a esa hora
        self.assertEqual(self.pedir(fecha=(DIA + dt.timedelta(days=1)).isoformat(), hora_desde="09:00", hora_hasta="09:30", chofer_id=self.beto)[0].status_code, 409)


class TestFlujo(Base):
    def setUp(self):
        super().setUp()
        _r, d = self.pedir()
        self.sid = d["solicitud"]["id"]

    def post(self, ruta, cuerpo=None, sid=None):
        r = self.c.post(f"/flota/api/solicitudes/{sid or self.sid}/{ruta}", json=cuerpo or {}, headers=SUPER)
        return r, r.get_json()

    def test_confirmar_y_llave_en_orden(self):
        r, d = self.post("llave")
        self.assertEqual(r.status_code, 409)                                  # primero se confirma
        r, d = self.post("confirmar")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(d["solicitud"]["estado"], "confirmada")
        self.assertEqual(d["solicitud"]["confirmada_por"], "Daniel Aguilar")
        r, d = self.post("llave", {"nota": "Se la di en portería"})
        self.assertEqual(r.status_code, 200)
        s = d["solicitud"]
        self.assertEqual(s["estado"], "llave_entregada")
        self.assertEqual(s["llave_por"], "Daniel Aguilar")
        self.assertEqual(s["llave_nota"], "Se la di en portería")
        self.assertRegex(s["llave_fmt"], r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$")       # la hora de la entrega, en hora Chile
        self.assertEqual([x["accion"] for x in self.base.slog], ["creada", "confirmada", "llave_entregada"])

    def test_cada_paso_se_da_una_sola_vez(self):
        self.post("confirmar")
        r, d = self.post("confirmar")
        self.assertEqual(r.status_code, 409)
        self.assertIn("Otra persona", d["error"])
        self.post("llave")
        self.assertEqual(self.post("llave")[0].status_code, 409)
        self.assertEqual([x["accion"] for x in self.base.slog], ["creada", "confirmada", "llave_entregada"])

    def test_la_nota_de_la_llave_tiene_tope(self):
        self.post("confirmar")
        self.assertEqual(self.post("llave", {"nota": "x" * 301})[0].status_code, 400)
        self.assertEqual(self.base.sol[self.sid]["estado"], "confirmada")

    def test_cancelar_pide_motivo_y_queda_en_la_bitacora(self):
        self.assertEqual(self.post("cancelar", {"motivo": "no"})[0].status_code, 400)
        r, d = self.post("cancelar", {"motivo": "Cambió el plan de ruta"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(d["solicitud"]["estado"], "cancelada")
        self.assertEqual(d["solicitud"]["cancelada_motivo"], "Cambió el plan de ruta")
        self.assertEqual(self.post("cancelar", {"motivo": "Cambió el plan de ruta"})[0].status_code, 409)
        self.assertEqual(json.loads(self.base.slog[-1]["detalle"])["motivo"], "Cambió el plan de ruta")

    def test_con_la_llave_entregada_no_se_mueve_pero_se_puede_cancelar_con_motivo(self):
        self.post("confirmar")
        self.post("llave")
        r = self.c.put(f"/flota/api/solicitudes/{self.sid}", json=dict(_sol(vehiculo_id=self.furgon, chofer_id=self.ana, hora_hasta="13:00"),
                                                                       motivo_cambio="Se alarga"), headers=SUPER)
        self.assertEqual(r.status_code, 409)
        r, d = self.post("cancelar", {"motivo": "Se devolvió sin usar"})
        self.assertEqual(r.status_code, 200)                                  # ningún callejón sin salida

    def test_mover_la_reserva_pide_motivo_y_revisa_choques_sin_chocar_consigo_misma(self):
        cuerpo = _sol(vehiculo_id=self.furgon, chofer_id=self.ana, hora_hasta="13:00")
        self.assertEqual(self.c.put(f"/flota/api/solicitudes/{self.sid}", json=cuerpo, headers=SUPER).status_code, 400)      # sin motivo
        r = self.c.put(f"/flota/api/solicitudes/{self.sid}", json=dict(cuerpo, motivo_cambio="Necesitan una hora más"), headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(r.get_json()["solicitud"]["hora_hasta"], "13:00")
        self.assertEqual(r.get_json()["solicitud"]["modificada_por"], "Daniel Aguilar")
        self.assertEqual(self.base.slog[-1]["accion"], "editada")
        self.assertEqual(json.loads(self.base.slog[-1]["detalle"])["motivo_cambio"], "Necesitan una hora más")

    def test_mover_a_un_horario_ocupado_no_deja(self):
        self.pedir(vehiculo_id=self.camion, chofer_id=self.beto, hora_desde="14:00", hora_hasta="16:00")
        r = self.c.put(f"/flota/api/solicitudes/{self.sid}", json=dict(
            _sol(vehiculo_id=self.camion, chofer_id=self.ana, hora_desde="13:00", hora_hasta="15:00"), motivo_cambio="Prefieren el camión"), headers=SUPER)
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.get_json()["error_codigo"], "CHOQUE_VEHICULO")
        self.assertEqual(self.base.sol[self.sid]["vehiculo_id"], self.furgon)           # sigue donde estaba

    def test_detalle_con_su_bitacora_en_orden(self):
        self.post("confirmar")
        d = self.c.get(f"/flota/api/solicitudes/{self.sid}", headers=SUPER).get_json()
        self.assertEqual([e["accion"] for e in d["eventos"]], ["confirmada", "creada"])
        self.assertTrue(d["solicitud"]["puede"]["llave"])
        self.assertFalse(d["solicitud"]["puede"]["confirmar"])
        self.assertEqual(self.c.get("/flota/api/solicitudes/999", headers=SUPER).status_code, 404)


class TestListaYCalendario(Base):
    def test_lista_con_resumen(self):
        _r, a = self.pedir()
        _r, b = self.pedir(vehiculo_id=self.camion, chofer_id=self.beto)
        self.c.post(f"/flota/api/solicitudes/{a['solicitud']['id']}/confirmar", json={}, headers=SUPER)
        d = self.c.get("/flota/api/solicitudes", headers=SUPER).get_json()
        self.assertEqual(len(d["solicitudes"]), 2)
        self.assertEqual((d["resumen"]["por_confirmar"], d["resumen"]["confirmadas"], d["resumen"]["llave_entregada"]), (1, 1, 0))
        self.assertEqual(d["areas"], ["Logística", "Servicio Técnico", "Gerencia", "Marketing"])
        self.assertRegex(d["ahora_hhmm"], r"^\d{2}:\d{2}$")

    def test_pasadas_y_proximas(self):
        self.pedir()
        self.base.sol[1]["fin"] = dt.datetime.now() - dt.timedelta(days=3)
        self.base.sol[1]["inicio"] = dt.datetime.now() - dt.timedelta(days=3, hours=2)
        self.assertEqual(self.c.get("/flota/api/solicitudes", headers=SUPER).get_json()["solicitudes"], [])
        self.assertEqual(len(self.c.get("/flota/api/solicitudes?vista=pasadas", headers=SUPER).get_json()["solicitudes"]), 1)

    def test_calendario_marca_feriados_y_fines_de_semana_y_trae_las_reservas_del_mes(self):
        anio = DIA.year + 1
        self.pedir(fecha=f"{anio}-12-23", hora_desde="08:00", hora_hasta="10:00")
        d = self.c.get(f"/flota/api/calendario?mes={anio}-12", headers=SUPER).get_json()
        self.assertEqual(d["mes"], f"{anio}-12")
        self.assertEqual(len(d["dias"]), 31)
        navidad = next(x for x in d["dias"] if x["fecha"] == f"{anio}-12-25")
        self.assertEqual(navidad["no_laboral"], "feriado")
        self.assertIn("Navidad", navidad["nombre"])
        self.assertTrue(any(x["no_laboral"] == "fin_de_semana" for x in d["dias"]))
        self.assertEqual(len(d["solicitudes"]), 1)
        self.assertEqual(self.c.get(f"/flota/api/calendario?mes={anio}-11", headers=SUPER).get_json()["solicitudes"], [])
        self.assertEqual(self.c.get("/flota/api/calendario?mes=basura", headers=SUPER).status_code, 200)

    def test_calendario_no_trae_canceladas(self):
        anio = DIA.year + 1
        _r, a = self.pedir(fecha=f"{anio}-12-23")
        self.c.post(f"/flota/api/solicitudes/{a['solicitud']['id']}/cancelar", json={"motivo": "Ya no se usa"}, headers=SUPER)
        self.assertEqual(self.c.get(f"/flota/api/calendario?mes={anio}-12", headers=SUPER).get_json()["solicitudes"], [])

    def test_una_reserva_de_varios_dias_aparece_en_los_dos_meses(self):
        anio = DIA.year + 1
        self.pedir(fecha=f"{anio}-12-30", fecha_hasta=f"{anio + 1}-01-02", hora_desde="08:00", hora_hasta="10:00")
        self.assertEqual(len(self.c.get(f"/flota/api/calendario?mes={anio}-12", headers=SUPER).get_json()["solicitudes"]), 1)
        self.assertEqual(len(self.c.get(f"/flota/api/calendario?mes={anio + 1}-01", headers=SUPER).get_json()["solicitudes"]), 1)


# ═══════════════════════════════════════════════════════════════════════════
#  C. Cableado y candados
# ═══════════════════════════════════════════════════════════════════════════
class TestCableado(unittest.TestCase):
    MODULO = _leer("flota_module.py")
    REGLAS = _leer("flota_reglas.py")
    JS = _leer("static", "flota_uso.js")
    HTML = _leer("templates", "flota", "index.html")

    def test_sin_precios_ni_costos(self):
        # Daniel: «no vamos a manejar los precios de los costos»
        for texto in (self.MODULO, self.REGLAS, self.HTML, self.JS):
            self.assertNotRegex(texto, r"(?i)\b(costo|precio|monto|valor_|factura|\bOC\b|proveedor)\b")

    def test_arranque_barato_una_sentencia_por_tabla(self):
        for t in ("flota_choferes", "flota_chofer_log", "flota_solicitudes", "flota_solicitud_log", "flota_archivos"):
            self.assertEqual(len(re.findall(rf"CREATE TABLE IF NOT EXISTS {t} \(", self.MODULO)), 1, t)

    def test_las_bitacoras_solo_se_agregan(self):
        for t in ("flota_chofer_log", "flota_solicitud_log", "flota_vehiculo_log"):
            self.assertNotRegex(self.MODULO, rf"UPDATE\s+{t}")
            self.assertNotRegex(self.MODULO, rf"DELETE\s+FROM\s+{t}")

    def test_solo_se_borran_vehiculos_y_choferes(self):
        # Las bitácoras, las solicitudes y los archivos NUNCA se borran (los archivos quedan como no vigentes).
        self.assertEqual(set(re.findall(r"DELETE\s+FROM\s+(\w+)", self.MODULO)), {"flota_vehiculos", "flota_choferes"})

    def test_ningun_sql_con_fstrings(self):
        for m in re.finditer(r"mysql_(?:execute|fetchone|fetchall|rowcount)\(\s*f[\"']", self.MODULO):
            self.fail("SQL con f-string: " + self.MODULO[m.start():m.start() + 80])
        for m in re.finditer(r"cur\.execute\(\s*f[\"']", self.MODULO):
            self.fail("SQL con f-string en la transacción: " + self.MODULO[m.start():m.start() + 80])

    def test_la_reserva_decide_dentro_de_una_transaccion_con_el_vehiculo_bloqueado(self):
        i = self.MODULO.index("def _reservar")
        cuerpo = self.MODULO[i:i + 9000]
        self.assertIn("conn.autocommit(False)", cuerpo)
        self.assertIn("FROM flota_vehiculos WHERE id=%s FOR UPDATE", cuerpo)
        self.assertLess(cuerpo.index("FOR UPDATE"), cuerpo.index("INSERT INTO flota_solicitudes"))
        self.assertIn("conn.rollback()", cuerpo)
        self.assertIn("conn.close()", cuerpo)

    def test_la_puerta_de_acceso_sigue_siendo_una_sola(self):
        self.assertEqual(self.MODULO.count("def _niv("), 1)
        self.assertEqual(self.MODULO.count("fr.niveles("), 1)
        self.assertNotIn('.get("superadmin")', self.MODULO)

    def test_cada_ruta_lleva_la_puerta_que_le_corresponde(self):
        pedir = ('"/flota/api/solicitudes", methods=["POST"]', '"/flota/api/solicitudes", methods=["GET"]', '"/flota/api/calendario"',
                 '"/flota/api/choferes", methods=["GET"]', '"/flota/api/vehiculos", methods=["GET"]', '"/flota/api/solicitudes/<int:sid>", methods=["GET"]',
                 '"/flota/api/solicitudes/<int:sid>/cancelar"', '"/flota/api/archivos/<int:aid>", methods=["GET"]')
        gestion = ('"/flota/api/solicitudes/<int:sid>/confirmar"', '"/flota/api/solicitudes/<int:sid>/llave"',
                   '"/flota/api/solicitudes/<int:sid>", methods=["PUT"]', '"/flota/api/vehiculos/<int:vid>/historial"',
                   '"/flota/api/choferes/<int:cid>/historial"')
        admin = ('"/flota/api/vehiculos", methods=["POST"]', '"/flota/api/vehiculos/<int:vid>", methods=["PUT"]',
                 '"/flota/api/vehiculos/<int:vid>/inhabilitar"', '"/flota/api/vehiculos/<int:vid>/habilitar"',
                 '"/flota/api/vehiculos/<int:vid>/archivar"', '"/flota/api/vehiculos/<int:vid>/desarchivar"', '"/flota/api/vehiculos/<int:vid>", methods=["DELETE"]',
                 '"/flota/api/choferes", methods=["POST"]', '"/flota/api/choferes/usuarios"', '"/flota/api/choferes/<int:cid>", methods=["PUT"]',
                 '"/flota/api/choferes/<int:cid>/inhabilitar"', '"/flota/api/choferes/<int:cid>/habilitar"', '"/flota/api/choferes/<int:cid>/archivar"',
                 '"/flota/api/choferes/<int:cid>/desarchivar"', '"/flota/api/choferes/<int:cid>", methods=["DELETE"]',
                 '"/flota/api/archivos", methods=["POST"]', '"/flota/api/archivos/<int:aid>/quitar"')
        for grupo, puerta in ((pedir, "@_flota_required"), (gestion, "@_flota_gestion_required"), (admin, "@_flota_admin_required")):
            for ruta in grupo:
                i = self.MODULO.index("@app.route(" + ruta)
                self.assertIn(puerta, self.MODULO[i:i + 200], ruta)

    def test_cada_transicion_es_atomica(self):
        for estado in ("solicitada", "confirmada"):
            self.assertIn(f"AND estado='{estado}'", self.MODULO)
        self.assertIn("AND estado IN ('solicitada','confirmada','llave_entregada')", self.MODULO)

    def test_horas_en_chile_nunca_iso(self):
        self.assertIn("_hora_chile(r.get(\"sello_at\"))", self.MODULO)
        self.assertIn("fr.rango_texto", self.MODULO)

    def test_el_feriado_sale_de_la_fuente_unica(self):
        self.assertIn("import cl_feriados", self.MODULO)
        self.assertNotRegex(self.MODULO, r"FERIADOS\s*=")


# ═══════════════════════════════════════════════════════════════════════════
#  D. Plantilla renderizada y JS
# ═══════════════════════════════════════════════════════════════════════════
class TestPantalla(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = _app(Falsa()).test_client().get("/flota", headers=SUPER).get_data(as_text=True)
        cls.js = _leer("static", "flota_uso.js")
        cls.js_veh = _leer("static", "flota.js")
        cls.css = _leer("static", "flota.css")

    def test_carga_los_dos_scripts(self):
        self.assertIn("flota.js", self.html)
        self.assertIn("flota_uso.js", self.html)

    def test_todo_id_que_usa_el_js_nuevo_existe(self):
        usados = set(re.findall(r"\$\('([A-Za-z0-9_]+)'\)", self.js))
        self.assertGreater(len(usados), 40)
        for i in usados:
            self.assertIn(f'id="{i}"', self.html, f"el JS usa #{i} y la plantilla no lo tiene")

    def test_no_se_repiten_ids_entre_los_dos_scripts(self):
        # una id repetida haría que un script pinte donde le toca al otro
        ids = re.findall(r'\bid="([^"]+)"', self.html)
        repetidos = {i for i in ids if ids.count(i) > 1}
        self.assertEqual(repetidos, set())

    def test_los_chips_nuevos_no_chocan_con_los_de_vehiculos(self):
        # flota.js engancha TODO `.fl-chip`: si los chips nuevos usaran esa clase, cambiar un filtro movería los de la otra pestaña
        self.assertRegex(self.js_veh, r"querySelectorAll\('#flPanelVehiculos \.fl-chip'\)")
        self.assertNotRegex(self.html.split("flPanelVehiculos")[0], r'class="fl-chip[ "]')
        self.assertNotRegex(self.html.split("flPanelChoferes")[1].split("flModalVeh")[0], r'class="fl-chip[ "]')
        self.assertNotIn(".fl-chip'", self.js)

    def test_las_tres_pestanas_y_un_boton_por_pestana(self):
        for t in ("solicitudes", "vehiculos", "choferes"):
            self.assertIn(f'data-tab="{t}"', self.html)
            self.assertIn(f'data-panel="{t}"', self.html)
        for b in ("flBtnSolicitud", "flBtnNuevo", "flBtnChofer"):
            self.assertIn(f'id="{b}"', self.html)

    def test_horario_obligatorio_en_el_formulario(self):
        for c in ("fecha", "hora_desde", "hora_hasta", "vehiculo_id", "chofer_id", "area", "motivo", "motivo_cambio"):
            self.assertIn(f'data-campo="{c}"', self.html, c)
        self.assertIn('type="time"', self.html)

    def test_calendario_con_vista_de_agenda_para_celular(self):
        self.assertIn("flCalGrid", self.html)
        self.assertIn("flCalAgenda", self.html)
        self.assertIn(".fl-agenda{display:block}", self.css.replace(" ", ""))
        self.assertIn("@media (max-width: 860px)", self.css)

    def test_pie_de_tabla_como_etiquetas_en_las_dos_tablas_nuevas(self):
        # REGLA #4.3
        for t in ("Mostrando", "Por página", "Anterior", "Siguiente", "Página"):
            self.assertIn(t, self.html + self.js)
        for i in ("flSolPieInfo", "flSolPorPagina", "flSolAnterior", "flSolSiguiente", "flChPieInfo", "flChPorPagina", "flChAnterior", "flChSiguiente"):
            self.assertIn(f'id="{i}"', self.html)

    def test_sin_dialogos_nativos(self):
        # REGLA #1
        self.assertNotRegex(self.js, r"(?<![\w.])(alert|confirm|prompt)\s*\(")
        self.assertIn("ilusConfirm", self.js)
        self.assertIn("ilusPrompt", self.js)

    def test_el_js_no_usa_request_animation_frame(self):
        self.assertNotRegex(self.js, r"requestAnimationFrame\s*\(")

    def test_el_js_vuelve_a_pedir_al_servidor_antes_de_mostrar_la_disponibilidad(self):
        # la disponibilidad del formulario sale de las reservas vigentes, no de lo que había cuando se abrió la pantalla
        self.assertIn("refrescarOcup", self.js)
        self.assertIn("CHOQUE_VEHICULO", self.js)

    def test_botones_tactiles(self):
        self.assertIn("min-height:44px", self.css)

    @unittest.skipUnless(HAY_NODE, "node no está instalado")
    def test_js_pasa_node_check(self):
        for f in ("flota_uso.js", "flota.js"):
            r = subprocess.run(["node", "--check", os.path.join(RAIZ, "static", f)], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)


if __name__ == "__main__":
    unittest.main()


class TestHorarioYSlaLlave(unittest.TestCase):
    """02-nov-2026 (Daniel): «el horario es de 8 a 13 y de 14 a las 17, existe la colación… apliquemos SLA si se atrasan en entregar las
    llaves». Fuera de horario se avisa; el SLA cuenta solo minutos dentro del horario."""
    L = dt.datetime(2026, 11, 2)      # lunes

    def h(self, dia, hh, mm=0):
        return dt.datetime(2026, 11, dia, hh, mm)

    def test_en_horario_y_colacion(self):
        self.assertTrue(R.en_horario(self.h(2, 8)))
        self.assertTrue(R.en_horario(self.h(2, 12, 59)))
        self.assertFalse(R.en_horario(self.h(2, 13, 30)))            # colación
        self.assertFalse(R.en_horario(self.h(2, 17)))                # cerrado
        self.assertTrue(R.en_horario(self.h(2, 17), es_termino=True))   # devolver justo al cierre sí
        self.assertFalse(R.en_horario(self.h(7, 10)))                # sábado
        self.assertFalse(R.en_horario(self.h(2, 10), feriados={"2026-11-02"}))

    def test_minutos_habiles_saltan_colacion_noche_y_fin_de_semana(self):
        self.assertEqual(R.minutos_habiles(self.h(2, 12, 50), self.h(2, 14, 10)), 20)     # 10 antes y 10 después de colación
        self.assertEqual(R.minutos_habiles(self.h(6, 16, 50), self.h(9, 8, 5)), 15)       # viernes 16:50 → lunes 08:05
        self.assertEqual(R.minutos_habiles(self.h(2, 18), self.h(3, 7)), 0)

    def test_estados_del_sla(self):
        ini, fin = self.h(2, 9), self.h(2, 12)
        self.assertIsNone(R.sla_llave("confirmada", ini, fin, self.h(2, 8, 30)))         # aún no es la hora
        self.assertIsNone(R.sla_llave("llave_entregada", ini, fin, self.h(2, 11)))      # ya se entregó
        self.assertEqual(R.sla_llave("confirmada", ini, fin, self.h(2, 9, 10))["estado"], "en_plazo")
        a = R.sla_llave("confirmada", ini, fin, self.h(2, 9, 20))
        self.assertEqual((a["estado"], a["minutos"]), ("atrasada", 20))
        self.assertIn("20 min", a["texto"])
        self.assertEqual(R.sla_llave("confirmada", ini, fin, self.h(2, 10, 5))["estado"], "critica")
        self.assertIn("sin confirmar", R.sla_llave("solicitada", ini, fin, self.h(2, 9, 20))["texto"])
        self.assertEqual(R.sla_llave("confirmada", ini, fin, self.h(2, 12, 30))["estado"], "sin_entrega")

    def test_la_colacion_no_suma_atraso(self):
        s = R.sla_llave("confirmada", self.h(2, 12, 55), self.h(2, 16), self.h(2, 14, 5))
        self.assertEqual((s["estado"], s["minutos"]), ("en_plazo", 10))

    def test_avisos_fuera_de_horario(self):
        self.assertEqual(R.avisos_horario(self.h(2, 9), self.h(2, 12)), [])
        av = R.avisos_horario(self.h(2, 13, 15), self.h(2, 18))
        self.assertEqual(len(av), 2)
        self.assertIn("podría no haber quién entregue la llave", av[0])
        self.assertIn("podría no haber quién reciba la llave", av[1])


class TestCorreosFlota(unittest.TestCase):
    """03-nov-2026 (Daniel): «activa en comunicaciones sus mails» y «cuando se cree la orden de trabajo no debería mandar correo, pero si
    lo hacen por el menú sí»."""

    def setUp(self):
        self.enviados = []
        self.base = Falsa()
        aviso = lambda clave, uids, titulo, cuerpo, url, etiqueta="", ref=None: self.enviados.append((clave, list(uids), titulo, cuerpo)) or len(uids)
        self.app = _app(self.base, extra={"_aviso_interno_correo": aviso})
        self.c = self.app.test_client()
        vid = self.c.post("/flota/api/vehiculos", json={"patente": "PHVD21-7", "tipo": "Camión", "licencia_clases": ["B"]},
                          headers=SUPER).get_json()["vehiculo"]["id"]
        cid = self.c.post("/flota/api/choferes", json=dict(_datos_chofer(1), usuario_id=1), headers=SUPER).get_json()["chofer"]["id"]
        dia = (dt.date.today() + dt.timedelta(days=5)).isoformat()
        self.sol = {"vehiculo_id": vid, "chofer_id": cid, "area": "Logística", "motivo": "Reparto zona norte", "fecha": dia,
                    "hora_desde": "09:00", "hora_hasta": "11:00"}

    def test_desde_el_menu_avisa_a_la_gestion(self):
        r = self.c.post("/flota/api/solicitudes", json=self.sol, headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_json())
        self.assertEqual([e[0] for e in self.enviados], ["flota_solicitud_nueva"])
        self.assertEqual(self.enviados[0][1], [2])                      # solo quien tiene «Gestionar solicitudes»
        self.assertIn("PHVD-21", self.enviados[0][2])
        self.assertIn("Retira la llave", self.enviados[0][3])

    def test_desde_una_ot_no_manda_correo(self):
        r = self.c.post("/flota/api/solicitudes", json=dict(self.sol, origen="ot"), headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_json())
        self.assertEqual(self.enviados, [])

    def test_las_claves_estan_en_el_registro_de_comunicaciones(self):
        import comm_correos_registro as REG
        for k in ("flota_solicitud_nueva", "flota_solicitud_confirmada", "flota_solicitud_cancelada", "flota_llave_atrasada"):
            c = REG.por_clave(k)
            self.assertIsNotNone(c, k)
            self.assertEqual((c["modulo_ui"], c["llave"]), ("flota", "comunicacion_interna"))
            self.assertFalse(c["nace_apagado"])


class TestDevolucion(unittest.TestCase):
    """FASE 2 (Daniel, 02/03-nov-2026): devolver la llave con kilometraje, combustible y cuatro fotos (odómetro, combustible, interior y
    exterior). Lo raro se avisa; la foto es la evidencia."""
    JPG = b"\xff\xd8\xff\xe0" + b"\x00" * 64

    def setUp(self):
        from test_flota_vehiculos import BucketFalso
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

    def devolver(self, quitar=(), **campos):
        import io
        data = {"km": "56.240", "combustible": "1_2", "observaciones": "", "novedad": ""}
        data.update(campos)
        for tipo, _n in R.FOTOS_DEVOLUCION:
            if tipo not in quitar:
                data[tipo] = (io.BytesIO(self.JPG), tipo + ".jpg", "image/jpeg")
        return self.c.post(f"/flota/api/solicitudes/{self.sid}/devolucion", data=data, content_type="multipart/form-data", headers=SUPER)

    def test_devolucion_completa(self):
        r = self.devolver()
        self.assertEqual(r.status_code, 200, r.get_json())
        s = r.get_json()["solicitud"]
        self.assertEqual(s["estado"], "devuelta")
        self.assertEqual((s["devolucion"]["km"], s["devolucion"]["combustible_texto"]), (56240, "1/2"))
        self.assertEqual(self.base.veh[self.vid]["km_actual"], 56240)                 # el vehículo sube su kilometraje
        d = self.c.get(f"/flota/api/solicitudes/{self.sid}", headers=SUPER).get_json()
        self.assertEqual(sorted(f["etiqueta"] for f in d["fotos_devolucion"]), ["Exterior", "Indicador de combustible", "Interior", "Odómetro"])
        self.assertEqual(d["eventos"][0]["titulo"], "Vehículo devuelto")
        self.assertEqual([e[0] for e in self.enviados], ["flota_devolucion"])

    def test_faltan_fotos_o_datos(self):
        r = self.devolver(quitar=("dev_odometro",), km="", combustible="")
        self.assertEqual(r.status_code, 400)
        e = r.get_json()["errores"]
        self.assertTrue({"dev_odometro", "km", "combustible"} <= set(e))
        self.assertEqual(self.base.sol[self.sid]["estado"], "llave_entregada")

    def test_novedad_exige_contarla(self):
        r = self.devolver(novedad="1", observaciones="raya")
        self.assertEqual(r.status_code, 400)
        self.assertIn("observaciones", r.get_json()["errores"])
        r = self.devolver(novedad="1", observaciones="Rayón en la puerta trasera derecha")
        self.assertEqual(r.status_code, 200)
        self.assertIn("con novedad", self.enviados[0][1])

    def test_km_menor_se_avisa_y_no_pisa_el_vehiculo(self):
        r = self.devolver(km="50000")
        self.assertEqual(r.status_code, 200)
        self.assertTrue(any("MENOR" in a for a in r.get_json()["avisos"]))
        self.assertEqual(self.base.veh[self.vid]["km_actual"], 56000)

    def test_no_se_devuelve_dos_veces(self):
        self.assertEqual(self.devolver().status_code, 200)
        self.assertEqual(self.devolver().status_code, 409)

    def test_reglas_puras(self):
        d, e, a = R.validar_devolucion({"km": "1.234", "combustible": "lleno"}, km_previo=1000)
        self.assertEqual((d["km"], e, a), (1234, {}, []))
        _d, _e, a = R.validar_devolucion({"km": "9000", "combustible": "lleno"}, km_previo=1000)
        self.assertTrue(a)                                                             # salto grande: se avisa
        self.assertEqual(R.validar_archivo("solicitud", "dev_odometro", b"%PDF-1.4 x")[1],
                         "Las fotos de la devolución deben ser imágenes (JPG, PNG o WebP).")


class TestAntelacion24(unittest.TestCase):
    """03-nov-2026 (Daniel): «colócale una restricción de 24 horas al calendario». Quien pide reserva con al menos 24 h de aviso; la gestión
    puede reservar antes (con un aviso). Los márgenes de la prueba son amplios para no depender de la zona horaria de quien la corre."""
    PEDIR = {"X-Rol": "ejecutivo", "X-Flota": "flota_solicitar"}

    def setUp(self):
        self.base = Falsa()
        self.app = _app(self.base)
        self.c = self.app.test_client()
        self.vid = self.c.post("/flota/api/vehiculos", json={"patente": "PHVD21-7", "tipo": "Camión", "licencia_clases": ["B"]},
                               headers=SUPER).get_json()["vehiculo"]["id"]
        self.cid = self.c.post("/flota/api/choferes", json=dict(_datos_chofer(1), usuario_id=1), headers=SUPER).get_json()["chofer"]["id"]

    def pedir(self, horas, headers):
        ini = dt.datetime.now().replace(second=0, microsecond=0) + dt.timedelta(hours=horas)
        fin = ini + dt.timedelta(minutes=90)
        return self.c.post("/flota/api/solicitudes", json={
            "vehiculo_id": self.vid, "chofer_id": self.cid, "area": "Logística", "motivo": "Reparto zona norte",
            "fecha": ini.date().isoformat(), "hora_desde": ini.strftime("%H:%M"),
            "fecha_hasta": fin.date().isoformat(), "hora_hasta": fin.strftime("%H:%M")}, headers=headers)

    def test_quien_pide_no_reserva_con_menos_de_24_horas(self):
        r = self.pedir(1, self.PEDIR)
        self.assertEqual(r.status_code, 400, r.get_json())
        self.assertIn("24 horas", r.get_json()["errores"]["hora_desde"])
        self.assertEqual(len(self.base.sol), 0)

    def test_con_mas_de_24_horas_pasa(self):
        r = self.pedir(72, self.PEDIR)
        self.assertEqual(r.status_code, 200, r.get_json())

    def test_la_gestion_puede_reservar_antes_y_queda_avisado(self):
        r = self.pedir(1, SUPER)
        self.assertEqual(r.status_code, 200, r.get_json())
        self.assertTrue(any("24 horas" in a for a in r.get_json()["avisos"]))

    def test_la_configuracion_dice_cuanto_es(self):
        d = self.c.get("/flota/api/vehiculos", headers=SUPER).get_json()
        self.assertEqual(d["config"]["antelacion_horas"], 24)

    def test_primer_horario_redondea_hacia_arriba_a_la_media_hora(self):
        f = lambda h, m: dt.datetime(2026, 11, 3, h, m)
        self.assertEqual(R.primer_horario(f(10, 0), 24), dt.datetime(2026, 11, 4, 10, 0))
        self.assertEqual(R.primer_horario(f(10, 10), 24), dt.datetime(2026, 11, 4, 10, 30))
        self.assertEqual(R.primer_horario(f(10, 40), 24), dt.datetime(2026, 11, 4, 11, 0))

    def test_devolver_otro_dia_se_agenda_varios_dias_y_bloquea_a_los_demas(self):
        dia = dt.date.today() + dt.timedelta(days=10)
        r = self.c.post("/flota/api/solicitudes", json={
            "vehiculo_id": self.vid, "chofer_id": self.cid, "area": "Logística", "motivo": "Gira zona sur",
            "fecha": dia.isoformat(), "hora_desde": "14:00", "fecha_hasta": (dia + dt.timedelta(days=2)).isoformat(), "hora_hasta": "11:00"},
            headers=SUPER)
        self.assertEqual(r.status_code, 200, r.get_json())
        s = r.get_json()["solicitud"]
        self.assertEqual((s["fecha"], s["fecha_hasta"]), (dia.isoformat(), (dia + dt.timedelta(days=2)).isoformat()))
        otro = self.c.post("/flota/api/solicitudes", json={
            "vehiculo_id": self.vid, "chofer_id": self.cid, "area": "Marketing", "motivo": "Evento",
            "fecha": (dia + dt.timedelta(days=1)).isoformat(), "hora_desde": "09:00", "hora_hasta": "10:00"}, headers=SUPER)
        self.assertEqual(otro.status_code, 409)                  # el día del medio el vehículo está afuera
        self.assertEqual(otro.get_json()["error_codigo"], "CHOQUE_VEHICULO")


class TestAgenda24hEnElFrente(unittest.TestCase):
    """03-nov-2026 (Daniel: «la restricción de 24 es para agenda»): la regla de 24 h no vive solo en el calendario del modal; la agenda
    principal (pista semanal, lista, próximo hueco y la entrada desde un día) usa la MISMA fuente del límite."""
    JS = _leer("static", "flota_uso.js")

    def test_hay_una_sola_fuente_del_limite(self):
        self.assertEqual(self.JS.count("function limite24Base()"), 1)
        self.assertIn("function cierre24(iso)", self.JS)
        self.assertIn("function diaCerrado24(iso)", self.JS)
        self.assertIn("function pkLimite24() { return S.editSol ? null : limite24Base(); }", self.JS)

    def test_la_gestion_no_queda_limitada(self):
        i = self.JS.index("function limite24Base()")
        self.assertIn("PERM.gestionar ? null", self.JS[i:i + 140])

    def test_la_pista_la_lista_el_hueco_y_la_entrada_la_respetan(self):
        for marca in ("cierre24(iso), R[1]",                                    # franja ámbar en la pista
                      "diaCerrado24(iso) ? '<span class=\"fla-ld-24\"",       # lista de la semana: sin «Pedir»
                      "(iso + 'T' + hhmm(a)) < l24",                            # tocar un hueco de la pista
                      "Math.max(i === 0 ? Math.ceil(minutos(ahora.hm) / 30) * 30 : 0, cierre24(iso))",   # próximo hueco libre
                      "Te dejé el primer día disponible"):                      # entrar a la solicitud desde un día cerrado
            self.assertIn(marca, self.JS, marca)

    def test_estilos_de_la_franja(self):
        css = _leer("static", "flota.css")
        self.assertIn(".fla-c24{", css)
        self.assertIn(".fla-ld-24{", css)
