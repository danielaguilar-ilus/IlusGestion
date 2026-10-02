# -*- coding: utf-8 -*-
"""Extensión del arnés de Retiros para probar la Guía de 6 pasos y la preparación por Check.

Se apoya en tests/_arnes_retiros.py (registra las rutas REALES con register_pickup_routes sobre un
ctx de mentira) y le agrega lo que ese borrador no tenía:

  · BDProd      BD falsa más fiel a producción:
                  - exige CONTEXTO DE APP en cada consulta (en producción mysql_fetchall/mysql_execute
                    van por get_db(), que usa flask.g: un hilo suelto sin app.app_context() revienta
                    con «Working outside of application context»). El borrador no lo exigía y por eso
                    no podía ver ese tipo de bug.
                  - responde pickup_settings, app_users (campana interna) y cualquier
                    «SELECT … FROM pickup_requests WHERE id=%s».
                  - lock para los hilos del aviso interno y del barrido.
                  - fallas de escritura programables (db.falla_escritura_si).
  · construir() igual que construir_app(), pero con BDProd, el ERP espiado (no debe tocarse) y el
                equipo interno configurable.

Nada de esto habla con MySQL, Check, el ERP ni con correo: todo queda registrado para poder afirmar
«esto NO se llamó».
"""
import os
import re
import sys
import threading
from unittest.mock import MagicMock

AQUI = os.path.dirname(os.path.abspath(__file__))
if AQUI not in sys.path:
    sys.path.insert(0, AQUI)

import _arnes_retiros as A  # noqa: E402
from flask import has_app_context  # noqa: E402

DIRS_TEMPORALES = []    # carpetas que el arnés crea por cada app (static/uploads/retiros); se borran al terminar

CLIENTE_EMAIL = "cliente.real@example.com"     # el cliente real de producción (nunca debe recibir nada)
EQUIPO_EMAIL = "equipo@sphs.cl"                 # lista interna «aviso_equipo_emails»


def _ajustes(equipo):
    """Fila de pickup_settings que NO dispara la corrección de «drift» de settings()."""
    return {"id": 1, "warehouse_name": "Bodega ILUS", "warehouse_addr": "Quilicura", "maps_url": "",
            "open_time": "09:00:00", "close_time": "16:30:00", "lunch_start": "12:30:00",
            "lunch_end": "14:00:00", "slot_minutes": 30, "buffer_cierre_min": 0,
            "parallel_capacity": 2, "min_notice_hours": 24, "proposal_expiry_hours": 48,
            "work_days": "1,2,3,4,5", "holidays": "", "alert_enabled": 0, "alert_title": "", "alert_message": "",
            "notify_emails": "", "web_responsable_user_id": None,
            "aviso_equipo_emails": ",".join(equipo), "vispera_activa": 0, "vispera_hasta": "15:00"}


class BDProd(A.BDFalsa):
    def __init__(self, equipo=()):
        super().__init__()
        self.exigir_contexto = False             # se activa al terminar de registrar las rutas
        self.lock = threading.RLock()
        self.ajustes = _ajustes(equipo)
        self.admins = []                         # filas {'id':…} de app_users activos (campana interna)
        self.falla_escritura_si = []             # [(regex, Excepcion)] sobre el SQL de un INSERT/UPDATE

    def _guardia(self):
        if self.exigir_contexto and not has_app_context():
            raise RuntimeError("Working outside of application context.")

    # ── lecturas ──────────────────────────────────────────────────────────
    def fetchall(self, sql, params=()):
        self._guardia()
        with self.lock:
            s = A._n(sql)
            low = s.lower()
            if "from `app_users`" in low or "from app_users" in low:
                self.consultas.append((s, params))
                return [dict(a) for a in self.admins]
            return super().fetchall(sql, params)

    def fetchone(self, sql, params=()):
        self._guardia()
        with self.lock:
            s = A._n(sql)
            low = s.lower()
            if low.startswith("select * from `pickup_settings` where id=1"):
                self.consultas.append((s, params))
                return dict(self.ajustes)
            if re.match(r"^select .+ from `pickup_requests` where id=%s$", low):
                self.consultas.append((s, params))
                fila = self.solicitudes.get(int(params[0]))
                return dict(fila) if fila else None
            if low.startswith("select username from `app_users`"):
                self.consultas.append((s, params))
                for a in self.admins:
                    if a.get("id") == params[0]:
                        return {"username": a.get("username")}
                return None
            return super().fetchone(sql, params)

    # ── escrituras ────────────────────────────────────────────────────────
    def execute(self, sql, params=()):
        self._guardia()
        with self.lock:
            s = A._n(sql)
            for patron, exc in self.falla_escritura_si:
                if re.search(patron, s, re.I):
                    raise exc
            return super().execute(sql, params)

    # ── utilidades para las pruebas ───────────────────────────────────────
    def escrituras_en(self, tabla):
        t = tabla.lower()
        return [e for e in self.escrituras if t in e[0].lower()]

    def eventos(self, rid, action=None):
        return self.logs_de(rid, action)


def construir(usuario=None, equipo=(EQUIPO_EMAIL,)):
    """(app, db, ctx, esp) con las rutas reales de Retiros. `esp.erp` es un espía del ERP Random: las
    rutas de la guía y de Check NO deben llamarlo (REGLA #4.1)."""
    original = A.BDFalsa
    A.BDFalsa = lambda: BDProd(equipo)
    try:
        app, db, ctx, esp = A.construir_app(usuario)
    finally:
        A.BDFalsa = original
    DIRS_TEMPORALES.append(ctx["BASE_DIR"])
    esp.erp = MagicMock(name="ERP Random (no debe tocarse)")
    ctx["_random_sql_query"] = esp.erp
    ctx["_random_sql_pool"] = esp.erp
    ctx["erp_engine"] = esp.erp
    db.exigir_contexto = True
    return app, db, ctx, esp
