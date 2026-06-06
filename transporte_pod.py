"""
Panel de Configuración POD (Prueba de Entrega) por courier.

Permite a Daniel configurar, desde la ficha del courier y sin tocar BD,
qué evidencia se exige al chofer al cerrar una entrega:

  - pod_fotos_min    cuántas fotos mínimas debe tomar (0-10)
  - pod_firma_req    si debe capturar firma del receptor (0/1)
  - pod_rut_req      si debe capturar RUT del receptor (0/1)
  - pod_geocerca_m   radio (metros) de la geocerca de entrega
                     (0 = desactivada, máx 5000)

Las columnas YA EXISTEN en `transport_couriers` (las agregó otra parte
del sistema). Este módulo SOLO consulta / actualiza.

Wire-in desde app.py:

    import transporte_pod
    transporte_pod.register_pod_routes(app, globals())

Estilo: DispatchTrack — paleta ILUS, mobile-first, sin alert nativo.
"""


def register_pod_routes(app, ctx):
    """Registra los endpoints del panel POD en la app Flask.

    ctx = globals() de app.py — de ahí sacamos los helpers compartidos.
    """
    mysql_fetchone  = ctx["mysql_fetchone"]
    mysql_execute   = ctx["mysql_execute"]
    jsonify         = ctx["jsonify"]
    request         = ctx["request"]
    render_template = ctx["render_template"]
    _tr_required    = ctx["_tr_required"]
    # current_username y _tr_log son opcionales — si no están, seguimos sin log.
    current_username = ctx.get("current_username") or (lambda: "sistema")
    _tr_log = ctx.get("_tr_log")

    # ── helpers de validación ───────────────────────────────────────────
    def _clamp_int(value, lo, hi, default):
        """Convierte a int dentro de [lo, hi]. Si falla, devuelve default."""
        try:
            n = int(value)
        except (TypeError, ValueError):
            return default
        if n < lo:
            return lo
        if n > hi:
            return hi
        return n

    def _to_bool(value):
        """Acepta True/False, 1/0, '1'/'0', 'true'/'false', 'on'/'off'."""
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(int(value))
        s = str(value or "").strip().lower()
        return s in ("1", "true", "yes", "on", "si", "sí")

    # ── GET: render del panel ───────────────────────────────────────────
    @app.route("/transporte/couriers/<int:cid>/pod", methods=["GET"])
    @_tr_required
    def tr_courier_pod_config(cid):
        courier = mysql_fetchone(
            "SELECT id, nombre, "
            "       COALESCE(pod_fotos_min, 1)   AS pod_fotos_min, "
            "       COALESCE(pod_firma_req, 1)   AS pod_firma_req, "
            "       COALESCE(pod_rut_req, 1)     AS pod_rut_req, "
            "       COALESCE(pod_geocerca_m, 0)  AS pod_geocerca_m "
            "FROM transport_couriers WHERE id=%s",
            (cid,),
        )
        if not courier:
            from flask import abort
            abort(404)
        return render_template(
            "transporte/courier_pod_config.html",
            courier=courier,
        )

    # ── POST: guarda la configuración ───────────────────────────────────
    @app.route("/transporte/couriers/<int:cid>/pod", methods=["POST"])
    @_tr_required
    def tr_courier_pod_config_save(cid):
        # Validamos que el courier existe ANTES de tocar nada.
        row = mysql_fetchone(
            "SELECT id, nombre FROM transport_couriers WHERE id=%s",
            (cid,),
        )
        if not row:
            return jsonify({"ok": False, "error": "Courier no encontrado"}), 404

        data = request.get_json(silent=True) or {}

        fotos    = _clamp_int(data.get("fotos_min"),    0, 10,   1)
        geocerca = _clamp_int(data.get("geocerca_m"),   0, 5000, 0)
        firma    = 1 if _to_bool(data.get("firma_req")) else 0
        rut      = 1 if _to_bool(data.get("rut_req"))   else 0

        try:
            mysql_execute(
                "UPDATE transport_couriers SET "
                "  pod_fotos_min   = %s, "
                "  pod_firma_req   = %s, "
                "  pod_rut_req     = %s, "
                "  pod_geocerca_m  = %s "
                "WHERE id = %s",
                (fotos, firma, rut, geocerca, cid),
            )
        except Exception as e:
            print(f"[tr_courier_pod_config_save] UPDATE error cid={cid}: {e}", flush=True)
            return jsonify({
                "ok": False,
                "error": "No se pudo guardar la configuración. Intenta nuevamente.",
            }), 500

        # Log de auditoría (best-effort, nunca rompe el endpoint).
        if _tr_log:
            try:
                detalle = (
                    f"POD config: fotos_min={fotos}, firma={firma}, "
                    f"rut={rut}, geocerca_m={geocerca} "
                    f"(by {current_username() or 'sistema'})"
                )
                _tr_log("courier", cid, "pod_config_update", detalle)
            except Exception:
                pass

        return jsonify({
            "ok": True,
            "courier_id": cid,
            "fotos_min":  fotos,
            "firma_req":  firma,
            "rut_req":    rut,
            "geocerca_m": geocerca,
        })

    return app
