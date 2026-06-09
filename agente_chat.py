"""Agente CONVERSACIONAL determinista de la ficha del cliente ILUS (CERO IA).

Responde preguntas puntuales del usuario usando SOLO el dict ya calculado por
_cliente_inteligencia(cid). Funciona por intents: normaliza la pregunta, hace
match por palabras clave y arma una respuesta con los datos reales del cliente.

Diseñado por un panel de especialistas (comercial, técnico de servicio, abogado
de garantías/SERNAC, analista financiero, ejecutivo de cuenta). Sin IA, sin
tokens, instantáneo. Resalta los dolores de ILUS: cubrir gratis/garantías
mientras el cliente paga a terceros, servicios sin facturar, garantía mal asignada.
"""
import re
import unicodedata


def _norm(s):
    """minúsculas + sin tildes (ñ→n) + puntuación a espacios → tokens limpios."""
    s = (s or "").lower().strip()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _similar(a, b):
    """Tolerancia a typos (difflib, determinista). True si a≈b."""
    import difflib
    return difflib.SequenceMatcher(None, a, b).ratio() >= 0.82


def _fmt_fecha(s):
    """'YYYY-MM-DD[...]' → 'DD/MM/YYYY' (regla del proyecto). Si no matchea, devuelve el original."""
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", str(s or ""))
    return f"{m.group(3)}/{m.group(2)}/{m.group(1)}" if m else (str(s) if s else "")


def _clp(v):
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    if n is None:
        return None
    return "$" + format(int(round(n)), ",d").replace(",", ".")


def _g(d, path, default=None):
    """Acceso seguro a campos anidados: _g(d, 'diagnostico_contrato.frecuencia_meses')."""
    cur = d
    for k in path.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


def _cob(d, k):
    pc = _g(d, "historia.por_cobertura", {}) or {}
    try:
        return int(pc.get(k, 0) or 0)
    except (TypeError, ValueError):
        return 0


def _nombre(d):
    return _g(d, "cliente.razon_social") or "este cliente"


def _len(v):
    return len(v) if isinstance(v, list) else 0


# ── Respuestas por intent ────────────────────────────────────────────
def _r_gratis_o_se_cobra(d):
    tiene = _g(d, "tiene_contrato")
    inc = _g(d, "diagnostico_contrato.incluye_gratis")
    nom = _nombre(d)
    out = []
    if _cob(d, "Garantía") > _cob(d, "Contrato") and _cob(d, "Garantía") > 0:
        out.append("⚠ Ojo: hay más servicios marcados como GARANTÍA que por contrato — posible garantía mal asignada (revisar si alguno se debió cobrar).")
    if tiene and inc:
        gi = _g(d, "diagnostico_contrato.gratis_incluidas")
        va = _clp(_g(d, "diagnostico_contrato.valor_anual"))
        cub = _g(d, "brecha_gratis.cubiertas", 0)
        esp = _g(d, "brecha_gratis.esperadas", 0)
        pen = _g(d, "brecha_gratis.pendientes", 0)
        out.append(f"El contrato de {nom} INCLUYE {gi or '—'} mantención(es) gratis al año; esas NO se cobran"
                   + (f" (van dentro del valor anual de {va} ya pactado)" if va else "") + ".")
        out.append(f"Llevamos {cub} de {esp} entregadas; quedan {pen} por cumplir. Todo lo que pase de las gratis pactadas SE COBRA.")
    elif tiene and not inc:
        out.append("El contrato NO contempla mantenciones gratis: toda visita correctiva fuera del plan SE COBRA, salvo garantía de fábrica del equipo.")
    else:
        out.append(f"{nom} NO tiene contrato vigente: por regla toda mantención SE COBRA (cliente paga). Solo es gratis si el equipo está dentro de la garantía del fabricante — verifícalo antes de regalar mano de obra.")
    out.append("📌 Recuerda: si el equipo está en garantía, una reparación de un tercero la ANULA → ILUS debe ser el único autorizado a intervenir (cláusula de exclusividad).")
    return "\n".join(out)


def _r_cuanto_cobrar(d):
    total = _g(d, "valorizacion.total")
    nom = _nombre(d)
    if not total:
        return ("Aún no hay una valorización calculada para este cliente. Genera la cotización/levantamiento "
                "desde la ficha para obtener el monto sugerido.")
    out = []
    pitch = _g(d, "valorizacion.pitch")
    if pitch:
        out.append(pitch)
    out.append(f"Para {nom} la valorización sugerida es {_clp(total)} bruto.")
    dpct = _g(d, "valorizacion.descuento_pct")
    if dpct:
        neto = _clp(_g(d, "valorizacion.neto"))
        iva = _clp(_g(d, "valorizacion.iva"))
        out.append(f"Con {dpct}% de descuento queda en {neto} neto + {iva} IVA.")
    if _g(d, "tiene_contrato"):
        mm = _clp(_g(d, "diagnostico_contrato.monto_mensual"))
        va = _clp(_g(d, "diagnostico_contrato.valor_anual"))
        if mm or va:
            out.append(f"Hoy paga {mm or '—'}/mes ({va or '—'}/año); úsalo como piso de negociación.")
    out.append("💰 Es plata sobre la mesa → arma la cotización y ciérrala antes de que el cliente busque un tercero.")
    return "\n".join(out)


def _r_fuga_a_terceros(d):
    fuga = _cob(d, "Tercero (fuga)") or _cob(d, "Tercero")
    riesgo = _g(d, "brecha_gratis.riesgo_fuga_tercero")
    nom = _nombre(d)
    if fuga or riesgo:
        out = []
        if fuga:
            out.append(f"🔴 ALERTA DE FUGA: detectamos {fuga} gestión(es) cubierta(s) por un TERCERO en {nom}.")
            out.append(f"Patrón peligroso: el cliente paga mantención a la competencia y a nosotros solo nos deja la garantía gratis ({_cob(d,'Garantía')} casos) → regalamos mano de obra mientras la competencia factura.")
        else:
            out.append(f"⚠ Riesgo de fuga a terceros en {nom}: el patrón sugiere que podrían hacer mantenciones con externos.")
        exp = _clp(_g(d, "brecha_gratis.exposicion_clp"))
        if exp:
            out.append(f"Exposición estimada: {exp}.")
        msg = _g(d, "brecha_gratis.mensaje")
        if msg:
            out.append(msg)
        tot = _clp(_g(d, "valorizacion.total"))
        out.append(f"⚖ Una intervención de un tercero ANULA la garantía del fabricante. Recupera al cliente con un contrato"
                   + (f" (valorización {tot})" if tot else "") + " antes de que se consolide con el tercero.")
        return "\n".join(out)
    return (f"✓ Sin señales de fuga: la cobertura está concentrada en ILUS ({_cob(d,'Contrato')} por contrato/garantía). "
            "Buen momento para fidelizar con un contrato anual y cerrarle la puerta a la competencia.")


def _r_mantenciones_vencidas(d):
    venc = _g(d, "mantenciones.vencidas", []) or []
    n = _len(venc)
    real = _g(d, "mantenciones.realizadas", 0)
    esp = _g(d, "mantenciones.esperadas_a_hoy", 0)
    if n == 0:
        return f"✓ Sin mantenciones vencidas. Realizadas {real} de {esp} esperadas a hoy: al día."
    out = [f"🔴 Hay {n} mantención(es) VENCIDA(S). Llevas {real} realizadas de {esp} esperadas a hoy (déficit de {max(0,(esp or 0)-(real or 0))})."]
    for v in venc[:6]:
        if isinstance(v, dict):
            f = _fmt_fecha(v.get("fecha") or v.get("fecha_esperada")) or "—"
            da = v.get("dias_atraso")
            out.append(f"  • {f}" + (f" — {da} día(s) de atraso" if da is not None else ""))
    out.append("📅 Prográmalas para evitar exposición de SLA y que el cliente llame a un tercero.")
    return "\n".join(out)


def _r_proxima_mantencion(d):
    pf = _g(d, "mantenciones.proxima_fecha")
    prox = _g(d, "mantenciones.proximas", []) or []
    frec = _g(d, "diagnostico_contrato.frecuencia_meses")
    if pf:
        out = [f"📅 La próxima mantención está agendada para el {_fmt_fecha(pf)}."]
        for p in prox[:3]:
            if isinstance(p, dict) and p.get("fecha"):
                out.append(f"  • {_fmt_fecha(p.get('fecha'))}" + (f" — {p.get('titulo') or p.get('tipo') or ''}" if (p.get('titulo') or p.get('tipo')) else ""))
        return "\n".join(out)
    if frec:
        return f"No hay próxima mantención agendada, pero el contrato exige una cada {frec} meses — debes programarla."
    return "Este cliente no tiene mantenciones futuras agendadas ni frecuencia definida."


def _r_servicios_sin_facturar(d):
    pen = _g(d, "facturacion.pendientes", 0) or 0
    if pen == 0:
        return "✓ Facturación al día: no hay servicios ejecutados pendientes de cobro."
    ant = _g(d, "facturacion.mas_antigua_dias")
    out = [f"🧾 Hay {pen} servicio(s) ejecutado(s) SIN FACTURAR" + (f". El más antiguo lleva {ant} días esperando cobro." if ant is not None else ".")]
    for it in (_g(d, "facturacion.items", []) or [])[:5]:
        if isinstance(it, dict):
            lbl = it.get("tipo_label") or it.get("titulo") or it.get("fecha") or "servicio"
            out.append(f"  • {lbl}")
    if ant is not None and ant > 30:
        out.insert(0, "⏰ CRÍTICO: cobro detenido hace más de un mes, riesgo de quedar incobrable.")
    out.append("Cada servicio prestado y no facturado es ingreso que ILUS regaló. Emite el documento con respaldo de la OT antes de cerrar el período.")
    return "\n".join(out)


def _r_mantenciones_gratis_pendientes(d):
    if not _g(d, "tiene_contrato") or not _g(d, "diagnostico_contrato.incluye_gratis"):
        return "Este cliente no tiene mantenciones gratis comprometidas por contrato; no hay obligación pendiente."
    gi = _g(d, "diagnostico_contrato.gratis_incluidas")
    esp = _g(d, "brecha_gratis.esperadas", 0)
    cub = _g(d, "brecha_gratis.cubiertas", 0)
    pen = _g(d, "brecha_gratis.pendientes", 0)
    out = [f"El contrato obliga {gi or '—'} mantención(es) gratis al año. A hoy: esperadas {esp}, entregadas {cub}, PENDIENTES {pen}."]
    if pen and pen > 0:
        exp = _clp(_g(d, "brecha_gratis.exposicion_clp"))
        out.insert(0, f"🔴 OBLIGACIÓN INCUMPLIDA: debemos {pen} gratis. Ejecútalas antes de que el cliente llame a un tercero; riesgo de reclamo (Ley 19.496)."
                   + (f" Exposición: {exp}." if exp else ""))
    else:
        out.append("✓ Al día con las gratis pactadas. Cumplimiento OK.")
    return "\n".join(out)


def _r_salud_riesgo(d):
    sc = _g(d, "score_salud", "—")
    nr = (_g(d, "nivel_riesgo") or "medio").lower()
    emoji = {"alto": "🔴", "medio": "🟡", "bajo": "🟢"}.get(nr, "🟡")
    out = [f"{emoji} Salud de la cuenta: {sc}/100 — riesgo {nr}."]
    raz = []
    if _g(d, "brecha_gratis.riesgo_fuga_tercero") or _cob(d, "Tercero (fuga)"):
        raz.append(f"Fuga a terceros ({_cob(d,'Tercero (fuga)')} gestiones a otro proveedor).")
    if _g(d, "brecha_gratis.exposicion_clp"):
        raz.append(f"Exposición por gratuitas no controladas: {_clp(_g(d,'brecha_gratis.exposicion_clp'))} ({_g(d,'brecha_gratis.pendientes',0)} pendientes).")
    if _g(d, "facturacion.pendientes"):
        raz.append(f"{_g(d,'facturacion.pendientes')} servicios sin facturar.")
    if _len(_g(d, "mantenciones.vencidas", [])):
        raz.append(f"{_len(_g(d,'mantenciones.vencidas',[]))} mantenciones vencidas.")
    if not _g(d, "tiene_contrato") or (_g(d, "diagnostico_contrato.estado") in ("vencido", "por_vencer")):
        raz.append(f"Contrato {_g(d,'diagnostico_contrato.estado') or 'inexistente'}.")
    if raz:
        out.append("Razones:")
        out += [f"  • {r}" for r in raz]
    else:
        out.append("No hay factores de riesgo activos; el nivel proviene del score histórico.")
    return "\n".join(out)


def _r_resumen_general(d):
    sc = _g(d, "score_salud", "—")
    nr = (_g(d, "nivel_riesgo") or "medio").lower()
    out = [f"📊 Salud {sc}/100 — riesgo {nr}."]
    out.append("Contrato " + (_g(d, "diagnostico_contrato.estado") or "—") if _g(d, "tiene_contrato") else "Sin contrato vigente.")
    re_ej = _g(d, "resumen_ejecutivo")
    if re_ej:
        out.append(re_ej)
    if not _g(d, "tiene_contrato") and _g(d, "valorizacion.total"):
        out.append(f"Prospecto activo (potencial {_clp(_g(d,'valorizacion.total'))}).")
    if _g(d, "brecha_gratis.exposicion_clp"):
        out.append(f"Dinero en juego: {_clp(_g(d,'brecha_gratis.exposicion_clp'))}.")
    if _g(d, "facturacion.pendientes"):
        out.append(f"{_g(d,'facturacion.pendientes')} servicios por facturar.")
    cal = _g(d, "calidad_informacion")
    if cal is not None:
        out.append(f"Calidad de la ficha: {cal}%." + (" ⚠ Información incompleta, algunas cifras pueden estar subestimadas." if (isinstance(cal,(int,float)) and cal < 60) else ""))
    return "\n".join(out)


def _r_estado_contrato(d):
    if not _g(d, "tiene_contrato"):
        return ("Este cliente NO tiene contrato vigente: prospecto de venta directa. Cada servicio se cobra suelto, "
                "sin recurrencia ni protección de exclusividad frente a terceros. Recomendable formalizar uno.")
    est = _g(d, "diagnostico_contrato.estado") or "—"
    frec = _g(d, "diagnostico_contrato.frecuencia_meses")
    out = [f"📄 Contrato {est}" + (f", frecuencia cada {frec} meses." if frec else ".")]
    mm = _clp(_g(d, "diagnostico_contrato.monto_mensual"))
    va = _clp(_g(d, "diagnostico_contrato.valor_anual"))
    if mm or va:
        out.append(f"MRR {mm or '—'}/mes ({va or '—'}/año).")
    if _g(d, "diagnostico_contrato.es_indefinido"):
        out.append(f"Vigencia indefinida (desde {_g(d,'diagnostico_contrato.vigencia_inicio') or '—'}).")
    else:
        out.append(f"Vigencia: {_g(d,'diagnostico_contrato.vigencia_inicio') or '—'} a {_g(d,'diagnostico_contrato.vigencia_fin') or '—'}.")
    if _g(d, "diagnostico_contrato.incluye_gratis"):
        out.append(f"Incluye {_g(d,'diagnostico_contrato.gratis_incluidas') or '—'} mantenciones gratis/año (vigilar que no se vuelvan fuga ni garantía mal asignada).")
    else:
        out.append("No incluye gratis: todo servicio se factura.")
    if est in ("vencido", "por_vencer"):
        out.insert(0, "🔴 RENOVAR YA / VENCIDO.")
    return "\n".join(out)


def _r_frecuencia(d):
    frec = _g(d, "diagnostico_contrato.frecuencia_meses")
    if _g(d, "tiene_contrato") and frec:
        try:
            al_ano = round(12 / float(frec))
        except Exception:
            al_ano = "—"
        real = _g(d, "mantenciones.realizadas", 0)
        esp = _g(d, "mantenciones.esperadas_a_hoy", 0)
        cumpl = "Vas al día." if (real or 0) >= (esp or 0) else f"Vas atrasado en {max(0,(esp or 0)-(real or 0))} visita(s)."
        return f"🔁 Frecuencia contratada: una mantención cada {frec} meses (~{al_ano} al año). A hoy: {real}/{esp}. {cumpl}"
    return ("No hay frecuencia contractual definida; las visitas se hacen a demanda. "
            "Oportunidad para proponer un contrato con frecuencia fija.")


def _r_historial(d):
    tot = _g(d, "historia.total", 0)
    ult = _g(d, "historia.ultima") or _g(d, "universo.ultima_fecha")
    gasto = _clp(_g(d, "historia.gasto_total"))
    out = [f"📋 Histórico: {tot} gestiones registradas"
           + (f", última el {_fmt_fecha(ult)}" if ult else "")
           + (f", gasto total {gasto}" if gasto else "") + "."]
    out.append(f"Contrato: {_cob(d,'Contrato')} | Cliente paga: {_cob(d,'Cliente paga')} | Garantía: {_cob(d,'Garantía')} | Tercero (fuga): {_cob(d,'Tercero (fuga)')}.")
    if _cob(d, "Tercero (fuga)"):
        out.append(f"⚠ {_cob(d,'Tercero (fuga)')} visita(s) las cubrió un TERCERO → ingreso recuperable si lo traemos de vuelta.")
    return "\n".join(out)


def _r_oportunidad(d):
    out = []
    if not _g(d, "tiene_contrato"):
        out.append(f"#1: NO tiene contrato. Propón contrato anual"
                   + (f" por {_clp(_g(d,'valorizacion.total'))}" if _g(d, "valorizacion.total") else "")
                   + f" sobre sus {_g(d,'equipos.total',0)} equipos (captura recurrencia).")
    elif _g(d, "diagnostico_contrato.estado") in ("vencido", "por_vencer"):
        out.append("#1: Renovación de contrato vencido/por vencer.")
    if _g(d, "equipos.criticos"):
        out.append(f"Venta de repuestos/recambio: {_g(d,'equipos.criticos')} equipo(s) en estado crítico.")
    if _g(d, "brecha_gratis.riesgo_fuga_tercero") or _cob(d, "Tercero (fuga)"):
        out.append(f"Recuperar servicios que hoy van a terceros ({_clp(_g(d,'brecha_gratis.exposicion_clp')) or 'exposición abierta'}).")
    for a in (_g(d, "acciones", []) or [])[:4]:
        if isinstance(a, dict) and a.get("titulo"):
            out.append(a["titulo"])
    pitch = _g(d, "valorizacion.pitch")
    if pitch:
        out.append("💬 " + pitch)
    if not out:
        return "Cuenta estable; oportunidad de upsell en cobertura ampliada o contrato multi-año."
    return "🎯 Oportunidades:\n" + "\n".join(f"  • {o}" for o in out)


def _r_que_hacer(d):
    nr = (_g(d, "nivel_riesgo") or "medio").lower()
    out = []
    acc = [a.get("titulo") for a in (_g(d, "acciones", []) or []) if isinstance(a, dict) and a.get("titulo")]
    if acc:
        out = acc
    else:
        if _len(_g(d, "mantenciones.vencidas", [])):
            out.append(f"Programar {_len(_g(d,'mantenciones.vencidas',[]))} mantención(es) vencida(s).")
        if _g(d, "brecha_gratis.pendientes"):
            out.append(f"Ejecutar {_g(d,'brecha_gratis.pendientes')} mantención(es) gratis pendientes antes de que las tome un tercero.")
        if _g(d, "facturacion.pendientes"):
            out.append(f"Facturar {_g(d,'facturacion.pendientes')} servicios pendientes.")
        if _g(d, "equipos.criticos"):
            out.append(f"Revisar {_g(d,'equipos.criticos')} equipo(s) crítico(s).")
        falt = _g(d, "datos_cliente.faltantes", []) or []
        if falt:
            out.append("Completar datos faltantes: " + ", ".join(str(x) for x in falt[:4]) + ".")
    if not out:
        return f"✓ Cliente al día (riesgo {nr}), sin acciones urgentes."
    return f"Riesgo {nr}. Próximos pasos:\n" + "\n".join(f"  {i+1}. {o}" for i, o in enumerate(out))


def _r_equipos(d):
    tot = _g(d, "equipos.total", 0)
    if not tot:
        return "No hay equipos levantados; agenda un levantamiento técnico antes de cotizar mantención."
    out = [f"🏋 El cliente tiene {tot} equipos registrados."]
    if _g(d, "equipos.criticos"):
        out.append(f"🔴 {_g(d,'equipos.criticos')} equipo(s) marcado(s) como CRÍTICO(s); priorizar en la próxima visita.")
    ss, sf = _g(d, "equipos.sin_serie", 0), _g(d, "equipos.sin_foto", 0)
    if ss or sf:
        out.append(f"Datos incompletos: {ss} sin N° de serie, {sf} sin foto (de {tot}). Completar en terreno para trazabilidad de garantías.")
    else:
        out.append("Inventario completo (todos con serie y foto).")
    return "\n".join(out)


def _r_datos_faltantes(d):
    cal = _g(d, "calidad_informacion")
    out = [f"📋 Calidad de la ficha: {cal}%." if cal is not None else "Datos de la ficha:"]
    falt = _g(d, "datos_cliente.faltantes", []) or []
    for f in falt:
        out.append(f"  • Falta: {f}")
    if _g(d, "equipos.sin_serie"):
        out.append(f"  • {_g(d,'equipos.sin_serie')} equipos sin N° de serie.")
    if _g(d, "equipos.sin_foto"):
        out.append(f"  • {_g(d,'equipos.sin_foto')} equipos sin foto.")
    if len(out) == 1:
        out.append("✓ Ficha completa, no falta información clave.")
    else:
        out.append("Completar estos datos sube el score y la confiabilidad de las cifras.")
    return "\n".join(out)


def _r_garantia_mal_asignada(d):
    g = _cob(d, "Garantía")
    tot = _g(d, "historia.total", 0) or 0
    if g == 0:
        return "No hay servicios cubiertos por garantía en el historial; sin riesgo de garantía mal asignada."
    senal = (g > _cob(d, "Contrato")) or (tot and g > tot * 0.4)
    if senal:
        exp = _clp(_g(d, "brecha_gratis.exposicion_clp"))
        return (f"⚠ POSIBLE GARANTÍA MAL ASIGNADA: de {tot} gestiones, {g} se cargaron como Garantía (gratis) vs {_cob(d,'Cliente paga')} cobradas. "
                "Si parte eran fallas por mal uso o fuera de cobertura del fabricante, se REGALÓ mano de obra cobrable."
                + (f" Exposición: {exp}." if exp else "")
                + " Recomendación: auditar las OT marcadas Garantía y exigir respaldo del fabricante antes de marcar gratis.")
    return f"✓ Clasificación sana: {_cob(d,'Contrato')} por contrato, {g} por garantía, {_cob(d,'Cliente paga')} cobradas. Sin sobre-uso evidente."


def _r_exposicion_total(d):
    bg = _g(d, "brecha_gratis.exposicion_clp", 0) or 0
    items = _g(d, "facturacion.items", []) or []
    fact_monto = 0
    for it in items:
        if isinstance(it, dict):
            try:
                fact_monto += float(it.get("monto") or it.get("total") or 0)
            except (TypeError, ValueError):
                pass
    total = bg + fact_monto
    if total == 0 and not _cob(d, "Tercero (fuga)"):
        return "✓ No hay exposición financiera abierta: ni gratuitas en deuda ni servicios sin facturar."
    out = [f"💸 Exposición financiera estimada: {_clp(total)}."]
    if bg:
        out.append(f"  • Gratuitas no entregadas (posible fuga): {_clp(bg)}.")
    if fact_monto:
        out.append(f"  • Servicios ejecutados sin facturar: {_clp(fact_monto)} ({_g(d,'facturacion.pendientes',0)} OT).")
    if _cob(d, "Tercero (fuga)"):
        out.append(f"  • {_cob(d,'Tercero (fuga)')} servicio(s) histórico(s) se fueron a un tercero → ingreso no capturado.")
    out.append(f"Nivel de riesgo: {(_g(d,'nivel_riesgo') or '—')} (salud {_g(d,'score_salud','—')}/100).")
    return "\n".join(out)


def _r_legal(d):
    out = []
    if _g(d, "brecha_gratis.pendientes"):
        out.append(f"Garantía incumplida: {_g(d,'brecha_gratis.pendientes')} gratis pendientes. Bajo Ley 19.496 el cliente pagó un servicio dentro del plan; el incumplimiento habilita reclamo SERNAC"
                   + (f" (exposición {_clp(_g(d,'brecha_gratis.exposicion_clp'))})" if _g(d, "brecha_gratis.exposicion_clp") else "") + ".")
    if _g(d, "facturacion.pendientes"):
        out.append(f"Facturación: {_g(d,'facturacion.pendientes')} servicios sin facturar (el más antiguo {_g(d,'facturacion.mas_antigua_dias','?')} días). Regularizar antes de que se cuestione el cobro.")
    if not _g(d, "tiene_contrato") or _g(d, "diagnostico_contrato.estado") == "vencido":
        out.append("Contrato: operando sin respaldo contractual vigente → expuestos a que el cliente desconozca obligaciones.")
    if not out:
        return "✓ Sin exposición legal relevante hoy: gratis al día, servicios facturados y contrato vigente. Mantener la trazabilidad de cada OT como respaldo."
    return f"⚖ Riesgo legal ({(_g(d,'nivel_riesgo') or '—')}):\n" + "\n".join(f"  • {o}" for o in out)


def _r_contacto(d):
    def v(k):
        return _g(d, "cliente." + k) or "no registrado"
    out = [f"🏢 {_nombre(d)}",
           f"RUT: {v('rut')}",
           f"Dirección: {_g(d,'cliente.direccion') or 'no registrada'}, {_g(d,'cliente.comuna') or ''}".rstrip(', '),
           f"Contacto: {_g(d,'cliente.contacto_nombre') or _g(d,'cliente.contacto') or 'no registrado'}"]
    return "\n".join(out)


# ── Tabla de intents (orden = prioridad) ─────────────────────────────
INTENTS = [
    ("gratis_o_se_cobra", _r_gratis_o_se_cobra, 1, ["gratis", "se cobra", "cobrar", "cobro", "cubierto", "cubrimos", "incluido", "incluye", "gratuita", "cortesia", "facturar o garantia"]),
    ("cuanto_cobrar", _r_cuanto_cobrar, 1, ["cuanto cobr", "cuanto le cobro", "cuanto deber", "deberia cobr", "cobrarle", "cuanto cotiz", "precio", "monto", "cotizo", "cotizar", "ofrecer", "cuanto vale", "tarifa", "valorizacion", "propuesta", "descuento", "cuanto queda", "plata", "lucas", "presupuesto"]),
    ("fuga_a_terceros", _r_fuga_a_terceros, 1, ["fuga", "tercero", "competencia", "otra empresa", "otro proveedor", "perdiendo", "externo", "competidor", "se nos va"]),
    ("mantenciones_gratis_pendientes", _r_mantenciones_gratis_pendientes, 1, ["gratis pendientes", "incluidas", "cuantas gratis", "le debemos", "pactadas", "cumplimos", "quedan gratis", "regalando", "regalo", "brecha"]),
    ("servicios_sin_facturar", _r_servicios_sin_facturar, 1, ["sin facturar", "facturacion", "por cobrar", "no facturado", "regularizar", "factura pendiente", "pendiente de cobro"]),
    ("mantenciones_vencidas", _r_mantenciones_vencidas, 1, ["vencid", "atrasad", "atraso", "retras", "no realizad", "incumpl", "fuera de plazo", "sla", "le debo una visita", "mantenciones pendientes"]),
    ("proxima_mantencion", _r_proxima_mantencion, 1, ["proxima", "proximo", "cuando toca", "cuando es la siguiente", "agendad", "siguiente visita", "cuando volver", "cuando vuelvo"]),
    ("frecuencia", _r_frecuencia, 2, ["cada cuanto", "frecuencia", "periodicidad", "cada cuantos meses", "cuantas mantenciones al ano", "ritmo", "cumplimiento"]),
    ("estado_contrato", _r_estado_contrato, 2, ["contrato", "vigente", "vence", "vencimiento", "renovar", "vigencia", "indefinido", "que dice el contrato", "clausula", "mrr"]),
    ("salud_riesgo", _r_salud_riesgo, 2, ["salud", "riesgo", "riesgoso", "peligro", "score", "nivel de riesgo", "que tan riesgosa", "critico"]),
    ("resumen_general", _r_resumen_general, 2, ["resumen", "panorama", "como esta este cliente", "como va", "overview", "situacion", "como esta la cuenta"]),
    ("historial", _r_historial, 3, ["historial", "historia", "cuantas visitas", "ultima visita", "ultima vez", "que se hizo", "gestiones", "gastado", "gasto", "historico"]),
    ("oportunidad", _r_oportunidad, 3, ["oportunidad", "vender", "venta", "upsell", "negocio", "crecer", "que le ofrezco", "que le puedo vender", "potencial"]),
    ("que_hacer", _r_que_hacer, 3, ["que hago", "que hacer", "que falta", "proximos pasos", "que sigue", "recomendacion", "prioridad", "acciones", "plan"]),
    ("equipos", _r_equipos, 3, ["equipos", "maquinas", "inventario", "sin serie", "sin foto", "cuantas maquinas", "parque", "equipo critico"]),
    ("garantia_mal_asignada", _r_garantia_mal_asignada, 4, ["mal asignada", "mal clasificada", "regalamos garantia", "garantia mal", "se debio cobrar", "debi cobrar", "exceso garantia", "regalado en garantia"]),
    ("exposicion_total", _r_exposicion_total, 4, ["exposicion", "en riesgo", "perdida", "en juego", "plata en riesgo", "riesgo economico", "cuanto pierdo", "dejamos de facturar"]),
    ("legal", _r_legal, 5, ["legal", "ley", "19496", "consumidor", "demanda", "demandar", "sernac", "reclamo", "responsabilidad"]),
    ("datos_faltantes", _r_datos_faltantes, 4, ["que datos faltan", "incompleta", "completar", "calidad de la ficha", "esta completa", "que le falta a la ficha"]),
    ("contacto", _r_contacto, 5, ["contacto", "direccion", "comuna", "rut del cliente", "telefono", "donde queda", "ubicacion", "correo"]),
    ("garantias", _r_gratis_o_se_cobra, 1, ["garantia", "garantias", "como van las garantias"]),
]

# Chips de preguntas sugeridas (las más útiles, en orden).
CHIPS = [
    "¿Cada cuánto se le hace mantención?",
    "¿Esta mantención es gratis o se cobra?",
    "¿Cómo van las garantías?",
    "¿Cuánto le cobro?",
    "¿Hay mantenciones atrasadas?",
    "¿Se está fugando a terceros?",
    "¿Qué tengo sin facturar?",
    "¿Cuándo es la próxima mantención?",
    "¿Cómo está este cliente?",
    "¿Qué hago ahora?",
]


def responder(pregunta, d):
    """Devuelve {intent, respuesta, sugerencias}. Determinista, sin IA."""
    q = _norm(pregunta)
    if not q or len(q) < 2:
        return {"intent": None,
                "respuesta": "Hazme una pregunta sobre este cliente — frecuencia, garantías, si se cobra o es gratis, atrasos, riesgo… o toca una sugerencia.",
                "sugerencias": CHIPS}
    qtoks = set(q.split())
    best, best_score, best_pri = None, 0.0, 99
    for key, fn, pri, kws in INTENTS:
        score = 0.0
        for k in kws:
            if k in q:
                score += 2.0 * len(k.split())   # coincidencia EXACTA de frase: peso doble
                continue
            ktoks = k.split()
            hit = 0
            for kt in ktoks:
                if kt in qtoks:
                    hit += 1
                elif len(kt) >= 4 and any(_similar(kt, qt) for qt in qtoks):
                    hit += 1                     # tolerancia a typos (garntia≈garantia)
            if hit:
                score += hit / len(ktoks)        # solapamiento parcial de tokens
        if score > best_score or (abs(score - best_score) < 1e-9 and score > 0 and pri < best_pri):
            best, best_score, best_pri = (key, fn), score, pri
    # Fallback INTELIGENTE: si no hay match razonable, dar el resumen del cliente
    # (NUNCA un "no entendí" seco — el agente siempre responde algo útil).
    if not best or best_score < 0.5:
        try:
            resumen = _r_resumen_general(d)
        except Exception:
            resumen = ""
        pre = f"No identifiqué la pregunta exacta, pero esto es lo más relevante de {_nombre(d)}:\n\n"
        return {"intent": "resumen_general_fallback",
                "respuesta": (pre + resumen) if resumen else
                             "Pregúntame sobre frecuencia, garantías, cobro, atrasos o riesgo de este cliente.",
                "sugerencias": CHIPS}
    key, fn = best
    try:
        resp = fn(d)
    except Exception as e:
        print(f"[agente_chat] intent {key} fallo: {e!r}", flush=True)
        resp = "No pude calcular la respuesta con los datos actuales de la ficha."
        key = key + "_error"
    return {"intent": key, "respuesta": resp, "sugerencias": CHIPS}
