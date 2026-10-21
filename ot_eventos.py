"""OT de DESINSTALACIÓN (madre e hija) — lógica PURA (Daniel, 19-oct-2026).

Daniel: «al momento de crear la orden de trabajo, si se selecciona instalación con desinstalación, el usuario tendrá que
agendar en otra hija cuando se va a hacer la desinstalación para que quede de una vez agendado… literalmente
cuadradito… que lo calendarice y deje agendado todo para que el técnico sepa».

2026-10-21: Daniel pidió eliminar la función «Eventos» («el de eventos no me sirve, elimínalo y el código»): las carpetas de
OT por evento, sus pantallas, rutas y reglas se quitaron. Este módulo conserva SOLO la desinstalación. El nombre del archivo
(`ot_eventos.py`) se deja igual porque lo importan app.py y las pruebas.

Este módulo NO conoce la base, Flask ni el ERP: recibe diccionarios (filas de mant_visitas ya leídas) y
devuelve veredictos. Las lecturas y escrituras viven en app.py (`_ot_crear_desinstalacion`, `_desinst_*`),
que lo llaman. Lo que decide el módulo:

  · Cuándo una OT hija es «cubierta» por la instalación (no consume saldo de la nota de venta, no pide autorización de
    $0): se DEDUCE de ot_relacion / ot_madre_id, nunca de valor_origen (crítica adversarial 19-oct, punto 1).
  · Las fechas de la desinstalación (REGLA #31: el domingo bloquea; sábado y feriado solo avisan; la hija no puede ir
    antes que la instalación).
  · Los guardias de cancelar / reagendar madre e hija.
  · Los avisos del barrido diario («que no se olvide»).
"""
import datetime as _dt

TIPO_MADRE = "instalacion"
TIPO_HIJA = "desinstalacion"
REL_DESINSTALACION = "desinstalacion"
ESTADOS_FUERA = ("cancelada", "anulada")           # una OT en estos estados ya no cuenta (ni como hija vigente)
ESTADOS_SIN_EMPEZAR = ("programada", "creada", "asignada", "reagendada", "pendiente_info")
ESTADOS_POR_CERRAR = ("completada", "pendiente_aprobacion", "firmada_tecnico", "pendiente_repuesto")
ESTADO_CERRADA = "cerrada"
# 🧭 2026-10-20: la OT PENDIENTE por falta de stock (ot_que_paso.py) cuelga de su madre con otra relación y nace con la fecha
# centinela 2099-12-31 («todavía sin fecha»). No es una desinstalación y esa fecha no es «la próxima fecha».
REL_PENDIENTE_STOCK = "pendiente_stock"
# 🩺 2026-10-21 (REGLA #38): la OT de SEGUIMIENTO de un hallazgo cuelga de la OT de origen con esta relación (ot_hallazgos.REL_SEGUIMIENTO).
# Tampoco es una desinstalación: no es «la hija» de nadie para el guardia de tipo.
REL_SEGUIMIENTO = "seguimiento"
ANIO_SIN_FECHA = 2099
ARGUMENTO_MIN = 30          # igual que _OT_AUT_ARGUMENTO_MIN: lo que lee Daniel para autorizar
MOTIVO_MIN = 10
DIAS_HABILES_AVISO = 2      # el barrido avisa N días hábiles antes de la desinstalación prevista si no hay hija
EDAD_MAX_PREVISTA_DIAS = 92  # solo informativo (la regla de los 3 meses de la spec se reemplazó por la autorización)

COBRO_CUBIERTA = "cubierta"
COBRO_PROPIO = "cobra"
COBROS_VALIDOS = (COBRO_CUBIERTA, COBRO_PROPIO)

_MESES = ("enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre",
          "noviembre", "diciembre")
_DIAS = ("lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo")


# ───────────────────────────── utilidades ─────────────────────────────
def parse_fecha(x):
    """date desde date/datetime/'aaaa-mm-dd'/'dd/mm/aaaa'. Lo que no se entiende → None (nunca inventa)."""
    if x is None or x == "":
        return None
    if isinstance(x, _dt.datetime):
        return x.date()
    if isinstance(x, _dt.date):
        return x
    t = str(x).strip()[:10]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return _dt.datetime.strptime(t, fmt).date()
        except ValueError:
            continue
    return None


def fmt_fecha(x):
    """dd/mm/aaaa (REGLA #6: nunca ISO crudo). Vacío si no se entiende."""
    f = parse_fecha(x)
    return f.strftime("%d/%m/%Y") if f else ""


def fmt_fecha_corta(x):
    f = parse_fecha(x)
    return f.strftime("%d/%m") if f else ""


def dia_semana(x):
    f = parse_fecha(x)
    return _DIAS[f.weekday()] if f else ""


def hora_min(h):
    """'HH:MM' (o time/timedelta de MySQL) → minutos desde medianoche; None si no se entiende."""
    if h is None or h == "":
        return None
    if isinstance(h, _dt.timedelta):
        return int(h.total_seconds() // 60)
    if isinstance(h, _dt.time):
        return h.hour * 60 + h.minute
    t = str(h).strip()[:5]
    try:
        hh, mm = t.split(":")
        hh, mm = int(hh), int(mm)
    except (ValueError, TypeError):
        return None
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return hh * 60 + mm


def min_txt(m):
    """Minutos desde medianoche → 'HH:MM' (2026-10-20: el mensaje «…después de que termine la instalación ()» salía con la
    hora vacía porque se le pasaban minutos enteros donde se esperaba 'HH:MM')."""
    try:
        m = int(m)
    except (TypeError, ValueError):
        return ""
    return "%02d:%02d" % (m // 60, m % 60)


def _fer_set(feriados):
    """Acepta dict {iso: nombre}, set/list de iso o de date → {iso: nombre|''}."""
    out = {}
    if isinstance(feriados, dict):
        for k, v in feriados.items():
            f = parse_fecha(k)
            if f:
                out[f.isoformat()] = v or ""
    else:
        for k in (feriados or []):
            f = parse_fecha(k)
            if f:
                out[f.isoformat()] = ""
    return out


def es_habil(f, feriados=None):
    """Lunes a viernes que no es feriado (REGLA #31)."""
    f = parse_fecha(f)
    if not f:
        return False
    return f.weekday() < 5 and f.isoformat() not in _fer_set(feriados)


def siguiente_habil(f, feriados=None, incluir_hoy=True):
    """El primer día hábil desde `f` (inclusive por defecto). Sin tope infinito: máx. 30 días."""
    f = parse_fecha(f)
    if not f:
        return None
    fer = _fer_set(feriados)
    d = f if incluir_hoy else f + _dt.timedelta(days=1)
    for _ in range(30):
        if d.weekday() < 5 and d.isoformat() not in fer:
            return d
        d += _dt.timedelta(days=1)
    return d


def restar_habiles(f, n, feriados=None):
    """La fecha `n` días hábiles antes de `f` (n ≥ 0)."""
    f = parse_fecha(f)
    if not f:
        return None
    fer = _fer_set(feriados)
    d, falta = f, max(0, int(n or 0))
    for _ in range(400):
        if falta <= 0:
            break
        d -= _dt.timedelta(days=1)
        if d.weekday() < 5 and d.isoformat() not in fer:
            falta -= 1
    return d


def vigente(estado):
    """¿Una OT con este estado cuenta como vigente? (hija vigente = NO cancelada ni anulada; incluye las cerradas)."""
    return str(estado or "").strip().lower() not in ESTADOS_FUERA


def sin_empezar(estado, fila=None):
    """¿La OT todavía no se empezó? Por su estado y, si se pasa la `fila`, también porque nadie la trabajó: 'pendiente_info'
    se usa DESPUÉS de la firma del técnico (fase «comp»), así que una OT ya ejecutada y firmada con ese estado NO está sin
    empezar (revisión 2026-10-20: la cascada de cancelar cancelaba una desinstalación hecha y firmada; la OT es evidencia)."""
    if str(estado or "").strip().lower() not in ESTADOS_SIN_EMPEZAR:
        return False
    if fila:
        for k in ("firma_tecnico_at", "hora_real_inicio", "fecha_realizada"):
            if fila.get(k):
                return False
    return True


# ───────────────────────── fechas de la desinstalación ─────────────────────────
def fecha_referencia_madre(madre):
    """(fecha, hora_fin_min) a partir de la cual puede ir la desinstalación. Si la instalación ya se ejecutó, manda su
    fecha real; si dura varios días, su fecha_fin y hora_fin (crítica 16)."""
    madre = madre or {}
    real = parse_fecha(madre.get("fecha_realizada"))
    prog = parse_fecha(madre.get("fecha_programada"))
    fin = parse_fecha(madre.get("fecha_fin"))
    if real:
        # 2026-10-19 (crítica 16): si la instalación YA se ejecutó, manda la fecha en que se hizo de verdad (aunque haya
        # sido antes de lo programado); la hora de término planificada ya no aplica.
        return real, None
    candidatas = [x for x in (fin, prog) if x]
    if not candidatas:
        return None, None
    ref = max(candidatas)
    hora = None
    # La hora de término (planificada) solo tiene sentido si el día de referencia es el de término programado.
    if ref == (fin or prog):
        hora = hora_min(madre.get("hora_fin"))
    return ref, hora


def evaluar_fecha_desinstalacion(fecha, hora_ini, hora_fin, madre, feriados=None):
    """Veredicto de la fecha/hora de una desinstalación frente a su instalación (madre).

    Devuelve {ok, bloqueo: {codigo, mensaje}|None, avisos: [texto]}. `ok` solo es False cuando hay `bloqueo`:
      · FECHA_REQUERIDA / FECHA_INVALIDA / HORA_INVALIDA
      · DOMINGO_BLOQUEADO — «Los domingos no se agendan órdenes de trabajo. Elige otro día.» (mismo texto de hoy)
      · DESINST_ANTES_DE_INSTALACION — la hija va el mismo día o después; el mismo día, su inicio ≥ término de la madre.
    Avisos (ámbar, no bloquean): sábado, feriado, horario sin definir el mismo día."""
    avisos = []
    if fecha is None or str(fecha).strip() == "":
        return {"ok": False, "bloqueo": {"codigo": "FECHA_REQUERIDA", "mensaje": "Falta la fecha de la desinstalación."},
                "avisos": avisos}
    f = parse_fecha(fecha)
    if not f:
        return {"ok": False, "bloqueo": {"codigo": "FECHA_INVALIDA",
                                         "mensaje": "La fecha de la desinstalación no es válida."}, "avisos": avisos}
    hi, hf = hora_min(hora_ini), hora_min(hora_fin)
    if (hora_ini not in (None, "") and hi is None) or (hora_fin not in (None, "") and hf is None):
        return {"ok": False, "bloqueo": {"codigo": "HORA_INVALIDA", "mensaje": "La hora de la desinstalación no es válida."},
                "avisos": avisos}
    if hi is not None and hf is not None and hf <= hi:
        return {"ok": False, "bloqueo": {"codigo": "HORA_INVALIDA",
                                         "mensaje": "La hora de término tiene que ser después de la de inicio."},
                "avisos": avisos}
    if f.weekday() == 6:
        return {"ok": False, "bloqueo": {"codigo": "DOMINGO_BLOQUEADO",
                                         "mensaje": "Los domingos no se agendan órdenes de trabajo. Elige otro día."},
                "avisos": avisos}
    ref, ref_hora = fecha_referencia_madre(madre)
    if ref is not None:
        if f < ref:
            return {"ok": False, "bloqueo": {
                "codigo": "DESINST_ANTES_DE_INSTALACION",
                "mensaje": "La desinstalación no puede ir antes de la instalación (%s). Elige el mismo día "
                           "después de que termine, o una fecha posterior." % fmt_fecha(ref)}, "avisos": avisos}
        if f == ref and ref_hora is not None:
            if hi is not None and hi < ref_hora:
                return {"ok": False, "bloqueo": {
                    "codigo": "DESINST_ANTES_DE_INSTALACION",
                    "mensaje": "El mismo día, la desinstalación tiene que empezar después de que termine la "
                               "instalación (%s)." % min_txt(ref_hora)}, "avisos": avisos}
            if hi is None:
                avisos.append("Define la hora de inicio: el mismo día tiene que ser después de que termine la "
                              "instalación (%s)." % min_txt(ref_hora))
    if f.weekday() == 5:
        avisos.append("Cae en sábado, cuando no hay operación normal.")
    nombre_fer = _fer_set(feriados).get(f.isoformat())
    if nombre_fer is not None:
        avisos.append("Es feriado" + (": %s." % nombre_fer if nombre_fer else "."))
    return {"ok": True, "bloqueo": None, "avisos": avisos}


def proponer_fecha_desinstalacion(madre=None, feriados=None, hoy=None):
    """Fecha propuesta para la desinstalación. 2026-10-20 (revisión): la propuesta NUNCA puede quedar bloqueada por el
    mismo sistema. Es el SIGUIENTE día hábil después de la instalación (su fecha_fin / hora_fin, o la fecha real si ya se
    hizo): así no propone el mismo día de la instalación (que bloquea si la instalación termina a las 13:00 y la
    desinstalación parte a las 09:00). 2026-10-21: se quitó el «fin del evento» como candidata (se eliminó Eventos).
    Si cae en domingo o feriado se propone el siguiente día hábil (crítica 16). Devuelve {fecha: date|None, motivo}."""
    ref, _h = fecha_referencia_madre(madre)
    base = siguiente_habil(ref, feriados, incluir_hoy=False) if ref is not None else None
    origen = "día hábil siguiente a la instalación"
    if base is None:
        return {"fecha": None, "motivo": None}
    hoy = parse_fecha(hoy)
    if hoy and base < hoy:
        base, origen = hoy, "hoy"
    if es_habil(base, feriados):
        return {"fecha": base, "motivo": "Propuesta: %s." % origen}
    sig = siguiente_habil(base, feriados, incluir_hoy=False)
    return {"fecha": sig, "motivo": "El %s cae en %s; se propone el siguiente día hábil." % (
        origen, "domingo" if base.weekday() == 6 else ("sábado" if base.weekday() == 5 else "feriado"))}


# ───────────────────────── ¿es «cubierta»? ─────────────────────────
def madre_cobra(madre):
    """¿La instalación SE COBRA al cliente? (revisión 2026-10-20, REGLA #24: «un $0 aprobado vale solo para LA OT para la que
    se pidió»). Una instalación en garantía, regalía, arriendo/leasing o de modalidad sin costo / interna NO cobra: su
    desinstalación no puede quedar «cubierta» por ella (sería un segundo $0 que Daniel nunca autorizó, sin candado de
    pérdida) y pasa por la puerta normal con su propio documento o su propio $0. Misma regla que `_ot_cobertura` de app.py;
    una clave que no viene se trata como la del caso normal ('pagado', sin motivo de $0)."""
    m = madre or {}
    mod = str(m.get("modalidad_cobro") or "pagado").strip().lower()
    cub = str(m.get("cubierto_por") or "").strip().lower()
    cero = str(m.get("cobro_cero_motivo") or "").strip()
    return mod == "pagado" and cub != "garantia" and not cero


def madre_valida_para_cubrir(madre, cliente_hija, tiene_doc_real, aut_aprobada=False):
    """¿Esta fila puede ser la MADRE de una desinstalación cubierta? Cuenta lo que dice la crítica adversarial:
      · instalación, sin madre propia (no hay cadenas), no cancelada ni anulada, del MISMO cliente;
      · con `desinstalacion_prevista` declarada;
      · con un documento de Random de verdad (nota de venta / factura / boleta, origen erp, es_cobro=1) —
        `tiene_doc_real` — o, como única excepción, una autorización aprobada que ya incluía la desinstalación
        (`aut_aprobada`: la solicitud de la madre decía «Incluye desinstalación dd/mm» y Daniel la vio).
    Devuelve (ok, motivo_si_no)."""
    m = madre or {}
    if str(m.get("tipo") or "").strip().lower() != TIPO_MADRE:
        return False, "La OT madre tiene que ser una instalación."
    if m.get("ot_madre_id"):
        return False, "La OT madre ya es una desinstalación: no hay cadenas."
    if not vigente(m.get("estado")):
        return False, "La OT madre está %s." % (m.get("estado") or "cancelada")
    if cliente_hija is not None and m.get("cliente_id") is not None and int(m["cliente_id"]) != int(cliente_hija):
        return False, "La OT madre es de otro cliente."
    if not m.get("desinstalacion_prevista"):
        return False, "La instalación no tiene una desinstalación prevista."
    if not madre_cobra(m):
        return False, ("La instalación no se cobra (garantía, regalía, arriendo o sin costo): su desinstalación no puede "
                       "quedar cubierta por ella y necesita su propio documento o su propia autorización de $0.")
    if not (tiene_doc_real or aut_aprobada):
        return False, ("La instalación no tiene un documento de Random (nota de venta, factura o boleta) que respalde "
                       "la desinstalación.")
    return True, None


def es_hija_cubierta(hija, madre_valida, cobro_hija=0, cobro_envio_hija=0):
    """La cobertura 'cubierta' de una hija ya existente. Todas las condiciones juntas:
      · tipo 'desinstalacion' y ot_relacion 'desinstalacion', con ot_madre_id;
      · la madre pasó `madre_valida_para_cubrir`;
      · el cobro ZZ de la hija es 0 o nulo (si cobra su propia línea de desinstalación, va por el camino normal).
    NUNCA se deduce de valor_origen."""
    h = hija or {}
    if str(h.get("tipo") or "").strip().lower() != TIPO_HIJA:
        return False
    if str(h.get("ot_relacion") or "").strip().lower() != REL_DESINSTALACION or not h.get("ot_madre_id"):
        return False
    if not madre_valida:
        return False
    for x in (cobro_hija, cobro_envio_hija):
        try:
            if float(x or 0) > 0:
                return False
        except (TypeError, ValueError):
            return False
    return True


def hijas_vigentes(hijas, excluir_id=None):
    """Las hijas de una madre que cuentan (NO canceladas ni anuladas; las cerradas SÍ cuentan: crítica 3)."""
    out = []
    for h in (hijas or []):
        if excluir_id is not None and h.get("id") is not None and int(h["id"]) == int(excluir_id):
            continue
        if vigente(h.get("estado")):
            out.append(h)
    return out


def puede_programar_desinstalacion(madre, hijas):
    """¿Se puede crear (o mostrar el botón «Programar desinstalación») la hija de esta OT? Devuelve (ok, motivo).
    Solo una hija vigente por instalación; la madre no puede estar cancelada ni anulada ni ser ella misma una hija."""
    m = madre or {}
    if str(m.get("tipo") or "").strip().lower() != TIPO_MADRE:
        return False, "Solo una instalación se desinstala después."
    if m.get("ot_madre_id"):
        return False, "Esta OT ya es una desinstalación."
    if not vigente(m.get("estado")):
        return False, "La instalación está %s." % (m.get("estado") or "cancelada")
    hv = hijas_vigentes(hijas)
    if hv:
        return False, "Esta instalación ya tiene su desinstalación (%s)." % (hv[0].get("numero_ot") or "OT #%s" % hv[0].get("id"))
    return True, None


def necesita_autorizacion_prevista(declarada_en_asistente):
    """Declarar `desinstalacion_prevista` DESPUÉS de crear la madre pide la autorización 'desinstalacion_cubierta'
    (argumento ≥ 30), salvo que se haya respondido «Sí» en el asistente. Crítica 4: así una instalación vieja no sirve
    de paraguas. 2026-10-21: se eliminó Eventos, así que ya no existe la excepción «la madre nació en un evento con fecha
    de fin»: declararla después SIEMPRE pide autorización (Daniel: «el de eventos no me sirve, elimínalo y el código»)."""
    return not declarada_en_asistente


def validar_cobro_desinstalacion(cobro, saldo_servicio_madre, saldo_despacho_madre=0, argumento=None):
    """Elección «cubierta» / «cobra» (crítica 5). El servidor calcula el saldo de servicio que queda en los documentos de
    la madre; la persona elige:
      · 'cobra' → la hija toma su línea (docs_tomas) y pasa por el camino normal;
      · 'cubierta' → si queda saldo libre, escribe un argumento (≥ 30) de por qué se deja sin cobrar.
    Devuelve (error|None, cobro_normalizado). Sin saldo no se pregunta: es 'cubierta'."""
    cobro = (str(cobro or "").strip().lower()) or None
    saldo = max(0, int(round(float(saldo_servicio_madre or 0)))) + max(0, int(round(float(saldo_despacho_madre or 0))))
    if cobro is None:
        cobro = COBRO_CUBIERTA if saldo <= 0 else None
        if cobro is None:
            return {"codigo": "COBRO_REQUERIDO",
                    "mensaje": "La nota de venta aún tiene saldo de servicio: elige si la desinstalación lo cobra "
                               "o queda cubierta por la instalación."}, None
    if cobro not in COBROS_VALIDOS:
        return {"codigo": "COBRO_INVALIDO", "mensaje": "Elige «cubierta por la instalación» o «se cobra»."}, None
    if cobro == COBRO_PROPIO and saldo <= 0:
        return {"codigo": "SIN_SALDO_PARA_COBRAR",
                "mensaje": "La nota de venta no tiene saldo de servicio para que la desinstalación lo cobre."}, None
    if cobro == COBRO_CUBIERTA and saldo > 0 and len(str(argumento or "").strip()) < ARGUMENTO_MIN:
        return {"codigo": "ARGUMENTO_CUBIERTA",
                "mensaje": "Queda saldo de servicio sin usar en la nota de venta: explica por qué la desinstalación "
                           "va sin cobro (mínimo %d caracteres)." % ARGUMENTO_MIN}, None
    return None, cobro


# ───────────────────────── guardias de cancelar / reagendar / borrar ─────────────────────────
def guardia_cancelar_madre(hijas, incluir_desinstalacion=False):
    """Cancelar o anular la madre. Devuelve (accion, error): accion ∈ {'sola', 'con_hija'}.
      · sin hija vigente → 'sola'
      · hija sin empezar → 409 OT_CON_DESINSTALACION salvo `incluir_desinstalacion` ('con_hija': se cancelan las dos en
        la misma transacción)
      · hija ya empezada (o cerrada) → 409 DESINST_EN_CURSO con su número."""
    hv = hijas_vigentes(hijas)
    if not hv:
        return "sola", None
    h = hv[0]
    num = h.get("numero_ot") or "OT #%s" % h.get("id")
    if not sin_empezar(h.get("estado"), h):
        return None, {"codigo": "DESINST_EN_CURSO",
                      "mensaje": "La desinstalación %s ya empezó (%s): no se puede cancelar la instalación." % (
                          num, h.get("estado") or "—")}
    if incluir_desinstalacion:
        return "con_hija", None
    return None, {"codigo": "OT_CON_DESINSTALACION",
                  "mensaje": "Esta instalación tiene la desinstalación %s agendada. Cancela primero esa o confirma "
                             "cancelar las dos." % num, "hija_id": h.get("id"), "hija_numero": num}


def guardia_reagendar_madre(nueva_fecha, nueva_fecha_fin, hijas, nueva_hora_fin=None):
    """Reagendar la madre a una fecha POSTERIOR a la de su hija: 409 «Reagenda primero la desinstalación OT-…». Si queda el
    MISMO día, compara las horas (revisión 2026-10-20): la instalación no puede terminar después de que empieza la
    desinstalación (crítica 16 pedía horas, no solo fechas)."""
    hv = hijas_vigentes(hijas)
    if not hv:
        return None
    h = hv[0]
    num = h.get("numero_ot") or "OT #%s" % h.get("id")
    fh = parse_fecha(h.get("fecha_programada"))
    f_nueva = parse_fecha(nueva_fecha_fin) or parse_fecha(nueva_fecha)
    if fh and f_nueva and f_nueva > fh:
        return {"codigo": "REAGENDAR_DESINSTALACION_PRIMERO",
                "mensaje": "Reagenda primero la desinstalación %s: quedaría antes que la instalación." % num,
                "hija_id": h.get("id")}
    if fh and f_nueva and f_nueva == fh:
        hi_h, hf_m = hora_min(h.get("hora_inicio")), hora_min(nueva_hora_fin)
        if hi_h is not None and hf_m is not None and hf_m > hi_h:
            return {"codigo": "REAGENDAR_DESINSTALACION_PRIMERO",
                    "mensaje": "Reagenda primero la desinstalación %s: el mismo día empieza a las %s y la instalación "
                               "terminaría a las %s." % (num, min_txt(hi_h), min_txt(hf_m)),
                    "hija_id": h.get("id")}
    return None


def guardia_borrar_madre(hijas):
    """Borrar una madre con hija vigente: rechazado (se borra primero la hija)."""
    hv = hijas_vigentes(hijas)
    if not hv:
        return None
    h = hv[0]
    return {"codigo": "MADRE_CON_DESINSTALACION",
            "mensaje": "La instalación tiene la desinstalación %s. Elimina o cancela primero la desinstalación." % (
                h.get("numero_ot") or "OT #%s" % h.get("id")), "hija_id": h.get("id")}


def guardia_cambio_tipo(ot, hijas):
    """El tipo de una OT con madre, o con hijas vigentes, o con desinstalación prevista, no se cambia (crítica 24)."""
    o = ot or {}
    if o.get("ot_madre_id") and str(o.get("ot_relacion") or "").strip().lower() != REL_SEGUIMIENTO:
        return {"codigo": "TIPO_PROTEGIDO",
                "mensaje": "Esta OT es la desinstalación de otra instalación: su tipo no se cambia."}
    if hijas_vigentes(hijas):
        return {"codigo": "TIPO_PROTEGIDO",
                "mensaje": "Esta instalación tiene una desinstalación agendada: su tipo no se cambia."}
    if o.get("desinstalacion_prevista"):
        return {"codigo": "TIPO_PROTEGIDO",
                "mensaje": "Esta instalación tiene una desinstalación prevista: su tipo no se cambia. Quita primero "
                           "la desinstalación prevista."}
    return None


def guardia_quitar_prevista(madre, hijas):
    """Quitar la desinstalación prevista: no mientras haya una hija vigente (primero se cancela esa)."""
    hv = hijas_vigentes(hijas)
    if hv:
        return {"codigo": "DESINSTALACION_AGENDADA",
                "mensaje": "La desinstalación %s sigue agendada: cancélala primero." % (
                    hv[0].get("numero_ot") or "OT #%s" % hv[0].get("id"))}
    return None


# ───────────────────────── estado de la desinstalación de una instalación ─────────────────────────
def estado_desinstalacion(madre, hijas, hoy=None):
    """Una SOLA fecha manda: con hija vigente, su fecha_programada; si no, la desinstalacion_prevista.
    Devuelve {codigo, fecha, hija_id, hija_numero, texto, nivel}:
      ninguna · por_definir (prevista sin fecha cierta no existe: ver sin_agendar) · sin_agendar (prevista, sin hija) ·
      agendada · hecha (hija cerrada) · vencida (la fecha pasó y no se hizo)."""
    m = madre or {}
    hoy = parse_fecha(hoy)
    hv = hijas_vigentes(hijas)
    prevista = parse_fecha(m.get("desinstalacion_prevista"))
    if hv:
        h = hv[0]
        fh = parse_fecha(h.get("fecha_programada"))
        num = h.get("numero_ot") or "OT #%s" % h.get("id")
        base = {"fecha": fh.isoformat() if fh else None, "hija_id": h.get("id"), "hija_numero": num}
        if str(h.get("estado") or "").lower() == ESTADO_CERRADA:
            return dict(base, codigo="hecha", nivel="verde", texto="Desinstalación %s hecha" % num)
        if fh and hoy and fh < hoy and sin_empezar(h.get("estado"), h):
            return dict(base, codigo="vencida", nivel="rojo",
                        texto="Desinstalación %s vencida (era el %s)" % (num, fmt_fecha(fh)))
        return dict(base, codigo="agendada", nivel="azul",
                    texto="Desinstalación programada: %s · %s" % (num, fmt_fecha(fh)))
    if prevista:
        base = {"fecha": prevista.isoformat(), "hija_id": None, "hija_numero": None}
        if hoy and prevista < hoy:
            return dict(base, codigo="vencida", nivel="rojo",
                        texto="Desinstalación sin agendar (se esperaba el %s)" % fmt_fecha(prevista))
        return dict(base, codigo="sin_agendar", nivel="ambar", texto="Desinstalación sin agendar")
    return {"codigo": "ninguna", "fecha": None, "hija_id": None, "hija_numero": None, "nivel": "gris",
            "texto": ""}


def bucket_estado(estado):
    """cerrada · por_cerrar · en_curso · pendiente · fuera."""
    e = str(estado or "").strip().lower()
    if e in ESTADOS_FUERA:
        return "fuera"
    if e == ESTADO_CERRADA:
        return "cerrada"
    if e in ESTADOS_POR_CERRAR:
        return "por_cerrar"
    if e == "en_curso":
        return "en_curso"
    return "pendiente"


# ───────────────────────── barrido diario («que no se olvide») ─────────────────────────
def aviso_barrido(madre, hijas, hoy=None, feriados=None, dias_antes=DIAS_HABILES_AVISO):
    """¿Hay que avisar por esta instalación? Devuelve None o {codigo, nivel, titulo, texto}:
      · 'por_agendar' — faltan ≤ N días hábiles para la desinstalación prevista y todavía no hay hija;
      · 'vencida' — la fecha prevista ya pasó sin hija;
      · 'hija_sin_madre_ejecutada' — llegó el día de la hija y la instalación (madre) no se ejecutó;
      · 'desinst_vencida' — la hija pasó su fecha sin hacerse y la instalación sí se ejecutó.
    Una instalación cancelada/anulada, o con la desinstalación ya hecha/agendada con tiempo, no avisa."""
    m = madre or {}
    hoy = parse_fecha(hoy)
    if hoy is None or not vigente(m.get("estado")):
        return None
    hv = hijas_vigentes(hijas)
    num_m = m.get("numero_ot") or "OT #%s" % m.get("id")
    if hv:
        h = hv[0]
        fh = parse_fecha(h.get("fecha_programada"))
        if (fh and fh <= hoy and sin_empezar(h.get("estado"), h)
                and bucket_estado(m.get("estado")) in ("pendiente", "en_curso")):
            return {"codigo": "hija_sin_madre_ejecutada", "nivel": "ambar",
                    "titulo": "La desinstalación toca y la instalación no se ejecutó",
                    "texto": "La desinstalación %s es para hoy o antes, pero la instalación %s sigue sin hacerse." % (
                        h.get("numero_ot") or "OT #%s" % h.get("id"), num_m)}
        # Revisión 2026-10-20: la desinstalación pasó su fecha sin hacerse y la instalación SÍ se ejecutó: antes no avisaba.
        if fh and fh < hoy and sin_empezar(h.get("estado"), h):
            return {"codigo": "desinst_vencida", "nivel": "rojo", "titulo": "La desinstalación quedó vencida",
                    "texto": "La desinstalación %s era para el %s y todavía no se hizo (la instalación %s ya se ejecutó)." % (
                        h.get("numero_ot") or "OT #%s" % h.get("id"), fmt_fecha(fh), num_m)}
        return None
    prevista = parse_fecha(m.get("desinstalacion_prevista"))
    if prevista is None:
        return None
    if prevista < hoy:
        return {"codigo": "vencida", "nivel": "rojo", "titulo": "Desinstalación sin agendar y vencida",
                "texto": "%s tenía la desinstalación prevista para el %s y todavía no está agendada." % (
                    num_m, fmt_fecha(prevista))}
    limite = restar_habiles(prevista, dias_antes, feriados)
    if limite is not None and hoy >= limite:
        return {"codigo": "por_agendar", "nivel": "ambar", "titulo": "Agenda la desinstalación",
                "texto": "%s tiene la desinstalación prevista para el %s y todavía no está agendada." % (
                    num_m, fmt_fecha(prevista))}
    return None


def aviso_cierre_madre(madre, hijas):
    """Al CERRAR una instalación con desinstalación prevista y sin hija vigente: aviso ámbar que NO bloquea."""
    m = madre or {}
    if not m.get("desinstalacion_prevista") or hijas_vigentes(hijas):
        return None
    return {"codigo": "cierre_sin_desinstalacion", "nivel": "ambar",
            "texto": "Esta instalación tiene una desinstalación prevista para el %s y todavía no está agendada." % (
                fmt_fecha(m.get("desinstalacion_prevista")))}


def resumen_instalacion_con_desinstalacion(fecha_desinst):
    """El texto que ve Daniel en la solicitud de autorización cuando la madre incluye su desinstalación (crítica 2)."""
    f = fmt_fecha_corta(fecha_desinst)
    return "Incluye desinstalación" + (" %s" % f if f else "")
