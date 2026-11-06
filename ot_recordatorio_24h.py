# -*- coding: utf-8 -*-
"""RECORDATORIO DE 24 HORAS DE LA ORDEN DE TRABAJO — cliente y técnico (Daniel, 5-nov-2026).

Daniel: «necesito que le recordemos también 24 horas tanto al técnico como al cliente… eso lo podemos establecer por el sistema de
comunicaciones, déjalo todo por el front». Y su manual MAN-SST-INS-01: todos los plazos son en HORAS HÁBILES (lunes a viernes de 08:00 a
17:00, con colación de 13:00 a 14:00).

Este módulo es PURO (sin base de datos, sin Flask, sin correo): decide CUÁNDO toca recordar una OT y arma las variables del texto. El
barrido que lo usa, las tablas y el envío viven en app.py (`_ot_recordatorio_barrido`), colgados del trabajo `simpliroute-poll` (cada 10 min).

REGLAS DE «CUÁNDO» (todas verificadas por pruebas en tests/test_ot_recordatorio_24h.py):
  · El recordatorio sale 24 horas antes del comienzo de la visita (sin hora definida, el comienzo se toma a las 08:00).
  · SOLO en horario de cobertura (día hábil, 08:00–13:00 y 14:00–17:00): nunca de madrugada, en la colación, de noche, fin de semana o feriado.
  · Si las 24 h caen fuera de cobertura (p. ej. visita el lunes a las 09:00 → el domingo a las 09:00), sale en el último tramo de
    cobertura anterior (el viernes por la tarde): es mejor avisar antes que no avisar.
  · Si la OT se creó DESPUÉS de ese momento (la visita es en menos de 24 h desde su creación), no se manda recordatorio: el aviso de
    creación ya le llegó hace instantes.
  · No se recuerda una visita que empieza en menos de una hora (llegaría tarde).
  · Un reagendamiento cambia la «clave de inicio» y, por tanto, vuelve a recordar la nueva fecha (nunca dos veces la misma).
CONFIDENCIALIDAD (REGLAS #19 y #26): ni el texto del cliente ni el del técnico llevan montos, costos, proveedores ni OC.
"""
import datetime as _dt

import ot_comunicacion as _ocom
import ot_correos_tecnicos as _otc

HORAS_ANTES = 24
MINUTOS_MINIMOS = 60                      # no se recuerda una visita que empieza en menos de esto
HORA_SIN_HORARIO = "08:00"                # visita sin hora definida: se toma el comienzo de la jornada
# Cobertura (manual MAN-SST-INS-01, §4): lunes a viernes 08:00–17:00 con colación 13:00–14:00. En minutos del día: [inicio, fin).
COBERTURA = ((8 * 60, 13 * 60), (14 * 60, 17 * 60))
MAX_DIAS_ATRAS = 12                       # cuántos días hacia atrás se busca un tramo de cobertura (fin de semana largo + feriados)

CORREO_CLIENTE = _ocom.CORREO_RECORDATORIO_CLIENTE
CORREO_TECNICO = _ocom.CORREO_RECORDATORIO_TECNICO


def _min(dt):
    return dt.hour * 60 + dt.minute


def inicio_ot(fecha, hora_inicio):
    """datetime (hora de Chile, ingenuo) en que empieza la visita, o None si la OT no tiene fecha (la fecha centinela de «todavía sin
    fecha», año 2099, cuenta como sin fecha)."""
    d = _otc.a_fecha(fecha)
    if not d:
        return None
    hm = _otc.hhmm(hora_inicio) or HORA_SIN_HORARIO
    return _dt.datetime(d.year, d.month, d.day, int(hm[:2]), int(hm[3:5]))


def clave_inicio(inicio):
    """Identifica «ESTA fecha y hora de visita» (idempotencia: no se recuerda dos veces lo mismo; si se reagenda, cambia)."""
    return inicio.strftime("%Y-%m-%d %H:%M") if inicio else ""


def en_cobertura(dt, es_habil):
    """¿`dt` cae dentro del horario de cobertura (día hábil, 08:00–13:00 o 14:00–17:00)?"""
    if not es_habil(dt.date()):
        return False
    m = _min(dt)
    return any(a <= m < b for a, b in COBERTURA)


def ultimo_instante_cobertura(dt, es_habil):
    """El último minuto de cobertura menor o igual a `dt` (ingenuo, Chile), o None si no hay en los últimos MAX_DIAS_ATRAS días."""
    for k in range(MAX_DIAS_ATRAS + 1):
        d = dt.date() - _dt.timedelta(days=k)
        if not es_habil(d):
            continue
        for a, b in reversed(COBERTURA):
            ini = _dt.datetime(d.year, d.month, d.day, a // 60, a % 60)
            fin = _dt.datetime(d.year, d.month, d.day, b // 60, b % 60) - _dt.timedelta(minutes=1)
            if ini > dt:
                continue
            return min(dt, fin)
    return None


def momento_envio(inicio, es_habil):
    """Desde cuándo se puede mandar el recordatorio de una visita que empieza en `inicio` (None si no se puede calcular).
    Normalmente es exactamente 24 h antes; si eso cae fuera de cobertura, es una hora antes del cierre del último tramo de cobertura previo
    (así el barrido, que corre cada 10 minutos, tiene tiempo de enviarlo dentro de ese mismo tramo)."""
    if not inicio:
        return None
    objetivo = inicio - _dt.timedelta(hours=HORAS_ANTES)
    if en_cobertura(objetivo, es_habil):
        return objetivo
    ult = ultimo_instante_cobertura(objetivo, es_habil)
    if ult is None:
        return None
    return ult - _dt.timedelta(minutes=60)


def toca_recordar(inicio, creada_chile, ahora, es_habil, ignorar_cobertura=False):
    """(sí, motivo). `ahora` y `creada_chile` son datetimes ingenuos en hora de Chile (creada puede ser None). `ignorar_cobertura` solo lo
    usa el modo «solo mirar» (dry-run) para poder revisar de noche qué se mandaría; el envío real nunca lo activa."""
    if not inicio:
        return False, "la OT no tiene fecha"
    if ahora >= inicio - _dt.timedelta(minutes=MINUTOS_MINIMOS):
        return False, "la visita empieza en menos de una hora o ya empezó"
    if not ignorar_cobertura and not en_cobertura(ahora, es_habil):
        return False, "fuera del horario de cobertura"
    envio = momento_envio(inicio, es_habil)
    if envio is None:
        return False, "sin tramo de cobertura previo"
    if ahora < envio:
        return False, "todavía no son 24 horas antes"
    if creada_chile is not None and creada_chile > envio:
        return False, "la OT se creó dentro de las 24 horas previas (ya recibió el aviso de creación)"
    return True, ""


def dia_visita_txt(inicio, hoy):
    """«hoy», «mañana» o «el martes 10/11/2026» (para el asunto y el texto)."""
    d = inicio.date()
    if d == hoy:
        return "hoy"
    if d == hoy + _dt.timedelta(days=1):
        return "mañana"
    return "el " + _otc.dia_txt(d)


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
#  Bloques de VALOR (Daniel, 5-nov-2026: «que el mensaje provoque verse inteligente y aporte valor»)
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
# Consejos para el CLIENTE según el tipo de trabajo. Son recomendaciones prácticas (no promesas ni condiciones): ayudan a que la visita salga a
# la primera. Un tipo que no está aquí usa los generales. Todo texto plano; se escapa al dibujarlo.
_CONSEJOS_GENERALES = (
    "Deja libre el acceso a los equipos del trabajo y evita que se usen mientras dure la visita.",
    "Procura que haya una persona que pueda recibir al equipo y confirmar la recepción al terminar.",
)
CONSEJOS_CLIENTE = {
    "instalacion": (
        "Deja despejado el lugar donde irá cada equipo y el camino de acceso (puertas, escaleras, ascensor).",
        "Confirma con tu administración el horario de acceso y descarga, si el recinto lo exige.",
        "Procura que haya una persona que pueda recibir al equipo y confirmar la recepción al terminar.",
    ),
    "preventiva": (
        "Deja los equipos sin uso durante la visita y con espacio libre alrededor para trabajar.",
        "Anota las fallas, ruidos o molestias que hayas notado para contárselas al técnico.",
        "Procura que haya una persona que pueda recibir al equipo y confirmar la recepción al terminar.",
    ),
    "correctiva": (
        "Cuéntale al técnico desde cuándo falla el equipo y qué pasaba en ese momento: acelera el diagnóstico.",
        "Si tienes fotos o videos de la falla, déjalos a mano.",
        "Deja el equipo sin uso hasta la visita y con espacio libre alrededor.",
    ),
    "garantia": (
        "Ten a mano la factura o boleta de compra del equipo: el técnico la necesita para registrar la garantía.",
        "Cuéntale al técnico desde cuándo falla el equipo y qué pasaba en ese momento.",
        "Deja el equipo sin uso hasta la visita y con espacio libre alrededor.",
    ),
    "desinstalacion": (
        "Deja el equipo desocupado y con espacio libre alrededor para poder desarmarlo.",
        "Confirma con tu administración el horario de acceso y salida de equipos, si el recinto lo exige.",
        "Procura que haya una persona que pueda confirmar la entrega al terminar.",
    ),
}
CONSEJOS_CLIENTE["visita_tecnica"] = CONSEJOS_CLIENTE["inspeccion"] = CONSEJOS_CLIENTE["levantamiento"] = (
    "Deja libre el acceso a los equipos que se van a revisar y evita usarlos durante la visita.",
    "Ten a mano a la persona que conoce los equipos o el recinto: el técnico puede tener preguntas.",
)
CONSEJOS_CLIENTE["capacitacion"] = (
    "Reúne con anticipación a las personas que van a participar y reserva el espacio y los equipos para la capacitación.",
    "Confirma que el horario indicado les acomoda a todos.",
)
CONSEJOS_CLIENTE["cambio_equipo"] = CONSEJOS_CLIENTE["instalacion"]

# Lo que el TÉCNICO debe cuidar al salir (manual MAN-SST-INS-01: checklist por equipo, fotos antes y después, firmas).
ANTES_DE_SALIR = (
    "Revisa el checklist de cada equipo en la OT y confirma que llevas las herramientas y repuestos necesarios.",
    "En terreno: foto del antes y del después de cada equipo, y el checklist completo.",
    "Al terminar, pide la firma del cliente en terreno y deja tu resumen de lo que encontraste y lo que hiciste.",
)


def clave_tipo(tipo):
    """El tipo de OT de la base normalizado a las claves de CONSEJOS_CLIENTE ('', 'otro' si no se conoce)."""
    t = str(tipo or "").strip().lower().replace(" ", "_")
    for k in ("instalacion", "preventiva", "correctiva", "garantia", "desinstalacion", "visita_tecnica", "inspeccion",
              "levantamiento", "capacitacion", "cambio_equipo"):
        if t.startswith(k):
            return k
    return "mantencion" if t.startswith("mantenc") else "otro"


def consejos_cliente(tipo):
    k = clave_tipo(tipo)
    if k == "mantencion":
        k = "preventiva"
    return CONSEJOS_CLIENTE.get(k, _CONSEJOS_GENERALES)


def _plural(n, uno, varios):
    return uno if n == 1 else varios


def que_esperar_html(equipos, personas):
    """«Qué va a pasar» para el CLIENTE: quién llega, a cuántos equipos y cómo cierra la visita (con su firma, como dice el manual)."""
    eq = [str(e).strip() for e in (equipos or []) if str(e or "").strip()]
    pe = [str(p).strip() for p in (personas or []) if str(p or "").strip()]
    lineas = [("Llegará " + ", ".join(pe) + " en el horario indicado." if pe else "Nuestro equipo técnico llegará en el horario indicado.")]
    if eq:
        # SOLO la cantidad (Daniel, 6-nov: «excluye los productos de los correos… se agrandarían»): los nombres están en la OT.
        lineas.append(f"Trabajará en {len(eq)} {_plural(len(eq), 'equipo', 'equipos')}.")
    lineas.append("Al terminar, el técnico te pedirá confirmar la recepción del trabajo con tu firma.")
    return _ocom.caja_valor("Qué va a pasar", _ocom.lista_valor(lineas), "#0ea5e9")


def preparacion_html(tipo):
    """«Para dejar todo listo» para el CLIENTE, según el tipo de trabajo."""
    return _ocom.caja_valor("Para dejar todo listo", _ocom.lista_valor(consejos_cliente(tipo)), "#16a34a")


def url_mapa(direccion):
    """Enlace de Google Maps a una dirección (búsqueda por texto). '' si no hay una dirección real."""
    d = str(direccion or "").strip()
    if not d or d in ("—", "-") or d == _otc.OCULTO_ANEXO:
        return ""
    from urllib.parse import quote
    return "https://www.google.com/maps/search/?api=1&query=" + quote(d, safe="")


def como_llegar_html(direccion):
    """Botón «Cómo llegar» (Google Maps) para el TÉCNICO."""
    u = url_mapa(direccion)
    if not u:
        return ""
    return ('<p style="margin:0 0 6px;font-size:13px"><a href="' + _otc.esc(u) + '" style="background:#0a0a0a;color:#fff;padding:9px 20px;'
            'border-radius:6px;text-decoration:none;font-weight:bold;display:inline-block">Cómo llegar</a></p>')


def _tel_legible(tel):
    """Teléfono para mostrar: solo dígitos, +, espacios y guiones (nada de texto libre). '' si no queda nada útil."""
    t = "".join(ch for ch in str(tel or "") if ch.isdigit() or ch in "+ -")
    t = " ".join(t.split())
    return t if sum(ch.isdigit() for ch in t) >= 7 else ""


def contacto_fila(nombre, telefono):
    """Una fila «Contacto en terreno: Nombre · teléfono» para la tabla del TÉCNICO ('' si no hay nada)."""
    n = str(nombre or "").strip()
    t = _tel_legible(telefono)
    partes = [p for p in (_otc.esc(n) if n else "", _otc.esc(t) if t else "") if p]
    if not partes:
        return ""
    return ('<tr><td style="padding:5px 0;font-size:13px;color:#555"><strong style="color:#222">Contacto en terreno:</strong>&nbsp; '
            + " &middot; ".join(partes) + "</td></tr>")


def notas_html(descripcion):
    """«Qué hay que hacer» para el TÉCNICO: el texto de la OT sin plata ni proveedores (`limpiar_texto_tecnico`). '' si no queda nada."""
    t = _otc.limpiar_texto_tecnico(descripcion, 400)
    return _ocom.caja_valor("Qué hay que hacer", _otc.esc(t), "#dc2626") if t else ""


def equipos_lista_html(equipos):
    """Antes listaba los equipos uno por línea para el TÉCNICO. 6-nov-2026 (Daniel: «excluye los productos de los correos… se agrandarían y se pondrían feos»):
    ya NO se listan; la fila «Equipos» de la tabla dice la cantidad y el detalle está en la OT (el botón «Ver la OT»). Devuelve '' siempre (la variable
    `{{equipos_lista_html}}` de la plantilla queda vacía)."""
    return ""


def antes_de_salir_html():
    return _ocom.caja_valor("Antes de salir", _ocom.lista_valor(ANTES_DE_SALIR), "#f59e0b")


def variables_recordatorio(ot, inicio, hoy, *, nombre_destinatario="", link_ot="", para="cliente", oculto=False, url_seguimiento=""):
    """Variables de los dos correos (escapadas para HTML). `ot`: id, numero_ot, cliente, tipo_label, tipo, fecha, hora_inicio, hora_fin, direccion,
    equipos (lista de nombres), tecnicos (lista de personas), descripcion, contacto_nombre, contacto_tel. El horario, si no se definió, dice
    «por confirmar».

    `para`: 'cliente' llena «Qué va a pasar» y «Para dejar todo listo»; 'tecnico' llena «Cómo llegar», el contacto en terreno, «Qué hay que hacer»,
    la lista de equipos y «Antes de salir». `oculto` (técnico con el Anexo de Servicios sin firmar): sin dirección, contacto ni «qué hay que hacer»
    (la misma regla de la ficha y de los demás correos a técnicos). `url_seguimiento`: el enlace público de seguimiento de la OT (REGLA #67);
    solo el correo al cliente lo dibuja (botón «Ver seguimiento en vivo» dentro del recorrido); sin URL no hay botón."""
    base = _ocom.variables_ot(ot, nombre_destinatario=nombre_destinatario, link_ot=link_ot,
                              tecnicos_nombres=ot.get("tecnicos") or [])
    base["fecha"] = _otc.esc(_otc.fecha_txt(inicio.date()) or "—")
    base["dia"] = _otc.esc(dia_visita_txt(inicio, hoy))
    base["horario"] = _otc.esc(_otc.horario_txt(ot.get("hora_inicio"), ot.get("hora_fin")))
    if para == "cliente":
        base["que_esperar_html"] = que_esperar_html(ot.get("equipos"), ot.get("tecnicos"))
        base["preparacion_html"] = preparacion_html(ot.get("tipo"))
        # «Tu servicio, paso a paso»: el recordatorio es el paso 2; lo que sigue es la visita (siempre es verdad: es el proceso de Servicio Técnico).
        base["recorrido_html"] = _ocom.recorrido_html(
            "recordatorio", "nuestro técnico llega en el horario indicado y, al terminar, te pedirá confirmar el trabajo con tu firma.",
            url_seguimiento=url_seguimiento)
    else:
        if not oculto:
            base["como_llegar_html"] = como_llegar_html(ot.get("direccion"))
            base["contacto_fila"] = contacto_fila(ot.get("contacto_nombre"), ot.get("contacto_tel"))
            base["notas_html"] = notas_html(ot.get("descripcion"))
        base["equipos_lista_html"] = equipos_lista_html(ot.get("equipos"))
        base["antes_de_salir_html"] = antes_de_salir_html()
    return base
