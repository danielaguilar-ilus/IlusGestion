# -*- coding: utf-8 -*-
"""ILUS Fitness · OT — HALLAZGOS DE LA OT (F1 · REGLA #38): funciones puras, sin BD ni Flask.

Daniel 2026-10-20, revisando la OT de Aurum (OT-2026-00171): «Indícame si se está tomando fotografías como evidencia, eso es necesario, y que
la OT avise para que se tome el caso y se levante una acción que debería ser otra OT.» Hasta hoy una Falla o una Alerta del checklist, un
equipo «que no se puede usar», una baja, un repuesto pedido o una incidencia quedaban ENTERRADOS en la OT: nadie recibía la campana, nadie
«tomaba el caso» y no nacía ninguna OT de seguimiento.

Decisiones de Daniel (no se vuelven a preguntar):
  1. Solo AVISAR, no trabar: un hallazgo sin gestionar NO impide cerrar la OT (la franja del cierre es ámbar, nunca un candado).
  2. El aviso va al RESPONSABLE de la OT (REGLA #36): a quién avisa «la gestión» lo decide UNA sola función de app.py
     (`_ot_gestion_destinatarios`); aquí no se arma ninguna lista de personas.
  3. La acción que se levanta es OTRA OT (de seguimiento), por el asistente «Nueva OT» ya existente, PRELLENADA (cliente, equipos, tipo
     correctiva, título y descripción con el hallazgo, fotos como referencia) y ligada a la OT de origen. Lo prellenado lo decide la
     persona (REGLA #32: el sistema sugiere, la persona decide).
  4. Un técnico NO ve esta gestión; su flujo de trabajo no cambia.

Qué es un hallazgo (nada de conceptos nuevos: salen de lo que el técnico ya registra):
  · una TAREA de checklist de tipo «verificación» respondida Alerta o Falla, o el «Resultado del control» (lista que ofrece «Apto» y
    «Apto con observaciones») con una opción distinta de «Apto» y «No llegó»;
  · una acción por producto o equipo: «No se puede usar» (fuera de servicio), «No tiene arreglo» (baja), «Pedir repuesto» e «Informar
    incidencia»; y un equipo con falla detectada (diagnóstico «Con falla»).
  «No llegó · falta stock» NO es un hallazgo: lo cubre «¿Qué pasó?» (REGLA #35).

Aquí viven el catálogo (severidades, estados), la evaluación de lo que el técnico respondió, las claves únicas, qué hacer cuando un hallazgo
reaparece, las reglas de «tomar», «descartar» y «levantar», la validación y el prellenado de la OT de seguimiento y los textos de la campana.
Las rutas, la base, la hora Chile y la campana viven en app.py (bloque «HALLAZGOS DE LA OT»).
"""
import datetime as _dt
import json
import re
import unicodedata

REL_SEGUIMIENTO = "seguimiento"     # mant_visitas.ot_relacion de la OT que nace de un hallazgo (ot_madre_id = la OT de origen)
TIPO_AVISO = "ot_hallazgo"          # mant_notificaciones.tipo de la campana «N hallazgos nuevos» (va al FINAL de las dos listas del ENUM: REGLA #18)
TIPO_AVISO_TOMA = "ot_hallazgo_toma"  # tipo del aviso suave «alguien tomó un hallazgo» (propio, para no chocar con la campana de arriba)
TIPO_OT_SEGUIMIENTO = "correctiva"  # el tipo que el asistente sugiere (la persona lo puede cambiar)

ORIGEN_TAREA, ORIGEN_PRODUCTO, ORIGEN_EQUIPO = "tarea", "producto", "equipo"
# 2026-10-27 (Daniel: «las inquietudes elimínalas… déjala como hallazgo»): lo que el técnico anota con «El cliente me comentó…» es un hallazgo más.
ORIGEN_CLIENTE = "cliente"
ORIGENES = (ORIGEN_TAREA, ORIGEN_PRODUCTO, ORIGEN_EQUIPO, ORIGEN_CLIENTE)

SEV_FALLA, SEV_FUERA_SERVICIO, SEV_BAJA = "falla", "fuera_servicio", "baja"
SEV_ALERTA, SEV_REPUESTO, SEV_INCIDENCIA = "alerta", "repuesto", "incidencia"

# Semáforo (REGLA #2/#15): rojo = hay que actuar ya, ámbar = mirar, azul = en camino, gris = informado. `orden` ordena la lista.
SEVERIDADES = {
    SEV_FALLA: {"texto": "Falla", "color": "rojo", "icono": "bi-exclamation-octagon-fill", "orden": 1, "prioridad": "alta"},
    SEV_FUERA_SERVICIO: {"texto": "Fuera de servicio", "color": "rojo", "icono": "bi-slash-circle-fill", "orden": 2, "prioridad": "alta"},
    SEV_BAJA: {"texto": "Dar de baja", "color": "negro", "icono": "bi-x-octagon-fill", "orden": 3, "prioridad": "alta"},
    SEV_ALERTA: {"texto": "Alerta", "color": "ambar", "icono": "bi-exclamation-triangle-fill", "orden": 4, "prioridad": "media"},
    SEV_REPUESTO: {"texto": "Repuesto", "color": "azul", "icono": "bi-tools", "orden": 5, "prioridad": "media"},
    SEV_INCIDENCIA: {"texto": "Incidencia", "color": "gris", "icono": "bi-flag-fill", "orden": 6, "prioridad": "media"},
}

EST_ABIERTO, EST_TOMADO, EST_RESUELTO, EST_DESCARTADO = "abierto", "tomado", "resuelto", "descartado"
ESTADOS = {
    EST_ABIERTO: {"texto": "Sin gestionar", "color": "ambar"},
    EST_TOMADO: {"texto": "Caso tomado", "color": "azul"},
    EST_RESUELTO: {"texto": "Con OT de seguimiento", "color": "verde"},
    EST_DESCARTADO: {"texto": "Descartado", "color": "gris"},
}
# Los estados en los que un hallazgo todavía admite una acción (tomar, levantar la OT, descartar).
ESTADOS_GESTIONABLES = (EST_ABIERTO, EST_TOMADO)

ESTADOS_OT_CAIDA = ("cancelada", "anulada")      # una OT de seguimiento cancelada deja el hallazgo otra vez sin gestionar
ESTADOS_OT_CERRADA = ("cerrada", "completada")   # una OT de seguimiento en estos estados ya terminó (la aprobación pendiente NO cuenta)

# 2026-10-22 (auditoría Impeccable, H-12). Semáforo de Retiros: ámbar = te toca, azul = espera, verde = hecho. Un hallazgo «con OT de
# seguimiento» todavía NO está hecho mientras esa OT siga abierta: se PINTA azul («en curso»); solo se pinta verde cuando esa OT ya se
# cerró. El estado GUARDADO sigue siendo 'resuelto' (lo leen cinco pantallas y sus pruebas): esto es únicamente cómo se muestra.
TXT_SEGUIMIENTO_EN_CURSO = "OT de seguimiento en curso"
TXT_SEGUIMIENTO_CERRADA = "OT de seguimiento cerrada"

VENTANA_AVISO_MIN = 30        # una sola campana por OT y por persona agrupando lo que llegue en estos minutos
DESCARTE_MIN = 10             # el motivo de un descarte: mínimo de letras (queda en la bitácora)
DESCARTE_MAX = 400
DETALLE_MAX = 1500
TITULO_MAX = 300
FOTOS_MAX = 12                # fotos que se guardan por hallazgo
FOTOS_REFERENCIA_MAX = 6      # fotos que pasan como referencia a la OT de seguimiento
HALLAZGOS_MAX_POR_SEGUIMIENTO = 20


# ── Utilidades ─────────────────────────────────────────────────────────────────────────────────────────────────────────────
def norm(s):
    """minúsculas, sin tildes y con espacios simples («No llegó» → «no llego»)."""
    s = unicodedata.normalize("NFD", str(s or ""))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return " ".join(s.lower().split())


def limpio(s, tope):
    return " ".join(str(s or "").split())[:tope]


def entero(x):
    """int positivo o None (nunca levanta; rechaza bool, NaN, 0 y negativos)."""
    try:
        if isinstance(x, bool) or x is None:
            return None
        if isinstance(x, str):
            x = x.strip()
            if not x:
                return None
        n = float(x)
    except (TypeError, ValueError):
        return None
    if n != n or n in (float("inf"), float("-inf")) or not float(n).is_integer() or n <= 0:
        return None
    return int(n)


def es_si(x):
    """True si el formulario dijo «sí» (si, sí, 1, true, yes, on). Cualquier otra cosa (o nada) es False."""
    if isinstance(x, bool):
        return x
    return norm(x) in ("si", "1", "true", "yes", "on")


def lista_enteros(x, maximo=HALLAZGOS_MAX_POR_SEGUIMIENTO):
    """[ids] sin repetir y en orden desde un texto «1,2,3», una lista o un solo número. Descarta lo que no sea un entero positivo."""
    if isinstance(x, str):
        crudos = re.split(r"[\s,;]+", x)
    elif isinstance(x, (list, tuple, set)):
        crudos = list(x)
    else:
        crudos = [x]
    out = []
    for c in crudos:
        n = entero(c)
        if n is not None and n not in out:
            out.append(n)
    return out[:maximo]


def _json(x, defecto):
    if isinstance(x, (dict, list)):
        return x
    if not x:
        return defecto
    try:
        return json.loads(x)
    except (TypeError, ValueError):
        return defecto


def lista_urls(x, maximo=FOTOS_MAX):
    """Las URL de fotos de un `fotos_json` (lista de textos o de {url}). Sin vacíos ni repetidas."""
    out = []
    for it in (_json(x, []) if not isinstance(x, list) else x) or []:
        u = it.get("url") if isinstance(it, dict) else it
        u = str(u or "").strip()
        if u and u not in out:
            out.append(u[:700])
    return out[:maximo]


# ── Qué es un hallazgo: la tarea que el técnico respondió ────────────────────────────────────────────────────────────────
_RES_APTO, _RES_APTO_OBS, _RES_NO_LLEGO = "apto", "apto con observaciones", "no llego"


def es_resultado(opciones):
    """El «Resultado del control» de la revisión: una lista que ofrece «Apto» y «Apto con observaciones» (mismo criterio que
    app.py::_chk_es_resultado, que es la regla de las exigencias de la revisión). Una lista cualquiera NO es un hallazgo."""
    ops = [norm(o) for o in (_json(opciones, []) if not isinstance(opciones, list) else opciones) if isinstance(o, str)]
    return _RES_APTO in ops and _RES_APTO_OBS in ops


def severidad_resultado(opcion):
    """Severidad de la opción elegida en el «Resultado del control», o None si no es un hallazgo (Apto, No llegó, vacío).
    Mismo mapa que el acta de recepción (app.py::_acta_resultado_canon): «Reparar» → falla, «Dar de baja» → baja, «Fuera de servicio» →
    fuera de servicio, «Apto con observaciones» y «Pendiente de diagnóstico» → alerta. Una opción que nadie conoce y no es «Apto»
    también se avisa como alerta: es mejor un aviso de más que un hallazgo enterrado."""
    n = norm(opcion)
    if not n or n in (_RES_APTO, _RES_NO_LLEGO, "sin resultado"):
        return None
    if n == _RES_APTO_OBS:
        return SEV_ALERTA
    # Palabras completas (o su raíz al comienzo de la palabra): «Preparar para la venta» o «Puede seguir trabajando» NO son una
    # reparación ni una baja aunque lleven «repar» o «baja» por dentro.
    if re.search(r"\brepar", n):
        return SEV_FALLA
    if re.search(r"\bbaja\b", n):
        return SEV_BAJA
    if "fuera de servicio" in n or re.search(r"\bno apto\b", n):
        return SEV_FUERA_SERVICIO
    if "pendiente" in n or "diagnostic" in n:
        return SEV_ALERTA
    if n.startswith("no lleg"):
        return None
    return SEV_ALERTA


def evaluar_tarea(t):
    """¿Esta tarea, con la respuesta que tiene HOY, es un hallazgo? → None o {severidad, valor_txt, clase}.
    `t` trae tipo_respuesta, valor_json (dict o texto) y opciones_lista_json. «No aplica» NO es un hallazgo (la respuesta anterior se
    conserva en la tarea como evidencia, pero ya no se avisa)."""
    t = t or {}
    vj = _json(t.get("valor_json"), {})
    if not isinstance(vj, dict) or vj.get("na"):
        return None
    tipo = str(t.get("tipo_respuesta") or "").strip().lower()
    if tipo == "verificacion":
        v = str(vj.get("valor") or "").strip().lower()
        if v == "falla":
            return {"severidad": SEV_FALLA, "valor_txt": "Falla", "clase": "verificacion"}
        if v == "alerta":
            return {"severidad": SEV_ALERTA, "valor_txt": "Alerta", "clase": "verificacion"}
        return None
    if tipo == "lista":
        if not es_resultado(t.get("opciones_lista_json")):
            return None
        opcion = str(vj.get("opcion") or "").strip()
        sev = severidad_resultado(opcion)
        if not sev:
            return None
        return {"severidad": sev, "valor_txt": opcion[:120], "clase": "resultado"}
    return None


def respuesta_puede_ser_hallazgo(valor_norm):
    """Camino rápido del gancho de la respuesta de una tarea (se llama por CADA respuesta del técnico): según el `valor_norm` que
    devolvió el servidor, ¿hay algo que hacer? → 'evaluar' (puede ser un hallazgo: Alerta/Falla o una opción de lista), 'retirar'
    (volvió a «Aprobado» o quedó «No aplica»: si había un hallazgo, se anota como retirado) o None (texto, número, GPS, serie…: nada)."""
    vn = valor_norm if isinstance(valor_norm, dict) else {}
    if vn.get("na"):
        return "retirar"
    v = str(vn.get("valor") or "").strip().lower()
    if v in ("alerta", "falla"):
        return "evaluar"
    if v == "aprobado":
        return "retirar"
    if "opcion" in vn:
        return "evaluar"
    return None


# ── Claves únicas (una por cosa distinta; así lo mismo no se duplica) ───────────────────────────────────────────────────
def ref_tarea(tid):
    return "t%d" % int(tid)


def ref_producto(imid, clave, extra=None):
    """«i4:fuera_servicio», «i4:rep:ab12cd34», «i4:inc:55»."""
    return "i%d:%s%s" % (int(imid), clave, (":" + str(extra)) if extra not in (None, "") else "")


def ref_equipo(mid, clave, extra=None):
    """«m9:fuera_servicio», «m9:baja», «m9:falla», «m9:rep:ab12cd34», «m9:inc:55»."""
    return "m%d:%s%s" % (int(mid), clave, (":" + str(extra)) if extra not in (None, "") else "")


# ── Textos ─────────────────────────────────────────────────────────────────────────────────────────────────────────────
def sev_info(sev):
    return SEVERIDADES.get(sev) or SEVERIDADES[SEV_ALERTA]


def nombre_con_ficha(nombre, sku=None, serie=None):
    """«Trotadora X (SKU ABC) · S/N 123» — el nombre COMPLETO, nunca cortado (REGLA #15)."""
    t = limpio(nombre, 300) or "Equipo"
    if sku:
        t += " (SKU %s)" % limpio(sku, 60)
    if serie:
        t += " · S/N %s" % limpio(serie, 60)
    return t


def titulo_tarea(sujeto, tarea, valor_txt):
    """«Trotadora X (SKU ABC) · Revisar cinta: Falla»."""
    partes = [p for p in (limpio(sujeto, 200), limpio(tarea, 200)) if p]
    base = " · ".join(partes) or "Tarea del checklist"
    return ("%s: %s" % (base, limpio(valor_txt, 80)) if valor_txt else base)[:TITULO_MAX]


def titulo_accion(sujeto, texto):
    """«Banco plano X: No se puede usar — pieza rota o faltante»."""
    s = limpio(sujeto, 200)
    return ("%s: %s" % (s, limpio(texto, 200)) if s else limpio(texto, 200))[:TITULO_MAX]


# ── Cómo se MUESTRA lo guardado (auditoría Impeccable del 21-oct, fase 1): nada de esto cambia lo que hay en la base ───────────
FORMATO_FECHA = "%d/%m/%Y %H:%M"        # H-24 · REGLA #6: día/mes/año y hora de Chile, siempre con el año
_SEP_SERIE = " · S/N "


def titulo_corto(titulo, equipo_nombre):
    """El título SIN repetir el equipo (H-16). Lo guardado es «<equipo> · <tarea>: <valor>» o «<equipo>: <qué pasó>» (titulo_tarea y
    titulo_accion) y la bandeja ya pone el nombre COMPLETO del equipo (REGLA #15) una sola vez, en el encabezado de su grupo. Si el título no
    empieza por el equipo (o es solo el equipo) se devuelve tal cual: nunca se inventa ni se corta nada."""
    t = limpio(titulo, 2 * TITULO_MAX)
    e = limpio(equipo_nombre, 400)
    if not t or not e:
        return t
    # titulo_tarea corta el sujeto a 200 letras (y el corte puede dejar un espacio al final); equipo_nombre guarda hasta 400.
    for pref in dict.fromkeys((e.rstrip(), limpio(e, 200).rstrip())):
        if pref and t.startswith(pref):
            for sep in (" · ", ": "):
                if t[len(pref):].startswith(sep):
                    resto = t[len(pref) + len(sep):].strip()
                    if resto:
                        return resto
    return t


def partir_serie(nombre):
    """(nombre, serie) de un nombre guardado con el hallazgo. `nombre_con_ficha` le agrega « · S/N …» al final con la serie DE ESE MOMENTO;
    aquí se separa para no repetirla (H-15). «Banco X (SKU 1) · S/N 123» → («Banco X (SKU 1)», «123»). Sin serie → (nombre, "")."""
    t = limpio(nombre, 400)
    i = t.rfind(_SEP_SERIE)
    if i < 0:
        return t, ""
    return t[:i].rstrip(), t[i + len(_SEP_SERIE):].strip()


def equipo_con_serie(nombre_guardado, serie_vigente=None):
    """«Banco X (SKU 1) · S/N 123» con la serie UNA sola vez: la vigente de la ficha del equipo (H-15). Si la que se guardó al reportar era
    otra se dicen las dos, rotuladas: «… · S/N 456 (al reportar: S/N 123)». Sin ninguna serie, solo el nombre."""
    base, vieja = partir_serie(nombre_guardado)
    base = base or "Equipo"
    nueva = limpio(serie_vigente, 60)
    if nueva and vieja and norm(nueva) != norm(vieja):
        return "%s%s%s (al reportar: S/N %s)" % (base, _SEP_SERIE, nueva, vieja)
    serie = nueva or vieja
    return base + (_SEP_SERIE + serie if serie else "")


_RX_EVIDENCIA = re.compile(r"(\s*·\s*)?Evidencia: (\d+) foto\(s\)(?:, (\d+) video\(s\))?\.?")


def detalle_visible(detalle, hay_fotos):
    """El detalle como se ve en la tarjeta (H-16). «Pedir repuesto» lo guarda terminado en «Evidencia: N foto(s)[, M video(s)].»: con las
    miniaturas a la vista, contar las fotos sobra; los videos se conservan (la tarjeta no los muestra). Lo guardado no se modifica."""
    t = str(detalle or "").strip()
    if not t or not hay_fotos:
        return t

    def _sin_fotos(m):
        videos = int(m.group(3) or 0)
        return ((m.group(1) or "") + "Evidencia: %d video(s)." % videos) if videos else ""
    return _RX_EVIDENCIA.sub(_sin_fotos, t).strip(" ·\t\n")


def lapso_texto(dias):
    """«menos de un día» · «1 día» · «N días» (H-23: antes salía «hace el mismo día»). Quien lo usa pone «hace …» o «En …»."""
    try:
        d = int(dias)
    except (TypeError, ValueError):
        return ""
    return "menos de un día" if d <= 0 else ("1 día" if d == 1 else "%d días" % d)


# Resumen «Estado de los equipos» (pestaña Trabajo de la OT, H-22): el motivo corto de un equipo que se saltó. Mismas razones que
# app.py::_RAZONES_SALTADO_VALIDAS. «No está / no llegó» va en ámbar (el equipo no estaba o no llegó); el resto, en gris.
SALTO_NO_ESTA = {"no_encontrado": "No estaba en el recinto", "falta_stock": "No llegó · falta stock"}
SALTO_MOTIVO_CORTO = {
    "sin_acceso": "sin acceso", "cliente_no_permite": "el cliente no lo permitió", "equipo_en_uso": "estaba en uso",
    "danado_inaccesible": "dañado o inaccesible", "dañado_inaccesible": "dañado o inaccesible", "sin_energia": "sin energía",
    "requiere_repuesto": "necesita un repuesto",
    "reagendar": "hay que reagendarlo", "cliente_lo_quito": "el cliente lo retiró", "no_aplica": "no aplica", "otro": "otro motivo",
}


def chip_salto(razon, parcial=False):
    """(texto, color) del chip de un equipo con `razon_saltado`, con el MOTIVO corto a la vista. Sin estados contradictorios: «No se pudo
    revisar» solo si de verdad no se revisó nada; si ya hay algo respondido o reportado en ese equipo (`parcial`), dice «Revisión incompleta»."""
    r = str(razon or "").strip().lower()
    if r in SALTO_NO_ESTA:
        return SALTO_NO_ESTA[r], "ambar"
    motivo = SALTO_MOTIVO_CORTO.get(r) or r.replace("_", " ")
    return ("Revisión incompleta" if parcial else "No se pudo revisar") + (": " + motivo if motivo else ""), "gris"


def grupo_abierto(items, n_grupos=2):
    """¿El grupo de un equipo se muestra ABIERTO? (H-20) Sí si la OT tiene un solo equipo (no hay nada que plegar) o si el grupo tiene algo de
    gravedad alta (falla, fuera de servicio, baja o riesgo para alguien) que todavía admite una acción. El resto queda plegado, con su
    contador a la vista. `items`: los hallazgos del grupo, con `estado_ef` (o `estado`), `severidad` y `riesgo`."""
    # 2026-10-26, Daniel: «no me gustan que las tarjetas se contraigan, me gusta ver el bosque siempre… si falta algo o ya está
    # gestionado déjalo abierto». Todo grupo nace ABIERTO; la persona puede plegarlo a mano si quiere, el sistema nunca.
    return True


def resumen_repuestos(lineas, maximo=4):
    """«Piola (cable de acero) × 2, Pin selector × 1 y 2 más» — solo nombres de pieza y cantidades (REGLAS #19/#26: nada de proveedor)."""
    partes = []
    for li in (lineas or []):
        nombre = limpio((li or {}).get("nombre") or (li or {}).get("repuesto_nombre"), 120)
        if not nombre:
            continue
        try:
            cant = float((li or {}).get("cantidad") or 1)
        except (TypeError, ValueError):
            cant = 1
        partes.append("%s × %s" % (nombre, ("%g" % cant)))
    if not partes:
        return "repuestos"
    if len(partes) > maximo:
        return ", ".join(partes[:maximo]) + " y %d más" % (len(partes) - maximo)
    return ", ".join(partes)


def estado_info(estado):
    return ESTADOS.get(estado) or ESTADOS[EST_ABIERTO]


def seguimiento_cerrado(seg_estado):
    """¿La OT de seguimiento ya terminó (cerrada o completada)? Una OT que espera su aprobación todavía no."""
    return str(seg_estado or "").strip().lower() in ESTADOS_OT_CERRADA


def estado_visual(estado, seg_cerrada=False):
    """Texto y color CON QUE SE VE un estado (H-12). Igual que `estado_info`, salvo «con OT de seguimiento» ('resuelto'): azul y «OT de
    seguimiento en curso» mientras esa OT siga abierta; verde y «OT de seguimiento cerrada» solo cuando ya se cerró. El estado guardado no
    cambia: esto es solo la capa de presentación. → {texto, color}."""
    if estado == EST_RESUELTO:
        return {"texto": TXT_SEGUIMIENTO_CERRADA if seg_cerrada else TXT_SEGUIMIENTO_EN_CURSO,
                "color": "verde" if seg_cerrada else "azul"}
    return dict(estado_info(estado))


# ── Cuando el mismo hallazgo aparece otra vez ──────────────────────────────────────────────────────────────────────────
def decidir_reaparicion(fila):
    """El técnico volvió a marcar algo que YA existe como hallazgo. → {estado, avisar, reabre}.
      · Activo (no retirado): sigue igual, no se avisa de nuevo.
      · Retirado (el técnico lo había devuelto a «Aprobado» o la acción se revirtió) y ahora reaparece:
          - estaba «descartado» por una persona → reabre como «abierto» y SE AVISA (hay información nueva);
          - estaba «abierto» → vuelve a estar activo y se avisa (la campana de la ventana de 30 minutos se junta sola);
          - estaba «tomado» o con OT de seguimiento → sigue en manos de quien lo tiene, sin avisar otra vez."""
    f = fila or {}
    estado = str(f.get("estado") or EST_ABIERTO)
    if not f.get("retirado_en"):
        return {"estado": estado, "avisar": False, "reabre": False}
    if estado == EST_DESCARTADO:
        return {"estado": EST_ABIERTO, "avisar": True, "reabre": True}
    if estado == EST_ABIERTO:
        return {"estado": EST_ABIERTO, "avisar": True, "reabre": True}
    return {"estado": estado, "avisar": False, "reabre": True}


def estado_efectivo(fila, seg_numero=None, seg_estado=None):
    """El estado que se muestra y se cuenta. Un hallazgo con OT de seguimiento cuya OT ya no existe o quedó cancelada/anulada vuelve a
    estar SIN gestionar (si no, el trabajo quedaría escondido como «resuelto»). → (estado, seguimiento_caido)."""
    f = fila or {}
    estado = str(f.get("estado") or EST_ABIERTO)
    seg_id = f.get("ot_seguimiento_id")
    if seg_id and estado in (EST_TOMADO, EST_RESUELTO):
        if not seg_numero and not seg_estado:
            return EST_ABIERTO, True
        if str(seg_estado or "").strip().lower() in ESTADOS_OT_CAIDA:
            return EST_ABIERTO, True
    return estado, False


def seguimiento_vigente(fila, seg_estado=None, seg_existe=True):
    """¿Este hallazgo ya tiene una OT de seguimiento que sigue viva?"""
    if not (fila or {}).get("ot_seguimiento_id"):
        return False
    if not seg_existe:
        return False
    return str(seg_estado or "").strip().lower() not in ESTADOS_OT_CAIDA


# ── Acciones de la gestión ─────────────────────────────────────────────────────────────────────────────────────────────
def puede_accion(accion, fila, seg_estado=None, seg_existe=True):
    """(ok, mensaje). `accion`: tomar | levantar | descartar | reabrir. El mensaje (para la persona) explica por qué no."""
    f = fila or {}
    estado = str(f.get("estado") or EST_ABIERTO)
    retirado = bool(f.get("retirado_en"))
    con_ot = seguimiento_vigente(f, seg_estado, seg_existe)
    if accion == "tomar":
        if retirado:
            return False, "El técnico retiró este hallazgo: ya no hay nada que tomar."
        if con_ot:
            return False, "Este hallazgo ya tiene una OT de seguimiento."
        if estado == EST_DESCARTADO:
            return False, "Este hallazgo fue descartado. Si hace falta, reábrelo primero."
        if estado == EST_TOMADO:
            return False, "Este caso ya lo tomó %s." % (f.get("tomado_por") or "otra persona")
        return True, ""
    if accion == "levantar":
        if retirado:
            return False, "El técnico retiró este hallazgo: ya no hay nada que seguir."
        if con_ot:
            return False, "Este hallazgo ya tiene una OT de seguimiento."
        if estado == EST_DESCARTADO:
            return False, "Este hallazgo fue descartado. Si hace falta, reábrelo primero."
        return True, ""
    if accion == "descartar":
        if con_ot:
            return False, "Este hallazgo ya tiene una OT de seguimiento: no se descarta."
        if estado == EST_DESCARTADO:
            return False, "Este hallazgo ya está descartado."
        return True, ""
    if accion == "reabrir":
        if retirado:
            return False, "El técnico retiró este hallazgo: ya no hay nada que reabrir."
        if estado != EST_DESCARTADO:
            return False, "Solo se reabre un hallazgo descartado."
        return True, ""
    return False, "Esa acción no existe."


def validar_descarte(motivo):
    """(motivo_limpio, None) o (None, mensaje). El motivo queda escrito en la bitácora: mínimo DESCARTE_MIN letras."""
    m = limpio(motivo, DESCARTE_MAX)
    if len(m) < DESCARTE_MIN:
        return None, "Cuenta por qué se descarta (mínimo %d caracteres): queda escrito en la bitácora de la OT." % DESCARTE_MIN
    return m, None


# ── La OT de seguimiento ─────────────────────────────────────────────────────────────────────────────────────────────────
def validar_seguimiento(origen, hallazgos, ids, cliente_id_nueva, seg_info=None):
    """Revisa, con lo que hay en la BASE (nunca con lo que dice el navegador), que estos hallazgos se puedan ligar a la OT nueva.
      origen: fila de la OT de origen (id, cliente_id, estado) o None.
      hallazgos: filas de mant_ot_hallazgos de esa OT (con id, visita_id, estado, retirado_en, ot_seguimiento_id).
      ids: los que la persona eligió.   cliente_id_nueva: el cliente de la OT que se está creando (None = trabajo interno).
      seg_info: {hallazgo_id: {existe, estado}} de la OT de seguimiento ya ligada, para saber si sigue viva.
    → (filas_elegidas, None) o (None, (mensaje, codigo))."""
    if not origen:
        return None, ("No encontramos la OT de la que viene este seguimiento.", "SEG_ORIGEN_NO_EXISTE")
    ids = lista_enteros(ids)
    if not ids:
        return None, ("Elige al menos un hallazgo para el seguimiento.", "SEG_SIN_HALLAZGOS")
    por_id = {int(h["id"]): h for h in (hallazgos or []) if h.get("id") is not None}
    elegidas = []
    for i in ids:
        h = por_id.get(i)
        if not h or int(h.get("visita_id") or 0) != int(origen["id"]):
            return None, ("Uno de los hallazgos no es de la OT de origen. Recarga la ficha y vuelve a elegir.", "SEG_HALLAZGO_AJENO")
        info = (seg_info or {}).get(i) or {}
        ok, msg = puede_accion("levantar", h, info.get("estado"), info.get("existe", True))
        if not ok:
            return None, (msg, "SEG_HALLAZGO_NO_DISPONIBLE")
        elegidas.append(h)
    cli_o = origen.get("cliente_id")
    if (int(cli_o) if cli_o else None) != (int(cliente_id_nueva) if cliente_id_nueva else None):
        return None, ("La OT de seguimiento tiene que ser del mismo cliente que la OT de origen (o de trabajo interno si la origen lo es). "
                      "Quita el vínculo para crear una OT de otro cliente.", "SEG_OTRO_CLIENTE")
    return elegidas, None


def armar_prellenado(origen, items):
    """Lo que el asistente «Nueva OT» muestra ya escrito para el seguimiento (la persona lo cambia como quiera, REGLA #32).
      origen: {numero_ot, cliente_nombre, tipo}.   items: [{id, severidad, titulo, detalle, equipo, maquina_id, fotos}] (ya filtrados).
    → {titulo, descripcion, maquinas, fotos, tipo_sugerido}."""
    numero = limpio((origen or {}).get("numero_ot"), 40) or "la OT de origen"
    cliente = limpio((origen or {}).get("cliente_nombre"), 160)
    if len(items) == 1:
        it = items[0]
        titulo = "Seguimiento de %s · %s" % (numero, limpio(it.get("equipo") or it.get("titulo"), 140))
    else:
        titulo = "Seguimiento de %s · %d hallazgos" % (numero, len(items)) + ((" · %s" % cliente) if cliente else "")
    lineas = []
    maquinas, fotos = [], []
    for it in items:
        info = sev_info(it.get("severidad"))
        t = "• %s — %s" % (info["texto"], limpio(it.get("titulo"), 240))
        det = limpio(it.get("detalle"), 300)
        if det:
            t += ". " + det
        lineas.append(t)
        mid = entero(it.get("maquina_id"))
        if mid and mid not in maquinas:
            maquinas.append(mid)
        for u in lista_urls(it.get("fotos")):
            if u not in fotos:
                fotos.append(u)
    fotos = fotos[:FOTOS_REFERENCIA_MAX]
    desc = "Seguimiento de %s%s. Lo que reportó el técnico en terreno:\n%s" % (
        numero, (" (%s)" % cliente) if cliente else "", "\n".join(lineas))
    if fotos:
        desc += "\nFotos del hallazgo (%d): quedan en «Videos y documentos» de esta OT como referencia." % len(fotos)
    return {"titulo": titulo[:200], "descripcion": desc[:2000], "maquinas": maquinas, "fotos": fotos,
            "tipo_sugerido": TIPO_OT_SEGUIMIENTO}


def descripcion_foto_referencia(numero_origen, titulo):
    return ("Hallazgo de %s · %s" % (numero_origen or "la OT de origen", limpio(titulo, 220)))[:300]


# ── La campana ───────────────────────────────────────────────────────────────────────────────────────────────────────────
def decidir_grupo(ultimo_grupo, ahora, ventana_min=VENTANA_AVISO_MIN):
    """La campana agrupa lo que llegue en `ventana_min` minutos: si hubo un aviso de esta OT dentro de la ventana, los nuevos se SUMAN a
    ese grupo; si no, empieza uno nuevo. → (grupo, continua). `ultimo_grupo` y `ahora` son datetimes del MISMO reloj (el de la base).
    Si la base no entregó la hora (no debería pasar), se usa la del servidor para que el grupo nunca quede vacío."""
    if not isinstance(ahora, _dt.datetime):
        ahora = _dt.datetime.utcnow().replace(microsecond=0)
    if ultimo_grupo and isinstance(ultimo_grupo, _dt.datetime):
        if _dt.timedelta(0) <= (ahora - ultimo_grupo) < _dt.timedelta(minutes=ventana_min):
            return ultimo_grupo, True
    return ahora, False


def decidir_campana(existente, continua):
    """¿Se actualiza la campana que ya tiene esa persona para esta OT, o se crea una nueva?
    `existente`: la última campana no archivada de (OT, persona) → {leida: bool} o None.
      · No hay ninguna → se crea.
      · Hay una sin leer → se actualiza (una sola campana por OT; el conteo se pone al día).
      · Hay una leída y el grupo continúa (menos de 30 minutos) → se actualiza y vuelve a quedar sin leer (llegó información nueva).
      · Hay una leída y el grupo es nuevo → se crea otra."""
    if not existente:
        return "crear"
    if not existente.get("leida"):
        return "actualizar"
    return "actualizar" if continua else "crear"


def titulo_campana(numero_ot, n):
    """«OT-2026-00171: 3 hallazgos nuevos» — una sola línea por OT (con el enlace a la ficha)."""
    n = max(1, int(n or 1))
    return ("%s: %d %s" % (numero_ot or "OT", n, "hallazgo nuevo" if n == 1 else "hallazgos nuevos"))[:200]


def prioridad_campana(items):
    """'urgente' si alguno puede lastimar a alguien; 'alta' si hay una falla, un equipo que no se puede usar o una baja; si no 'media'."""
    pri = "media"
    for it in (items or []):
        if it.get("riesgo"):
            return "urgente"
        if sev_info(it.get("severidad"))["prioridad"] == "alta":
            pri = "alta"
    return pri


def cuerpo_campana(cliente, items, quien=None, maximo=3):
    """Sin plata, sin documentos, sin proveedores (REGLAS #19/#26). Nombres completos: «Trotadora X: Falla…; Banco Y: No se puede usar»."""
    partes = []
    for it in (items or [])[:maximo]:
        partes.append("%s — %s%s" % (sev_info(it.get("severidad"))["texto"], limpio(it.get("titulo"), 160),
                                     " (PUEDE LASTIMAR A ALGUIEN)" if it.get("riesgo") else ""))
    extra = max(0, len(items or []) - maximo)
    t = "%s: %s" % (limpio(cliente, 120) or "Sin cliente", "; ".join(partes))
    if extra:
        t += "; y %d más" % extra
    t += ". Entra a la OT para tomar el caso o levantar una OT de seguimiento."
    if quien:
        t += " Lo reportó %s." % limpio(quien, 80)
    return t[:1500]


def cuerpo_toma(quien, titulo):
    """Aviso suave al responsable cuando OTRA persona toma el caso (nada que suene a castigo)."""
    return ("%s tomó el caso: %s. No hace falta que hagas nada: ya hay alguien a cargo." % (limpio(quien, 80) or "Alguien del equipo",
                                                                                         limpio(titulo, 220)))[:1500]


def titulo_toma(numero_ot):
    return ("%s: alguien tomó un hallazgo" % (numero_ot or "OT"))[:200]


# ── Conteos ───────────────────────────────────────────────────────────────────────────────────────────────────────────────
def resumir(items):
    """{total, abiertos, tomados, con_ot, descartados, retirados} de la lista que ve la ficha (cada item con `estado_ef` y `retirado`)."""
    r = {"total": 0, "abiertos": 0, "tomados": 0, "con_ot": 0, "descartados": 0, "retirados": 0}
    for it in (items or []):
        r["total"] += 1
        if it.get("retirado"):
            r["retirados"] += 1
            continue
        e = it.get("estado_ef") or it.get("estado")
        if e == EST_ABIERTO:
            r["abiertos"] += 1
        elif e == EST_TOMADO:
            r["tomados"] += 1
        elif e == EST_RESUELTO:
            r["con_ot"] += 1
        elif e == EST_DESCARTADO:
            r["descartados"] += 1
    return r


def sql_abierto(alias="h", seg="s"):
    """Fragmento SQL (UNA sola definición de «sin gestionar» para el panel, el filtro y la franja del cierre): activo (no retirado) y
    abierto, o con OT de seguimiento que ya no existe o quedó cancelada/anulada. `alias` y `seg` son nombres FIJOS del código, nunca de
    una persona: el SQL que llama debe unir la OT de seguimiento (LEFT JOIN mant_visitas <seg> ON <seg>.id = <alias>.ot_seguimiento_id)."""
    for a in (alias, seg):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", a or ""):
            raise ValueError("alias inválido")
    return ("({a}.retirado_en IS NULL AND ({a}.estado='abierto' OR ({a}.estado IN ('tomado','resuelto') "
            "AND {a}.ot_seguimiento_id IS NOT NULL AND ({s}.id IS NULL OR {s}.estado IN ('cancelada','anulada')))))"
            ).format(a=alias, s=seg)


# ── 2026-10-26: el MONITOR de hallazgos con el lenguaje de Retiros (Daniel: «no veo similitud con la parte de retiros, no están las
#    tarjetas… quisiera verla como el módulo de retiros: identificar quién solicita, de qué OT, de qué cliente, todo bien trazable») ──
VISTAS_MONITOR = {
    "sin_gestionar": "Sin gestionar",
    "vencidos": "Plazo vencido",
    "tomados": "Caso tomado",
    "con_ot": "Con OT de seguimiento",
    "descartados": "Descartados",
    "todos": "Todos",
}


def sql_vista(vista, alias="h", seg="s"):
    """El WHERE de cada vista del monitor, con la MISMA definición de «sin gestionar» (`sql_abierto`). Solo hallazgos activos."""
    ab = sql_abierto(alias, seg)
    a = alias
    if vista == "sin_gestionar":
        return ab
    if vista == "tomados":
        return "(NOT {ab} AND {a}.estado='tomado')".format(ab=ab, a=a)
    if vista == "con_ot":
        return "(NOT {ab} AND {a}.estado='resuelto')".format(ab=ab, a=a)
    if vista == "descartados":
        return "({a}.estado='descartado')".format(a=a)
    return "1=1"


def hace_txt(horas):
    """«hace 3 h» · «hace 2 días» (desde que el técnico lo reportó). Sin plata, sin fechas en inglés (REGLA #6)."""
    try:
        h = max(0.0, float(horas or 0))
    except (TypeError, ValueError):
        return ""
    if h < 1:
        return "hace menos de 1 h"
    if h < 24:
        return "hace %d h" % int(h)
    d = int(h // 24)
    return "hace 1 día" if d == 1 else "hace %d días" % d
