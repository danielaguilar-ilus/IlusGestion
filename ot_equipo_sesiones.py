# -*- coding: utf-8 -*-
"""Sesiones de trabajo por equipo dentro de una OT (Daniel, 2026-11-04).

Pedido: «si el técnico selecciona otro equipo debe contraer el equipo seleccionado y pausar sus tiempos para que continúe con el
otro, reconociendo si hay otro técnico trabajando un equipo». Hasta hoy el tiempo de un equipo se estimaba desde su primera y su
última tarea: nada se podía pausar. Una SESIÓN es un tramo en que UN técnico tuvo UN equipo abierto: empieza al abrirlo y termina
al abrir otro, contraerlo o salir de la OT. Nada se borra: cada tramo queda guardado con su motivo.

Módulo puro (sin base de datos ni Flask) para poder probarlo rápido. `app.py` guarda y lee las filas; aquí viven las reglas.

Reglas que NO se negocian:
  · Una pausa que nadie avisó (el teléfono se durmió, se perdió la señal, se cerró la pestaña) NO cuenta como trabajo: el tramo
    termina `GRACIA_SEG` después de su último latido, no cuando el técnico por fin vuelve a tocar la pantalla.
  · Que otro técnico esté en un equipo es un AVISO, nunca un candado: dos personas pueden repartirse un equipo grande.
  · El tiempo por equipo lo ve la gestión; el técnico no (Daniel 2026-09-08: «los tiempos del técnico no los necesita ver»).
"""
import re
from datetime import datetime, timedelta

# Cada cuánto manda el navegador un latido mientras el equipo sigue abierto y la pantalla visible.
LATIDO_SEG = 30
# Sin latido durante este tiempo la sesión se considera pausada: el tramo se corta en `último latido + GRACIA_SEG`.
GRACIA_SEG = 180
# «Trabajando ahora» = un latido más reciente que esto (tres latidos de margen, para no parpadear con una señal mala).
ACTIVO_SEG = 100

ACCIONES = ("iniciar", "latido", "pausar")
MOTIVOS_FIN = ("cambio", "contrajo", "salio", "inactivo", "cerrada")

_CLAVE_RE = re.compile(r"^[ei][0-9]{1,9}$")


def clave_valida(clave):
    """Un equipo del cliente es «e<id>» y un producto agregado a la OT es «i<id>» (mismo criterio que window.OTD_ACC)."""
    return isinstance(clave, str) and bool(_CLAVE_RE.match(clave))


def clave_de(maquina_id, interno=False):
    return ("i" if interno else "e") + str(int(maquina_id))


def _como_fecha(x):
    return x if isinstance(x, datetime) else None


def fin_efectivo(inicio, fin, ultimo_latido, ahora):
    """Hasta cuándo CUENTA una sesión: el fin real (o `ahora` si sigue abierta), pero nunca más allá del último latido + gracia."""
    inicio, fin, ultimo_latido, ahora = map(_como_fecha, (inicio, fin, ultimo_latido, ahora))
    if inicio is None:
        return None
    lat = ultimo_latido or inicio
    tope = lat + timedelta(seconds=GRACIA_SEG)
    corte = fin if fin is not None else (ahora or tope)
    return min(corte, tope)


def segundos(sesion, ahora):
    """Segundos de trabajo de UNA sesión (`inicio`, `fin`, `ultimo_latido`). Nunca negativo."""
    fe = fin_efectivo(sesion.get("inicio"), sesion.get("fin"), sesion.get("ultimo_latido"), ahora)
    ini = _como_fecha(sesion.get("inicio"))
    if fe is None or ini is None:
        return 0
    return max(0, int((fe - ini).total_seconds()))


def total_por_clave(sesiones, ahora):
    """{clave: segundos} sumando todas las sesiones (de todos los técnicos) de cada equipo."""
    out = {}
    for s in sesiones or []:
        c = s.get("clave")
        if not c:
            continue
        out[c] = out.get(c, 0) + segundos(s, ahora)
    return out


def sigue_viva(sesion, ahora):
    """¿La sesión está abierta Y con latido reciente? (la que cuenta como «trabajando ahora»)."""
    if sesion.get("fin") is not None:
        return False
    lat = _como_fecha(sesion.get("ultimo_latido")) or _como_fecha(sesion.get("inicio"))
    ahora = _como_fecha(ahora)
    if lat is None or ahora is None:
        return False
    return (ahora - lat).total_seconds() <= ACTIVO_SEG


def decidir_reanudacion(sesion_abierta, ahora):
    """Qué hacer con la sesión abierta de este técnico en este equipo cuando llega un latido o un «abrir».
    'refrescar' = seguía viva (se actualiza el latido) · 'reabrir' = estuvo parada más de la gracia (se cierra donde el último latido
    la dejó y nace otra: el hueco NO se cuenta) · 'nueva' = no tenía ninguna."""
    if not sesion_abierta:
        return "nueva"
    lat = _como_fecha(sesion_abierta.get("ultimo_latido")) or _como_fecha(sesion_abierta.get("inicio"))
    ahora = _como_fecha(ahora)
    if lat is None or ahora is None:
        return "reabrir"
    return "refrescar" if (ahora - lat).total_seconds() <= GRACIA_SEG else "reabrir"


def otros_trabajando(sesiones, ahora, yo_id=None):
    """{clave: [nombres]} de los OTROS técnicos que tienen ese equipo abierto ahora. Sin repetidos, en orden de llegada."""
    out = {}
    for s in sorted(sesiones or [], key=lambda x: x.get("inicio") or datetime.min):
        if not sigue_viva(s, ahora):
            continue
        if yo_id is not None and s.get("user_id") == yo_id:
            continue
        nombre = (s.get("user_nombre") or "").strip() or "Otro técnico"
        lista = out.setdefault(s.get("clave"), [])
        if nombre not in lista:
            lista.append(nombre)
    return out


def texto_aviso(nombres):
    """«Fulano está trabajando aquí» / «Fulano y Mengano están trabajando aquí» / «A, B y C están…»."""
    n = [x for x in (nombres or []) if x]
    if not n:
        return ""
    if len(n) == 1:
        return f"{n[0]} está trabajando aquí"
    return f"{', '.join(n[:-1])} y {n[-1]} están trabajando aquí"


def texto_duracion(seg):
    """Segundos -> «1h 5m» / «12m» / «40s». Vacío si no hubo trabajo registrado."""
    if not seg or seg <= 0:
        return ""
    seg = int(seg)
    h, resto = divmod(seg, 3600)
    m = resto // 60
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m"
    return f"{seg}s"
