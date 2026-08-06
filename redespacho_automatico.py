"""Motor de decisión de REDESPACHO AUTOMÁTICO tras una entrega fallida.

Daniel, 2026-08-05, textual: "quiero agotar todas las instancias de manera
automática... que en el sistema me salga identificado que se volvió a subir
y que será entregado en x días... rápido, automático y trazable... utiliza
toda la inteligencia posible."

LA PARTE "INTELIGENTE" ES ESTA: no todos los couriers pueden reintentarse
igual, y tratarlos como si pudieran sería peor que no automatizar nada.

    · SimpliRoute (Felca, Milling, cualquier courier que integre por ahí):
      ILUS YA sabe recrear una visita — hoy es un botón manual
      (/transporte/api/items/<id>/simpliroute/reenviar). Se puede automatizar
      con seguridad: se sabe pedir, se sabe confirmar, se sabe fallar limpio.

    · FedEx: ILUS puede crear guías por API, pero FedEx gestiona sus PROPIOS
      reintentos según su servicio contratado. Generar una guía nueva
      automáticamente ante una entrega fallida arriesga DUPLICAR el envío y
      el cobro — no resuelve nada, empeora las cosas. Por eso el motor NUNCA
      recomienda auto-crear un envío FedEx: enruta a gestión humana,
      inmediata y trazable, no en silencio.

    · Shipit: hoy (2026-08) solo cotiza (Fase 1 en producción). No existe
      todavía una API de creación de envío ni de reintento -- no hay nada
      que automatizar. Blindado explícitamente para no fingir una
      automatización que no existe.

    · Clickex: sin API en absoluto (Daniel, 2026-08-05: "no sé cómo
      trabajaría Clickex, que estoy pidiendo también la API"). Mismo
      blindaje que Shipit.

MÓDULO PURO: sin Flask, sin BD, sin red -- mismo patrón que shipit_client.py
y simpliroute_client.py. Así se puede testear sin levantar la app y sin
tocar SimpliRoute/FedEx de verdad. app.py orquesta llamando a estas
funciones puras y haciendo el I/O (leer intentos previos, llamar a la API
real, escribir el resultado, notificar al cliente).

Test: tests/test_redespacho_automatico.py
"""
import re
from datetime import timedelta

# ── Política por courier ────────────────────────────────────────────────
# Máximo de reintentos AUTOMÁTICOS antes de exigir intervención humana.
# Daniel no fijó un número: se elige 2 como default conservador -- "agotar
# las instancias" no debe leerse como "reintentar para siempre" (un domicilio
# con dirección mal escrita fallará las veces que se reintente).
MAX_REINTENTOS_AUTOMATICOS = 2

ACCION_CREAR_VISITA_SIMPLIROUTE = "crear_visita_simpliroute"
ACCION_GESTION_HUMANA_FEDEX     = "gestion_humana_fedex"
ACCION_GESTION_HUMANA_SIN_API   = "gestion_humana_sin_api"
ACCION_TOPE_ALCANZADO           = "tope_alcanzado"

_ACCIONES_VALIDAS = {
    ACCION_CREAR_VISITA_SIMPLIROUTE,
    ACCION_GESTION_HUMANA_FEDEX,
    ACCION_GESTION_HUMANA_SIN_API,
    ACCION_TOPE_ALCANZADO,
}


def _tipo_courier(courier_nombre, simpliroute_courier_integra_fn):
    """Clasifica el courier según su capacidad REAL de redespacho.

    `simpliroute_courier_integra_fn` se inyecta (en vez de importar app.py)
    para que este módulo siga sin depender de Flask/BD -- app.py pasa su
    propia `_simpliroute_courier_integra`.
    """
    nom = (courier_nombre or "").strip().lower()
    if not nom:
        return "desconocido"
    if simpliroute_courier_integra_fn and simpliroute_courier_integra_fn(courier_nombre):
        return "simpliroute"
    if "fedex" in nom:
        return "fedex"
    if "shipit" in nom:
        return "shipit"
    if "clickex" in nom:
        return "clickex"
    return "desconocido"


def evaluar_reintento(courier_nombre, intentos_previos, *,
                       simpliroute_courier_integra_fn=None,
                       max_reintentos=MAX_REINTENTOS_AUTOMATICOS):
    """Decide qué hacer ante una entrega fallida. NUNCA lanza excepción.

    Devuelve dict:
        accion:        una de _ACCIONES_VALIDAS
        puede_automatizar: bool -- True SOLO si `accion` crea algo de verdad
        motivo_operativo:  para logs/auditoría (ES, técnico)
        motivo_cliente:    frase lista para mostrar al cliente (ES, sin jerga)

    `intentos_previos` es el número de redespachos AUTOMÁTICOS ya hechos
    para este documento (0 en el primer fallo).
    """
    intentos_previos = max(0, int(intentos_previos or 0))
    tipo = _tipo_courier(courier_nombre, simpliroute_courier_integra_fn)

    if tipo == "simpliroute":
        if intentos_previos >= max_reintentos:
            return {
                "accion": ACCION_TOPE_ALCANZADO,
                "puede_automatizar": False,
                "motivo_operativo": (
                    f"Ya se intentó redespachar automáticamente "
                    f"{intentos_previos} vez(es) con {courier_nombre} sin éxito. "
                    f"Se detiene el reintento automático (tope={max_reintentos}) "
                    f"para no repetir un fallo que probablemente sea de "
                    f"dirección o disponibilidad del cliente."),
                "motivo_cliente": (
                    "Ya intentamos la entrega más de una vez sin éxito. "
                    "Nuestro equipo te va a contactar directamente para coordinar."),
            }
        return {
            "accion": ACCION_CREAR_VISITA_SIMPLIROUTE,
            "puede_automatizar": True,
            "motivo_operativo": (
                f"{courier_nombre} integra con SimpliRoute: se puede recrear "
                f"la visita automáticamente (intento {intentos_previos + 1} "
                f"de {max_reintentos})."),
            "motivo_cliente": "Reprogramamos tu entrega automáticamente.",
        }

    if tipo == "fedex":
        return {
            "accion": ACCION_GESTION_HUMANA_FEDEX,
            "puede_automatizar": False,
            "motivo_operativo": (
                "FedEx gestiona sus propios reintentos según el servicio "
                "contratado. Crear una guía nueva automáticamente arriesga "
                "DUPLICAR el envío y el cobro -- se enruta a un humano en "
                "vez de adivinar."),
            "motivo_cliente": (
                "Tu pedido está con FedEx. Nuestro equipo va a hacer "
                "seguimiento directo con ellos y te va a avisar apenas "
                "tengamos una fecha confirmada."),
        }

    if tipo in ("shipit", "clickex"):
        nombre_visible = "Shipit" if tipo == "shipit" else "Clickex"
        return {
            "accion": ACCION_GESTION_HUMANA_SIN_API,
            "puede_automatizar": False,
            "motivo_operativo": (
                f"{nombre_visible} todavía no tiene una API de creación de "
                f"envío/reintento conectada a ILUS -- no hay nada que "
                f"automatizar hasta que esa integración exista."),
            "motivo_cliente": (
                "Nuestro equipo va a coordinar directamente el reintento "
                "de tu entrega y te va a confirmar la nueva fecha."),
        }

    return {
        "accion": ACCION_GESTION_HUMANA_SIN_API,
        "puede_automatizar": False,
        "motivo_operativo": (
            f"Courier «{courier_nombre}» no reconocido por el motor de "
            f"redespacho automático -- se enruta a gestión humana por "
            f"seguridad, no se adivina."),
        "motivo_cliente": (
            "Nuestro equipo va a coordinar directamente el reintento "
            "de tu entrega y te va a confirmar la nueva fecha."),
    }


# ── Próxima fecha estimada (hábil, Chile) ───────────────────────────────
# ILUS no opera sábado/domingo (ver memoria del proyecto). El redespacho no
# se agenda para el día siguiente a secas: si el fallo ocurre un viernes,
# el próximo intento hábil es el lunes, no el sábado.
def proxima_fecha_habil(fecha_falla, *, dias_habiles_despues=1):
    """`fecha_falla` es un date/datetime. Devuelve un date hábil (lun-vie),
    saltando fin de semana, `dias_habiles_despues` días hábiles adelante.
    """
    fecha = fecha_falla.date() if hasattr(fecha_falla, "date") else fecha_falla
    restantes = max(1, int(dias_habiles_despues or 1))
    while restantes > 0:
        fecha = fecha + timedelta(days=1)
        if fecha.weekday() < 5:   # 0=lunes ... 4=viernes
            restantes -= 1
    return fecha


def mensaje_redespacho_cliente(*, courier_nombre, fecha_estimada_str, accion):
    """Texto lo bastante concreto para bajar la ansiedad CON un hecho, no
    solo con tono. Daniel: "no solo un mensaje que baje la ansiedad, sino
    la gestión automática... que me diga que se generó una entrega tal día,
    de manera inmediata"."""
    if accion == ACCION_CREAR_VISITA_SIMPLIROUTE:
        return (f"Ya reprogramamos tu entrega con {courier_nombre}. "
                f"Nuevo intento estimado: {fecha_estimada_str}.")
    return (f"Estamos coordinando un nuevo intento de entrega con "
            f"{courier_nombre}. Te confirmamos la fecha apenas la tengamos "
            f"— dentro del próximo día hábil.")
