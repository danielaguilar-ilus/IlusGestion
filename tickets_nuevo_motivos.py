# -*- coding: utf-8 -*-
"""Tickets 2.0 (27-oct-2026) · motivos principales SUGERIDOS por tipo de solicitud.

Daniel: «el motivo: lista sugerida según el tipo (el sistema SUGIERE, la
persona elige o escribe)» — REGLA #32: nunca se impone. Lo que se guarda en
`tk_tickets.motivo_principal` (VARCHAR(160)) es lo que la persona eligió o
escribió; estas listas solo acortan el camino en el modal «Nuevo Ticket».

Módulo puro (sin Flask ni base de datos) para poder probarlo solo. La clave
es el `tipo` de TK_TIPOS (tickets_module.py); un tipo que no esté acá recibe
la lista `_GENERICOS`. Se incluyen `facturacion` y `consulta` a propósito:
otro trabajo los está sumando a TK_TIPOS y así nacen con sugerencias sin
tocar nada más.

Criterio (mesa de ayuda de equipos de gimnasio): 4 a 7 motivos por tipo,
en el lenguaje del cliente, del más frecuente al menos frecuente, sin
tecnicismos internos y sin datos de plata (REGLA #26).
"""

MOTIVOS_POR_TIPO = {
    "install": (
        "Agendar instalación",
        "Reprogramar la instalación",
        "Instalación incompleta",
        "Falta un accesorio o una pieza",
        "Equipo llegó dañado",
        "Consulta de requisitos del lugar",
    ),
    "tech_support": (
        "Equipo no enciende",
        "Ruido o vibración anormal",
        "Error en la consola o pantalla",
        "Banda o correa desalineada",
        "Se detiene durante el uso",
        "Problema de lubricación",
        "Consulta de uso del equipo",
    ),
    "shipping": (
        "Consulta de fecha de entrega",
        "Pedido atrasado",
        "Entrega incompleta",
        "Producto dañado en el transporte",
        "Cambio de dirección de entrega",
    ),
    "quotation": (
        "Cotizar mantención",
        "Cotizar instalación",
        "Cotizar repuesto",
        "Cotizar equipo nuevo",
        "Cotizar traslado de equipos",
    ),
    "return": (
        "Producto con falla de fábrica",
        "Llegó un producto equivocado",
        "No cumple lo esperado",
        "Devolución por garantía",
    ),
    "tech_evaluation": (
        "Evaluar el estado del equipo",
        "Informe técnico para seguro",
        "Evaluación previa a una compra",
        "Diagnóstico de falla recurrente",
    ),
    "maintenance": (
        "Mantención preventiva programada",
        "Mantención por contrato",
        "Equipo con uso intensivo",
        "Lubricación y ajuste",
        "Mantención post instalación",
    ),
    "spare_parts": (
        "Repuesto por desgaste",
        "Repuesto por falla",
        "Consulta de disponibilidad",
        "Pieza faltante en la entrega",
    ),
    "equipment_transfer": (
        "Traslado entre sucursales",
        "Reubicación dentro del gimnasio",
        "Desinstalación y traslado",
        "Traslado a bodega",
    ),
    "warranty": (
        "Falla dentro de garantía",
        "Consulta de cobertura",
        "Reclamo de garantía",
    ),
    "repair": (
        "Reparación de falla mecánica",
        "Reparación de falla eléctrica",
        "Reparación de consola",
        "Reparación de tapiz o estructura",
        "Cambio de pieza dañada",
    ),
    "spare_parts_store": (
        "Compra de repuesto en bodega",
        "Reposición de stock",
        "Repuesto urgente",
    ),
    "spare_parts_import": (
        "Importación de repuesto",
        "Seguimiento de una importación",
        "Repuesto sin stock local",
    ),
    "control_calidad": (
        "Revisión de ingreso a bodega",
        "Revisión por reclamo de cliente",
        "Muestreo de lote",
        "Revisión antes de despachar",
    ),
    "trabajo_bodega": (
        "Orden y reubicación",
        "Armado de equipo",
        "Preparación de pedido",
        "Inventario",
    ),
    "capacitacion": (
        "Capacitación de uso al cliente",
        "Capacitación técnica interna",
        "Inducción de personal nuevo",
    ),
    "facturacion": (
        "Error en la factura",
        "Solicitud de nota de crédito",
        "Consulta de pago o saldo",
        "Cambio de datos de facturación",
    ),
    "consulta": (
        "Consulta general",
        "Información de un producto",
        "Estado de una solicitud anterior",
        "Horarios y cobertura",
    ),
}

_GENERICOS = (
    "Consulta general",
    "Reclamo",
    "Seguimiento de una solicitud anterior",
)

LARGO_MAXIMO = 160   # tk_tickets.motivo_principal VARCHAR(160)


def motivos_para(tipo):
    """Lista de motivos sugeridos para un tipo (siempre al menos una opción)."""
    return list(MOTIVOS_POR_TIPO.get(str(tipo or "").strip(), _GENERICOS))


def motivos_todos():
    """{tipo: [motivos…]} listo para serializar al navegador."""
    return {t: list(m) for t, m in MOTIVOS_POR_TIPO.items()}


def normalizar_motivo(texto):
    """Lo que escribió o eligió la persona, listo para guardar (o None)."""
    t = " ".join(str(texto or "").split())
    return t[:LARGO_MAXIMO] or None
