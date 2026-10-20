# -*- coding: utf-8 -*-
"""Estado POR PRODUCTO de una OT de trabajo interno (Daniel, 2026-10-20).

Contexto. Daniel, con los técnicos operando la revisión de la NVI 687: «necesito que antes de comenzar los checklist
los técnicos tengan la posibilidad de darle de baja, de dejar fuera de servicio, de indicar que no llegó, de solicitar
piola o cualquier repuesto». Hasta ahora esas salidas vivían DENTRO de la plantilla (el ítem «Resultado del control»);
las plantillas «(LU)» de Lenin no lo traen y el técnico quedaba sin salida. Esta barra de acciones es INDEPENDIENTE del
checklist: cada producto tiene su propio estado, guardado en mant_ot_producto_estado (una fila por acción, con quién,
cuándo, por qué y la foto; nunca se borra: revertir marca la fila, no la elimina).

Este módulo es PURO (sin Flask, sin base de datos, sin red) para poder probarlo en milisegundos:

  · los textos y las listas de motivos (lenguaje de técnico),
  · la validación de cada acción (qué se exige y qué no),
  · cuándo un producto deja de exigir su checklist para cerrar,
  · el resultado que lee el acta de recepción,
  · el seguimiento de un pedido de repuesto (Pedido enviado → Bodega revisando → Pieza lista → Instalada),
  · los repuestos que se SUGIEREN por defecto según el sistema del producto (REGLA #32: el sistema sugiere, la
    persona decide; el campo de la plantilla reemplaza esta lista solo si una persona lo llenó).

Las rutas, la base y la hora Chile viven en app.py (REGLA #6, #19 y #26: nada de proveedores ni costos aquí).
"""
import re
import unicodedata

ESTADO_FUERA_SERVICIO = "fuera_servicio"
ESTADO_BAJA = "baja"
ESTADO_NO_LLEGO = "no_llego"
ESTADOS = (ESTADO_FUERA_SERVICIO, ESTADO_BAJA, ESTADO_NO_LLEGO)

# Lo que dice el botón (lenguaje del técnico) y lo que lee logística en el acta.
BOTON = {
    ESTADO_FUERA_SERVICIO: "No se puede usar",
    ESTADO_BAJA: "No tiene arreglo",
    ESTADO_NO_LLEGO: "No llegó",
}
ACTA = {
    ESTADO_FUERA_SERVICIO: "Fuera de servicio",
    ESTADO_BAJA: "Dar de baja",
    ESTADO_NO_LLEGO: "No llegó",
}
INSIGNIA = {
    ESTADO_FUERA_SERVICIO: "FUERA DE SERVICIO",
    ESTADO_BAJA: "DADO DE BAJA",
    ESTADO_NO_LLEGO: "NO LLEGÓ",
}

# Motivos de la lista («qué pasa»). La clave se guarda; el texto es lo que se lee.
MOTIVOS = {
    ESTADO_FUERA_SERVICIO: (
        ("no_enciende", "No enciende o no responde"),
        ("se_traba", "Se traba o no se mueve bien"),
        ("ruido", "Ruido o vibración fuerte"),
        ("pieza_rota", "Pieza rota, suelta o faltante"),
        ("cable", "Cable o piola cortada"),
        ("estructura", "Estructura dañada (fisura, doblada, soldadura)"),
        ("consola", "Consola o pantalla con falla"),
        ("otro", "Otro: lo cuento abajo"),
    ),
    ESTADO_BAJA: (
        ("estructura", "Estructura quebrada o deformada"),
        ("golpe", "Golpe o accidente: daño total"),
        ("electrico", "Se quemó o se mojó (motor o electrónica)"),
        ("oxido", "Óxido o corrosión avanzada"),
        ("no_conviene", "Repararlo cuesta más que reponerlo"),
        ("sin_repuesto", "No hay repuesto para arreglarlo"),
        ("otro", "Otro: lo cuento abajo"),
    ),
}

# «¿A quién le preguntaste?» (No llegó). «Todavía no pregunté» es una respuesta válida: queda escrito.
CONSULTADO = (
    ("bodega", "Bodega"),
    ("logistica", "Logística"),
    ("jefatura", "Mi jefatura"),
    ("responsable", "Quien lo llevó o lo recibió"),
    ("cliente", "El cliente"),
    ("nadie", "Todavía no pregunté"),
)

MIN_DETALLE_FUERA_SERVICIO = 10
MIN_DETALLE_BAJA = 12      # igual que la baja de un equipo de cliente (ot2_api_equipo_dar_baja)
MAX_DETALLE = 1000
MAX_NOMBRE_CONSULTADO = 80


def _norm(s):
    s = unicodedata.normalize("NFD", str(s or ""))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return " ".join(s.lower().split())


def _limpio(s, tope=MAX_DETALLE):
    return " ".join(str(s or "").split())[:tope]


def motivos_de(estado):
    """[(clave, texto)] de la lista de motivos de la acción, o ()."""
    return MOTIVOS.get(estado, ())


def texto_motivo(estado, clave):
    for k, t in motivos_de(estado):
        if k == clave:
            return t
    return ""


def texto_consultado(clave):
    for k, t in CONSULTADO:
        if k == clave:
            return t
    return ""


def _entero(valor):
    try:
        if isinstance(valor, bool):
            return None
        if isinstance(valor, str):
            valor = valor.strip().replace(",", ".")
            if valor == "":
                return None
        n = float(valor)
    except (TypeError, ValueError):
        return None
    if n != n or n in (float("inf"), float("-inf")) or not float(n).is_integer():
        return None
    return int(n)


def _si_no(valor):
    """True / False / None (no respondió). Acepta «si», «sí», «no», 1, 0, true, false."""
    if isinstance(valor, bool):
        return valor
    n = _norm(valor)
    if n in ("si", "1", "true", "yes"):
        return True
    if n in ("no", "0", "false"):
        return False
    return None


def normalizar(estado, datos, total=1, n_fotos=0):
    """Valida UNA acción y la deja lista para guardar.

    estado : 'fuera_servicio' | 'baja' | 'no_llego'.
    datos  : motivo_clave, detalle, cantidad, consultado_clave, consultado_detalle, riesgo («si» / «no»).
    total  : unidades del producto en el documento (cantidad_doc).
    n_fotos: fotos que llegaron con la acción.
    Devuelve (limpio, None) o (None, (mensaje, codigo)). El mensaje está escrito para el técnico."""
    datos = datos or {}
    if estado not in ESTADOS:
        return None, ("Esa acción no existe.", "ACCION_INVALIDA")
    try:
        total = max(1, int(total or 1))
    except (TypeError, ValueError):
        total = 1
    cantidad = _entero(datos.get("cantidad"))
    if cantidad is None:
        cantidad = total
    if cantidad < 1 or cantidad > total:
        return None, (f"La cantidad va de 1 a {total} (las unidades del documento).", "CANTIDAD_INVALIDA")
    detalle = _limpio(datos.get("detalle"))
    out = {"estado": estado, "motivo_clave": None, "motivo": None, "detalle": detalle or None,
           "cantidad": cantidad, "cantidad_total": total, "consultado": None, "riesgo_lesion": 0}

    if estado == ESTADO_NO_LLEGO:
        clave = str(datos.get("consultado_clave") or "").strip()
        if clave not in {k for k, _ in CONSULTADO}:
            return None, ("Cuéntanos a quién le preguntaste por este producto. Si todavía no preguntaste, elige "
                          "«Todavía no pregunté».", "CONSULTADO_REQUERIDO")
        quien = texto_consultado(clave)
        nombre = _limpio(datos.get("consultado_detalle"), MAX_NOMBRE_CONSULTADO)
        if nombre and clave != "nadie":
            quien = f"{quien} ({nombre})"
        out["consultado"] = quien
        return out, None

    clave = str(datos.get("motivo_clave") or "").strip()
    if clave not in {k for k, _ in motivos_de(estado)}:
        return None, ("Elige qué pasa en la lista." if estado == ESTADO_FUERA_SERVICIO
                      else "Elige el motivo en la lista.", "MOTIVO_REQUERIDO")
    out["motivo_clave"] = clave
    out["motivo"] = texto_motivo(estado, clave)

    if estado == ESTADO_FUERA_SERVICIO:
        if len(detalle) < MIN_DETALLE_FUERA_SERVICIO:
            return None, (f"Cuenta con tus palabras qué pasa (mínimo {MIN_DETALLE_FUERA_SERVICIO} caracteres): es lo "
                          "que lee quien decide qué hacer.", "DETALLE_REQUERIDO")
        riesgo = _si_no(datos.get("riesgo"))
        if riesgo is None:
            return None, ("Responde si el producto puede lastimar a alguien: es una pregunta de seguridad.",
                          "RIESGO_REQUERIDO")
        out["riesgo_lesion"] = 1 if riesgo else 0
        if int(n_fotos or 0) < 1:
            return None, ("Sube al menos una foto de lo que pasa: sin foto no hay aviso.", "FOTO_REQUERIDA")
        return out, None

    # baja
    if len(detalle) < MIN_DETALLE_BAJA:
        return None, (f"Escribe el diagnóstico: qué le pasó y por qué no se puede recuperar (mínimo "
                      f"{MIN_DETALLE_BAJA} caracteres).", "DIAGNOSTICO_REQUERIDO")
    if int(n_fotos or 0) < 1:
        return None, ("Dar de baja exige al menos una foto del producto como quedó.", "FOTO_REQUERIDA")
    return out, None


def _activa(fila):
    if not fila:
        return False
    a = fila.get("activo", 1)
    return str(a) in ("1", "True", "true") if a is not None else True


def aplica_a_todo(fila):
    """La acción cubre TODAS las unidades del producto (o no se sabe cuántas: se asume que sí)."""
    try:
        c, t = fila.get("cantidad"), fila.get("cantidad_total")
        if c is None or t is None:
            return True
        return int(c) >= int(t)
    except (TypeError, ValueError):
        return True


def dispensa_checklist(fila):
    """True si el producto deja de exigir su checklist para firmar y cerrar: acción vigente que cubre todas sus
    unidades. Una acción parcial («no llegaron 2 de 6») deja el checklist de las unidades que sí están."""
    return bool(fila) and _activa(fila) and fila.get("estado") in ESTADOS and aplica_a_todo(fila)


def mapa_dispensados(filas):
    """{item_manual_id: fila vigente} solo de los productos cuyo checklist se dispensa. `filas` pueden venir en
    cualquier orden: manda la más reciente (id más alto) de cada producto."""
    por_producto = {}
    for f in filas or []:
        try:
            im = int(f.get("item_manual_id"))
        except (TypeError, ValueError):
            continue
        if not _activa(f):
            continue
        previa = por_producto.get(im)
        if previa is None or int(f.get("id") or 0) >= int(previa.get("id") or 0):
            por_producto[im] = f
    return {im: f for im, f in por_producto.items() if dispensa_checklist(f)}


def resultado_acta(fila):
    """Resultado del producto para el acta de recepción (Apto, No llegó…), o None si no hay acción vigente que
    cubra todo el producto (entonces manda el «Resultado del control» de la plantilla, si lo trae)."""
    if not dispensa_checklist(fila):
        return None
    return ACTA.get(fila.get("estado"))


def nota_parcial(fila):
    """Texto para el acta cuando la acción cubre solo una parte: «No llegaron 2 de 6». '' si cubre todo."""
    if not _activa(fila) or fila.get("estado") not in ESTADOS or aplica_a_todo(fila):
        return ""
    c, t = int(fila.get("cantidad")), int(fila.get("cantidad_total"))
    if fila["estado"] == ESTADO_NO_LLEGO:
        return f"No llegó {c} de {t}" if c == 1 else f"No llegaron {c} de {t}"
    if fila["estado"] == ESTADO_BAJA:
        return f"Dar de baja: {c} de {t}"
    return f"Fuera de servicio: {c} de {t}"


def resumen_linea(fila):
    """Una línea para el aviso de la tarjeta, el acta y la bitácora (sin hora: la pone app.py en hora Chile)."""
    if not fila or fila.get("estado") not in ESTADOS:
        return ""
    e = fila["estado"]
    partes = [INSIGNIA[e]]
    if fila.get("motivo"):
        partes.append(str(fila["motivo"]))
    if e == ESTADO_NO_LLEGO and fila.get("consultado"):
        partes.append("preguntó a: " + str(fila["consultado"]))
    if e == ESTADO_FUERA_SERVICIO and str(fila.get("riesgo_lesion") or 0) in ("1", "True", "true"):
        partes.append("PUEDE LASTIMAR A ALGUIEN")
    nota = nota_parcial(fila)
    if nota:
        partes.append(nota)
    return " · ".join(partes)


# ───────────────────────── seguimiento de un pedido de repuesto ─────────────────────────
# Las mismas palabras del prototipo aprobado por Daniel (20-oct-2026): «Tu pedido · Revisión de bodega · Pieza lista en bodega ·
# Pieza instalada». static/ot_acciones.js dibuja el seguimiento con estas frases.
PASOS_PEDIDO = (
    ("enviado", "Tu pedido"),
    ("bodega", "Revisión de bodega"),
    ("lista", "Pieza lista en bodega"),
    ("instalada", "Pieza instalada"),
)
# Estado de la solicitud (mant_ot_repuesto_solicitudes.estado) → paso en el que está AHORA (2 a 5; 5 = todo hecho). «Tu pedido» ya
# está hecho en cuanto se envía: lo siguiente es que bodega lo revise. «pedido» es una gestión de compra de gestión: el técnico solo
# ve que la pieza todavía no está lista (REGLA #19: nunca proveedor ni N° de OC).
_PASO_DE_ESTADO = {"solicitado": 2, "validado": 3, "pedido": 3, "recibido": 4, "instalado": 5}


def seguimiento_pedido(estado):
    """{'paso': 1..4 | 0, 'rechazado': bool, 'pasos': [{'clave','texto','estado': hecho|actual|pendiente}]}.
    Un pedido rechazado no avanza: el técnico lo ve como «No se pedirá» (bodega dejó la nota)."""
    e = str(estado or "").strip().lower()
    rechazado = e == "rechazado"
    paso = 0 if rechazado else _PASO_DE_ESTADO.get(e, 2)
    pasos = []
    for i, (clave, texto) in enumerate(PASOS_PEDIDO, start=1):
        if rechazado:
            st = "pendiente"
        elif i < paso:
            st = "hecho"
        elif i == paso:
            st = "actual"
        else:
            st = "pendiente"
        pasos.append({"clave": clave, "texto": texto, "estado": st})
    return {"paso": paso, "rechazado": rechazado, "pasos": pasos}


# ───────────────────────── repuestos sugeridos por defecto (REGLA #32) ─────────────────────────
# Por SISTEMA del producto (el mismo que elige el checklist heredado). Son NOMBRES de pieza para pedir de un toque; el
# pedido queda «escrito a mano · por validar» y bodega lo asocia. Jamás proveedor ni costo (REGLA #19 y #26). Si una
# persona llenó «Repuestos sugeridos» en la plantilla del producto, esa lista REEMPLAZA esta (ver sugeridos_para).
# 2026-10-20 (propuesta del técnico senior aprobada por Daniel): se AGREGAN piezas a cada sistema (nada se quita, REGLA #4.2) y
# nacen los sistemas «eliptica» y «remo». Cada nombre de pieza tiene su ficha de medidas en ot_piezas_medidas.py (la prueba
# tests/test_ot_medidas_diagnostico.py falla si una pieza queda sin ficha).
SUGERIDOS_POR_SISTEMA = {
    "selector_peso": ["Piola (cable de acero)", "Pin selector", "Polea con rodamiento", "Buje o rodamiento de pivote",
                      "Pad de asiento, respaldo o rodillo", "Tope de goma del stack", "Varilla guía o buje de guía",
                      "Perilla de ajuste de asiento"],
    "carga_discos": ["Pin de seguro", "Rodamiento o buje de pivote", "Tope de goma", "Cuerno porta discos",
                     "Pad de asiento o respaldo"],
    "poleas": ["Cable de acero (piola)", "Mosquetón", "Agarre (manilla)", "Pin de altura", "Polea con rodamiento"],
    "trotadora": ["Cinta (banda)", "Tabla (deck)", "Motor de tracción", "Consola", "Llave magnética de emergencia",
                  "Rodillo (delantero o trasero)", "Correa del motor", "Placa controladora", "Motor de inclinación"],
    "bicicleta": ["Correa de transmisión", "Pedales (par)", "Biela", "Consola", "Sillín o poste", "Perilla de ajuste"],
    "eliptica": ["Rodillo o rueda", "Brazo o manija", "Pedales (par)", "Correa de transmisión", "Biela", "Consola"],
    "remo": ["Cinta o cuerda de tirón", "Asiento o carro", "Rodillos del carro", "Consola"],
    "banco": ["Pin o perilla de ajuste", "Rodillo apoya piernas", "Nivelador o taco de goma", "Pad de asiento o respaldo"],
    "rack": ["Brazo de seguridad", "Pin de seguridad", "Kit de pernos y tuercas", "J-hook"],
    "cargas_libres": ["Tuerca o tapa de cabezal", "Bandeja del rack", "Goma o taco de apoyo", "Collarín",
                      "Disco o mancuerna faltante"],
}

# Sistemas que NO tienen bloque de checklist propio (REGLA #32: no se toca ninguna plantilla): se reconocen por la clase del
# catálogo (cuando exista) o por el nombre. Solo sirven para sugerir piezas y guiar el diagnóstico; nunca para elegir un checklist.
SISTEMA_DE_CLASE = {
    "eliptica": "eliptica", "elipticas": "eliptica", "eliptica_pro": "eliptica", "elliptical": "eliptica",
    "remo": "remo", "remos": "remo", "remadora": "remo", "rower": "remo", "remo_concept": "remo",
}

# Nombre del bloque «Revisión — …» → sistema. El checklist heredado ya resuelve el bloque de cada SKU (clase del
# catálogo o excepción por producto); de ahí sale el sistema aunque el técnico haya elegido una plantilla «(LU)».
SISTEMA_DE_BLOQUE = {
    "Revisión — Selector de peso": "selector_peso",
    "Revisión — Carga de discos": "carga_discos",
    "Revisión — Poleas ajustables": "poleas",
    "Revisión — Trotadora (cinta y motor)": "trotadora",
    "Revisión — Bicicleta (transmisión y consola)": "bicicleta",
    "Revisión — Bancos y ajustes": "banco",
    "Revisión — Rack y soportes": "rack",
    "Revisión — Cargas libres (por línea)": "cargas_libres",
}

# Último recurso, solo si el producto no tiene clase en el catálogo: se adivina por el nombre y se dice en pantalla.
# «Remo sentado», «remo bajo»… son máquinas de fuerza con stack (no una remadora): el \bremo\b solo vale si no sigue esa palabra.
_RX_SISTEMA_NOMBRE = (
    ("trotadora", re.compile(r"trotad|treadmill|caminadora|cinta de correr")),
    ("eliptica", re.compile(r"el[ií]ptic|elliptical|cross ?trainer")),
    ("remo", re.compile(r"remadora|rowing|\brower\b|concept ?2|\bremo\b(?!\s+(sentado|bajo|alto|con|en|unilateral|horizontal|t\b|plate|selector|de pie))")),
    ("bicicleta", re.compile(r"bicicleta|spinning|\bbike\b")),
    ("banco", re.compile(r"\bbanco\b|\bbench\b")),
    ("rack", re.compile(r"\brack\b|jaula|power cage|squat stand")),
    ("poleas", re.compile(r"dual (cable|pulley)|polea|cross over|crossover")),
    ("cargas_libres", re.compile(r"mancuerna|\bdisco|kettlebell|pesa rusa|barra ol")),
    ("carga_discos", re.compile(r"plate loaded|carga de discos|pendulum|hack squat|booty|glute")),
    ("selector_peso", re.compile(r"selector|selectoriz|stack|pec deck|lat pull|leg (ext|curl)|chest press")),
)


def sistema_de_bloque(nombre_bloque):
    return SISTEMA_DE_BLOQUE.get(str(nombre_bloque or "").strip())


def sistema_por_nombre(nombre_producto):
    n = _norm(nombre_producto)
    if not n:
        return None
    for sistema, rx in _RX_SISTEMA_NOMBRE:
        if rx.search(n):
            return sistema
    return None


def sistema_de_clase(slug):
    """Sistema según la CLASE del catálogo cuando esa clase es la de una elíptica o un remo (hoy el catálogo no las trae: queda
    listo para cuando existan). None si la clase no es de esas."""
    return SISTEMA_DE_CLASE.get(str(slug or "").strip().lower())


def refinar_sistema(sistema, nombre_producto):
    """El bloque de checklist «Bicicleta (transmisión y consola)» sirve a bicicletas Y elípticas: para SUGERIR piezas y guiar el
    diagnóstico se afina por el nombre (una elíptica no lleva el mismo diagnóstico que una bicicleta). No cambia ningún
    checklist. Cualquier otro sistema vuelve igual."""
    if sistema in (None, "bicicleta"):
        afinado = sistema_por_nombre(nombre_producto)
        if afinado in ("eliptica", "remo"):
            return afinado
    return sistema


def sugeridos_por_defecto(sistema):
    return list(SUGERIDOS_POR_SISTEMA.get(sistema or "", ()))


def limpiar_lista(lista, tope=20, largo=80):
    """Nombres de pieza sin vacíos ni repetidos (sin importar tildes ni mayúsculas). Un texto con «$» se descarta:
    aquí nunca va un costo."""
    out, vistos = [], set()
    for x in lista or []:
        if not isinstance(x, str):
            continue
        s = " ".join(x.split()).strip(" ·•-")[:largo].strip()
        if len(s) < 2 or "$" in s:
            continue
        k = _norm(s)
        if k in vistos:
            continue
        vistos.add(k)
        out.append(s)
        if len(out) >= tope:
            break
    return out


def sugeridos_para(listas_de_plantilla, sistema):
    """Lista final de sugeridos de UN producto. La lista que una persona escribió en la plantilla REEMPLAZA a la de
    por defecto (REGLA #32); si ninguna plantilla del producto tiene lista, vale la del sistema. Devuelve
    (lista, origen) con origen 'plantilla' | 'sistema' | 'ninguno'."""
    de_plantilla = limpiar_lista([x for lst in (listas_de_plantilla or []) for x in (lst or [])])
    if de_plantilla:
        return de_plantilla, "plantilla"
    base = sugeridos_por_defecto(sistema)
    return (base, "sistema") if base else ([], "ninguno")


# ───────────────────────── «No está / no llegó» de un EQUIPO de una OT de CLIENTE ─────────────────────────
# 🧰 2026-10-20 (Daniel, con el caso de la OT-2026-00171 de Aurum: Isabel no pudo terminar porque 2 accesorios no llegaron
# por falta de stock: «esas desviaciones que suceden y para las que debemos estar preparados, como que no está el equipo…
# déjalo bien entretenido»). Una entrada en la barra de CADA equipo con tres opciones:
#   · «No está en el lugar»  → razón de siempre 'no_encontrado' (la de «No pude revisarlo»).
#   · «No llegó · falta stock» → razón NUEVA 'falta_stock'. Sin foto: no hay nada que fotografiar.
#   · «Otra razón»           → las razones de siempre (sin acceso, el cliente no permite, en uso, sin energía, requiere
#                              repuesto, reagendar…).
# Por debajo es el MISMO camino de «No pude revisarlo» (mant_visita_equipos.estado_revision='saltado' + razón + observación):
# así el equipo deja de bloquear la firma y el cierre. Además cada una queda en mant_ot_producto_estado (quién, cuándo,
# motivo, foto) con maquina_id, para poder CONSULTAR después qué equipos quedaron pendientes por falta de stock.
#
# 📌 HECHO el 2026-10-20 en «¿Qué pasó?» de la OT (ot_que_paso.py + app.py::_qp_aplicar; NO se construye aquí): los equipos con estado 'falta_stock' son
# la base de «cerrar lo hecho + OT hija pendiente sin fecha, misma factura» y de los avisos a gestión y bodega. El dato ya
# está listo: app.py::_otpe_pendientes_falta_stock(vid=None) devuelve, por equipo, visita, equipo, SKU, cantidad, quién y
# cuándo, y GET /ot/api/<vid>/pendientes-falta-stock lo entrega a gestión.
ESTADO_NO_ESTA = "no_esta"
ESTADO_FALTA_STOCK = "falta_stock"
ESTADO_NO_REVISADO = "no_revisado"
ESTADOS_EQUIPO = (ESTADO_NO_ESTA, ESTADO_FALTA_STOCK, ESTADO_NO_REVISADO)

# Razón que queda en mant_visita_equipos.razon_saltado (la de «No pude revisarlo»). «Otra razón» usa la que se elige.
RAZON_SALTADO_DE = {ESTADO_NO_ESTA: "no_encontrado", ESTADO_FALTA_STOCK: "falta_stock"}

BOTON_EQUIPO = {
    ESTADO_NO_ESTA: "No está en el lugar",
    ESTADO_FALTA_STOCK: "No llegó · falta stock",
    ESTADO_NO_REVISADO: "Otra razón",
}
CHIP_EQUIPO = {
    ESTADO_NO_ESTA: "No está en el lugar",
    ESTADO_FALTA_STOCK: "No llegó · falta stock",
    ESTADO_NO_REVISADO: "No se pudo revisar",
}

MOTIVOS_EQUIPO = {
    ESTADO_NO_ESTA: (
        ("no_estaba", "No está en el lugar donde debería estar"),
        ("cliente_lo_movio", "El cliente lo movió o lo guardó"),
        ("cliente_lo_saco", "El cliente lo sacó del recinto"),
        ("nadie_sabe", "Nadie sabe dónde está"),
        ("otro", "Otro: lo cuento abajo"),
    ),
    ESTADO_FALTA_STOCK: (
        ("sin_stock", "No hay stock en bodega"),
        ("pedido_sin_llegar", "Está pedido y todavía no llega"),
        # 2026-10-20: «Llegó incompleto o dañado» salió de aquí: si llegó, el equipo está y se revisa (lo que falta va por
        # «Pedir repuesto» o «Informar incidencia»); marcarlo como «no llegó» lo sacaba de la firma sin revisarlo.
        ("otro", "Otro: lo cuento abajo"),
    ),
    # Las claves son las razones de «No pude revisarlo» (se guardan tal cual en razon_saltado). «dado_de_baja» no va: la baja
    # de verdad es «No tiene arreglo» (foto + diagnóstico), nunca una razón de «no se pudo revisar».
    ESTADO_NO_REVISADO: (
        ("sin_acceso", "No había acceso al equipo"),
        ("equipo_en_uso", "Estaba en uso por un socio"),
        ("cliente_no_permite", "El cliente no permitió intervenirlo"),
        ("danado_inaccesible", "Dañado o inaccesible"),
        ("sin_energia", "Sin energía"),
        ("requiere_repuesto", "Necesita un repuesto que no tengo"),
        ("reagendar", "Hay que reagendarlo"),
        ("otro", "Otro: lo cuento abajo"),
    ),
}
MIN_DETALLE_EQUIPO = 10


def motivos_equipo(opcion):
    return MOTIVOS_EQUIPO.get(opcion, ())


def normalizar_equipo(opcion, datos, n_fotos=0):
    """Valida UNA salida «No está / no llegó» de un equipo de cliente y la deja lista para guardar.

    opcion : 'no_esta' | 'falta_stock' | 'no_revisado'.
    datos  : motivo_clave, detalle.
    n_fotos: fotos que llegaron. La de «No llegó · falta stock» NO lleva foto (si llega alguna se ignora: devuelve
             'fotos_validas' = 0 para que la ruta no las suba).
    Devuelve (limpio, None) o (None, (mensaje, codigo)). El mensaje está escrito para el técnico."""
    datos = datos or {}
    if opcion not in ESTADOS_EQUIPO:
        return None, ("Esa opción no existe.", "ACCION_INVALIDA")
    clave = str(datos.get("motivo_clave") or "").strip()
    lista = dict(motivos_equipo(opcion))
    if clave not in lista:
        return None, ("Elige el motivo en la lista.", "MOTIVO_REQUERIDO")
    detalle = _limpio(datos.get("detalle"))
    if clave == "otro" and len(detalle) < MIN_DETALLE_EQUIPO:
        return None, (f"Cuéntanos con tus palabras qué pasó (mínimo {MIN_DETALLE_EQUIPO} caracteres): es lo que lee quien "
                      "decide qué hacer.", "DETALLE_REQUERIDO")
    motivo = lista[clave] if clave != "otro" else "Otro motivo"
    razon = RAZON_SALTADO_DE.get(opcion) or ("otro" if clave == "otro" else clave)
    obs = motivo if clave != "otro" else ""
    if detalle:
        obs = (obs + " — " + detalle) if obs else detalle
    return {
        "estado": opcion, "razon_saltado": razon, "motivo_clave": clave, "motivo": motivo,
        "detalle": detalle or None, "observacion": obs[:2000], "cantidad": 1, "cantidad_total": 1,
        "fotos_validas": 0 if opcion == ESTADO_FALTA_STOCK else max(0, int(n_fotos or 0)),
    }, None


def primer_nombre(usuario):
    """«Isabel Rojas» → «Isabel». '' si no hay nombre."""
    partes = str(usuario or "").split()
    return partes[0] if partes else ""


def chip_equipo(fila, cuando=""):
    """Texto del chip de la tarjeta del equipo: «No llegó · falta stock · 20/10 · Isabel». `cuando` ya viene en hora
    Chile (dd/mm) desde app.py (REGLA #6). Una acción que no es de equipo devuelve ''."""
    if not fila or fila.get("estado") not in ESTADOS_EQUIPO:
        return ""
    partes = [CHIP_EQUIPO[fila["estado"]]]
    if fila["estado"] == ESTADO_NO_REVISADO and fila.get("motivo"):
        partes.append(str(fila["motivo"]))
    if cuando:
        partes.append(cuando)
    quien = primer_nombre(fila.get("creado_por"))
    if quien:
        partes.append(quien)
    return " · ".join(partes)
