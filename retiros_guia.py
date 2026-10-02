# -*- coding: utf-8 -*-
"""Guía de 6 pasos de la ficha de un retiro: qué está hecho, qué falta y qué toca ahora.

Funciones PURAS (sin BD, sin Flask, sin red): pickups_module.py lee los datos y los entrega aquí;
aquí solo se decide. Se prueban sin conexión en tests/test_retiros_guia.py.

Daniel 2026-10-02: "que los pasos estén enumerados, los objetos bordeados en rojo, y que te diga
exactamente qué hace falta si vas a un paso y no has terminado el anterior. Algo bien intuitivo".

Los 6 pasos (los 5 que dictó Daniel + el cierre que ya existía):
    1 Facturas      ¿Son las facturas/boletas que el cliente viene a retirar?     (marca «Confirmo»)
    2 Responsable   ¿Quién se hace cargo de este retiro?                          (automático, «Me hago cargo»)
    3 Productos     ¿Son los productos y cantidades que se lleva?                 (marca «Confirmo»)
    4 Agenda        ¿Qué día y a qué hora viene el cliente?                       (propuesta → cita confirmada)
    5 Preparación   ¿Ya está listo el pedido en bodega?                           (Check lo detecta solo)
    6 Entrega       ¿Ya se lo llevó el cliente?                                   (Marcar como RETIRADO)

Estados de un paso:
    hecho      verde    terminado
    actual     ROJO     lo tiene que hacer una persona ahora
    espera     ámbar    se espera a otro (el cliente, bodega/Check)
    pendiente  gris     todavía no toca; dice qué falta antes
    bloqueado  gris     no se puede hasta resolver algo (con el motivo)

Los retiros que YA venían avanzados (con propuesta, cita o estado posterior) dan por confirmados
los pasos 1 y 3: la marca «Confirmo» es nueva y no se le pide nada retroactivo a lo que ya está en curso.
"""
import hashlib

TERMINALES = ("rechazada", "fallida", "retirada", "cerrada")

PASOS = (
    (1, "facturas", "Facturas", "¿Son las facturas o boletas que el cliente viene a retirar?", "#paso-2"),
    (2, "responsable", "Responsable", "¿Quién se hace cargo de este retiro?", "#paso-resp"),
    (3, "productos", "Productos", "¿Son los productos y cantidades que se lleva?", "#paso-3"),
    (4, "agenda", "Agenda", "¿Qué día y a qué hora viene el cliente?", "#paso-4"),
    (5, "preparacion", "Preparación", "¿Ya está listo el pedido en bodega?", "#paso-confirmacion"),
    (6, "entrega", "Entrega", "¿Ya se llevó el pedido el cliente?", "#paso-confirmacion"),
)


def firma(items):
    """Huella corta y estable de una lista de cosas (documentos, productos). Si cambia la lista,
    cambia la huella y la confirmación anterior deja de valer."""
    base = "|".join(sorted(str(i) for i in (items or [])))
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]


def _paso(n, **kw):
    _n, clave, titulo, pregunta, ancla = PASOS[n - 1]
    p = {"n": n, "clave": clave, "titulo": titulo, "pregunta": pregunta, "ancla": ancla,
         "estado": "pendiente", "faltan": [], "avisos": [], "resumen": "", "accion": None,
         "correo": False, "bloquea": False, "secundario": False}
    p.update(kw)
    return p


def evaluar(c):
    """`c` (dict) → {'pasos': [6 dicts], 'siguiente': n|None, 'terminal': str|None, 'previos': {n: [...]}}.

    Claves de `c` (todas opcionales, con valores neutros):
      status, n_docs, docs (lista de {'rotulo','con_saldo','otro_rut'}), docs_firma, docs_conf (firma
      confirmada o None), docs_conf_quien, prod_n, prod_firma, prod_conf, prod_conf_quien,
      adelantado, responsable (nombre), correo_ok, propuesta (bool), cita (bool), cambio_pedido,
      preparado, picking_total, picking_hechos, check_listo (None si no se sabe)."""
    st = c.get("status") or ""
    n_docs = int(c.get("n_docs") or 0)
    adelantado = bool(c.get("adelantado"))
    terminal = {"rechazada": "El cliente rechazó este retiro.", "fallida": "Este retiro no se concretó."}.get(st)
    # «cerrada» también se usa para cerrar duplicados o spam que nunca se retiraron: solo cuenta como
    # retiro completado si hay evidencia (quién retiró o el paso por «retirada»).
    if st == "cerrada" and not c.get("evidencia_retiro", True):
        terminal = "Este retiro se cerró sin que el cliente lo retirara."
    retirado = st == "retirada" or (st == "cerrada" and not terminal)
    cita = bool(c.get("cita")) or st in ("en_preparacion", "retirada") or (st == "cerrada" and not terminal)
    cambio = bool(c.get("cambio_pedido"))
    preparado = bool(c.get("preparado"))

    # ── 1 · Facturas ─────────────────────────────────────────────────────────
    docs_ok = n_docs > 0 and (adelantado or (c.get("docs_conf") and c.get("docs_conf") == c.get("docs_firma")))
    p1 = _paso(1)
    if n_docs == 0:
        p1.update(estado="actual", faltan=["Agregar la factura o boleta que el cliente va a retirar."],
                  accion={"tipo": "ir", "texto": "Agregar factura o boleta"})
    elif docs_ok:
        quien = c.get("docs_conf_quien")
        con_marca = bool(c.get("docs_conf") and c.get("docs_conf") == c.get("docs_firma"))
        p1.update(estado="hecho",
                  resumen=(f"{n_docs} documento{'s' if n_docs != 1 else ''} confirmado{'s' if n_docs != 1 else ''}" + (f" por {quien}" if quien else "")
                           if con_marca else "Ya venía en curso (sin marca «Confirmo»)"))
    else:
        cambio_lista = bool(c.get("docs_conf"))
        p1.update(estado="actual",
                  faltan=(["La lista de documentos cambió desde que la confirmaste. Revísala y confírmala de nuevo."]
                          if cambio_lista else
                          ["Revisar que el número y el RUT de la factura o boleta sean los correctos y confirmarla."]),
                  accion={"tipo": "confirmar_docs", "texto": "Confirmo estas facturas"})
    for d in (c.get("docs") or []):
        if d.get("con_saldo") == 0:
            p1["avisos"].append(f"{d.get('rotulo')}: sin saldo (ya se entregó o no hay stock). Solo se entrega lo que tenga saldo.")
        if d.get("otro_rut"):
            p1["avisos"].append(f"{d.get('rotulo')}: es de otro RUT (se registró el motivo).")

    # ── 2 · Responsable ──────────────────────────────────────────────────────
    resp = (c.get("responsable") or "").strip()
    p2 = _paso(2)
    if resp:
        p2.update(estado="hecho", resumen=f"A cargo de {resp}")
    elif retirado:
        p2.update(estado="hecho", resumen="Sin responsable registrado")
    elif adelantado or st == "en_preparacion":
        # Retiro ya en curso: faltar el responsable es un AVISO, no el «siguiente paso» (eso es lo operativo).
        p2.update(estado="pendiente", secundario=True,
                  faltan=["Falta asignar responsable. Toca «Me hago cargo» cuando puedas: queda tu nombre, tomado de tu sesión."],
                  accion={"tipo": "tomar", "texto": "Me hago cargo"})
    else:
        p2.update(estado="actual", faltan=["Tocar «Me hago cargo de este retiro»: queda tu nombre, tomado de tu sesión."],
                  accion={"tipo": "tomar", "texto": "Me hago cargo"})

    # ── 3 · Productos ────────────────────────────────────────────────────────
    p3 = _paso(3)
    prod_n = int(c.get("prod_n") or 0)
    prod_ok = n_docs > 0 and (adelantado or (c.get("prod_conf") and c.get("prod_conf") == c.get("prod_firma")))
    if n_docs == 0:
        p3.update(estado="bloqueado", faltan=["Primero agrega la factura o boleta (paso 1): de ahí salen los productos."])
    elif prod_ok:
        quien = c.get("prod_conf_quien")
        con_marca = bool(c.get("prod_conf") and c.get("prod_conf") == c.get("prod_firma"))
        p3.update(estado="hecho",
                  resumen=(f"{prod_n} producto{'s' if prod_n != 1 else ''} confirmado{'s' if prod_n != 1 else ''}" + (f" por {quien}" if quien else "")
                           if con_marca else "Ya venía en curso (sin marca «Confirmo»)"))
    elif prod_n == 0:
        p3.update(estado="actual", faltan=["Los documentos no traen productos para retirar. Revisa las facturas (paso 1)."],
                  accion={"tipo": "ir", "texto": "Revisar productos"})
    elif not docs_ok:
        p3.update(estado="pendiente", faltan=["Antes confirma las facturas (paso 1)."],
                  accion={"tipo": "confirmar_productos", "texto": "Confirmo estos productos", "deshabilitada": True})
    else:
        cambio_prod = bool(c.get("prod_conf"))
        p3.update(estado="actual",
                  faltan=(["Los productos o cantidades cambiaron desde que los confirmaste. Revísalos y confírmalos de nuevo."]
                          if cambio_prod else
                          ["Revisar productos y cantidades (nada en rojo sin explicar) y confirmarlos."]),
                  accion={"tipo": "confirmar_productos", "texto": "Confirmo estos productos"})

    # ── 4 · Agenda ───────────────────────────────────────────────────────────
    p4 = _paso(4)
    previos_4 = _faltan_de([p1, p2, p3])
    if (cita or c.get("propuesta")) and cambio and st not in ("en_preparacion", "retirada", "cerrada"):
        p4.update(estado="actual", correo=True, ancla="#paso-esperando",
                  faltan=["El cliente pidió otra fecha. Respóndele: acepta su fecha o propón otra."],
                  accion={"tipo": "ir", "texto": "Responder al cliente"})
    elif cita:
        p4.update(estado="hecho", resumen="Cita confirmada")
    elif c.get("propuesta"):
        p4.update(estado="espera", ancla="#paso-esperando",
                  faltan=["Esperar la respuesta del cliente a la fecha propuesta (o marcar que aceptó por teléfono)."],
                  accion={"tipo": "ir", "texto": "Ver la propuesta"})
    elif n_docs == 0:
        p4.update(estado="bloqueado", bloquea=True, faltan=["Primero agrega la factura o boleta (paso 1)."])
    else:
        p4.update(estado="actual" if not previos_4 else "pendiente", correo=True,
                  faltan=["Elegir un bloque libre en el calendario y proponérselo al cliente (le llega un correo)."],
                  accion={"tipo": "proponer", "texto": "Proponer fecha y hora al cliente"})
        if previos_4:
            p4["avisos"].append("Antes de proponer conviene terminar: " + "; ".join(f"paso {n}" for n, _ in previos_4) + ".")
        if not c.get("correo_ok", True):
            # Aviso blando: la propuesta también sale por los otros correos del retiro, por SMS y WhatsApp.
            p4["avisos"].append("No hay un correo válido del cliente: revisa la ficha del retiro antes de proponer.")

    # ── 5 · Preparación ──────────────────────────────────────────────────────
    p5 = _paso(5)
    if retirado:
        p5.update(estado="hecho", resumen="Pedido preparado")
    elif st == "en_preparacion":
        if preparado:
            p5.update(estado="hecho", resumen="Pedido listo para entregar")
        else:
            tot, hec = int(c.get("picking_total") or 0), int(c.get("picking_hechos") or 0)
            p5.update(estado="espera",
                      faltan=["Bodega junta el pedido. Check lo detecta solo; también puedes marcarlo a mano en la lista."
                              + (f" Van {hec} de {tot}." if tot else "")])
    elif (cita or c.get("propuesta")) and cambio:
        p5.update(estado="bloqueado", bloquea=True, faltan=["Primero responde el cambio de fecha del cliente (paso 4)."])
    elif cita:
        p5.update(estado="actual", correo=True,
                  faltan=["Enviar el pedido a preparación: bodega recibe la lista y al cliente le llega un correo."],
                  accion={"tipo": "preparacion", "texto": "Enviar a preparación"})
    else:
        p5.update(estado="pendiente", bloquea=True, faltan=["Falta que el cliente confirme la cita (paso 4)."])

    # ── 6 · Entrega ──────────────────────────────────────────────────────────
    p6 = _paso(6)
    if retirado:
        p6.update(estado="hecho", resumen="Retiro completado")
    elif st == "en_preparacion" and preparado:
        p6.update(estado="actual", correo=True,
                  faltan=["Cuando el cliente llegue y se lleve el pedido: «Marcar como RETIRADO» y anotar quién lo retiró."],
                  accion={"tipo": "retirar", "texto": "Marcar como RETIRADO"})
    elif st == "en_preparacion":
        p6.update(estado="pendiente", faltan=["Primero bodega termina de preparar el pedido (paso 5)."],
                  accion={"tipo": "retirar", "texto": "Marcar como RETIRADO"})
    else:
        p6.update(estado="pendiente", bloquea=True, faltan=["Primero hay que enviar el pedido a preparación (paso 5)."])

    pasos = [p1, p2, p3, p4, p5, p6]
    if terminal:
        for p in pasos:
            if p["estado"] != "hecho":
                p.update(estado="pendiente", faltan=[], accion=None, avisos=[], correo=False)
    pend = [p for p in pasos if p["estado"] != "hecho"]
    principales = [p for p in pend if not p.get("secundario")]
    siguiente = ((principales or pend)[0]["n"] if pend else None) if not terminal else None
    previos = {p["n"]: _faltan_de(pasos[:p["n"] - 1]) for p in pasos}
    return {"pasos": pasos, "siguiente": siguiente, "terminal": terminal, "previos": {str(k): v for k, v in previos.items()}}


def _faltan_de(pasos):
    """[(n, [textos])] de los pasos que aún no están hechos (un paso 'espera' del cliente cuenta)."""
    return [(p["n"], list(p["faltan"])) for p in pasos if p["estado"] != "hecho"]
