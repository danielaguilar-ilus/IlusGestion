# -*- coding: utf-8 -*-
"""🧾 2026-10-18 — Vista Tabla del panel de OT (templates/ot2/panel.html), versión 2.

Daniel (dueño, viendo la tabla en vivo): «necesito que mejores esa tabla. Es muy, muy simple. Tiene información, pero
está como desordenado… no sé por qué no me gusta cómo se ve». Y desde su teléfono: «estoy viendo un check que está
arriba de un anexo».

Pruebas de TEXTO sobre la plantilla (ninguna prueba renderiza el panel: REGLA #9 manda env.parse + navegador):
  · existen las zonas nuevas (semáforo por fila, chip único, recorrido, filtro de agenda, grilla de celular);
  · todo lo de hoy sigue (REGLA #4.2): casilla y maestro, número y prefijo, ticket, anexo, «Sin anexo», factura de
    proveedor, comuna, equipos, documentos, saldo, clasificación, tipo, estado y subtexto, barra, repuestos, agenda y
    relativo, técnico e Int/Ext, flecha, fila de detalle, paginación, orden, KPI, barra de superadmin, filtros;
  · la casilla de celular ya no va en position:absolute (la causa del check encima del anexo);
  · nada de alert/confirm/prompt nativos en lo nuevo (REGLA #1); el archivo conserva CRLF.
Correr con:  python -m pytest tests/test_ot_panel_tabla_v2.py -q -p no:warnings
"""
import os
import re
import unittest

import jinja2

from tests.test_ot_saldo_servicio import RAIZ, _leer

H = _leer("templates/ot2/panel.html")
I_V2 = H.index("TABLA v2 (2026-10-18)")          # desde acá va el CSS nuevo…
CSS_V2 = H[I_V2:H.index("</style>", I_V2)]        # …hasta el cierre de ese <style>
I_TABLA = H.index("{# ══════════════════ TABLA ══════════════════ #}")
I_KANBAN = H.index("{# ══════════════════ KANBAN ══════════════════ #}")
TABLA = H[I_TABLA:I_KANBAN]                      # el bloque HTML de la vista Tabla


class TestParsea(unittest.TestCase):
    def test_jinja_parsea_y_conserva_crlf(self):   # REGLA #9
        jinja2.Environment(extensions=["jinja2.ext.do"]).parse(H)
        with open(os.path.join(RAIZ, "templates", "ot2", "panel.html"), "rb") as fh:
            crudo = fh.read()
        self.assertEqual(crudo.count(b"\n"), crudo.count(b"\r\n"), "el archivo va en CRLF")

    def test_cita_de_daniel_y_fecha(self):
        self.assertIn("2026-10-18", H)
        self.assertIn("Es muy, muy simple", H)
        self.assertIn("está como desordenado", H)


class TestZonasNuevas(unittest.TestCase):
    def test_semaforo_por_fila(self):
        self.assertIn('o2t-s-{{ f.tabla_semaforo or \'gris\' }}', TABLA)
        for c in ("gris", "rojo", "vivo", "ambar", "azul", "verde"):
            self.assertIn("tr.o2t-s-" + c + "{--o2t-sem:", H, c)
        self.assertIn('<span class="o2t-sem" role="img" aria-label="{{ f.tabla_semaforo_txt or \'\' }}"', TABLA)
        # Rojo solo para atraso y ejecución en vivo; la flecha al pasar el mouse ya no es roja.
        self.assertIn("tr.o2t-s-rojo{--o2t-sem:#dc2626} tr.o2t-s-vivo{--o2t-sem:#dc2626}", H)
        self.assertIn(".ot2-tabla tbody tr:hover .ot2-go a{color:#fff;background:#0a0a0a", H)

    def test_chip_unico_y_paleta(self):   # REGLA #2
        self.assertIn(".o2t-chip{display:inline-flex;align-items:center;gap:4px;font-size:.72rem", H)
        for c, bg in (("gris", "#f3f4f6"), ("azul", "#dbeafe"), ("verde", "#dcfce7"), ("ambar", "#fff8e1"), ("rojo", "#fee2e2")):
            self.assertIn(".o2t-chip." + c + "{background:" + bg, H, c)

    def test_recorrido_real_en_estado_y_avance(self):
        for k in ('<td data-label="Estado y avance" class="o2t-eta-td">', '{% set _rec = f.recorrido %}',
                  '<ol class="o2t-rec" aria-label="Recorrido de la OT">', "{{ ' hecho' if h.hecho }}",
                  "{{ ' inferido' if h.inferido }}", "{{ ' actual' if h.actual }}", "{{ ' cancel' if _rec.cancelada }}",
                  '<span class="o2t-rec-l">{{ h.corto }}</span>', "<span class=\"o2t-rec-f\">{{ h.cuando or '' }}</span>",
                  '{{ _rec.texto }}', '{% for _a in _rec.avisos %}'):
            self.assertIn(k, TABLA, k)

    def test_filtro_rapido_de_agenda(self):
        self.assertIn('<nav class="o2t-agenda" aria-label="Filtro rápido de agenda">', TABLA)
        self.assertIn("{% for _ak, _al in agenda_opts %}", TABLA)
        self.assertIn("agenda=(None if f_agenda == _ak else _ak)", TABLA)     # tocar el activo lo quita
        self.assertIn(".o2t-agchip.on{background:#0a0a0a;border-color:#0a0a0a;color:#fff}", H)
        self.assertIn(".o2t-agchip{display:inline-flex;align-items:center;gap:6px;min-height:44px", H)   # REGLA #3
        # El parámetro viaja con TODO: formulario, pie (3), encabezados, KPI (4), pestaña Tabla, nota y Limpiar.
        self.assertIn('{% if f_agenda %}<input type="hidden" name="agenda" value="{{ f_agenda }}">{% endif %}', H)
        self.assertGreaterEqual(H.count("agenda=f_agenda or None"), 9)
        # 🧩 2026-10-20: el filtro por evento se suma a la misma condición (Limpiar, nota de resultados y vacío).
        self.assertIn("{% if q or fase or f_tipo or f_tec or f_interna or f_anexo or f_pago or f_agenda or f_evento %}", H)
        self.assertIn("· agenda <b>{{ agenda_opts | selectattr('0','equalto',f_agenda)", H)
        self.assertIn("+ (1 if f_agenda else 0) + (1 if f_evento else 0) %}", H)
        # 2026-10-19: el Reporte Excel también lleva el filtro de agenda (antes con «Hoy» activo bajaba todo).
        self.assertIn("&interna={{ f_interna or '' }}&agenda={{ f_agenda or '' }}&evento={{ f_evento or '' }}\"", H)
        # En celular la tira avisa que hay más chips a la derecha y conserva el ícono de calendario como título.
        i = H.index("@media (max-width:640px){\n  .o2t-agenda{")
        tira = H[i:H.index("}\n", H.index(".o2t-agenda-t span", i)) + 2]
        self.assertIn("mask-image:linear-gradient(to right,#000 calc(100% - 40px),transparent)", tira)
        self.assertIn("padding:0 40px 4px 0", tira)
        self.assertIn(".o2t-agenda-t span{display:none}", tira)
        self.assertNotIn(".o2t-agenda-t{display:none}", H)
        self.assertIn('<i class="bi bi-calendar2-week" aria-hidden="true"></i><span>Agenda</span>', TABLA)

    def test_recorrido_con_etiquetas_en_escritorio_angosto(self):
        """2026-10-19: entre 881 y 1400px de contenedor (notebooks de 1280 a 1680px con menú lateral) las etiquetas
        de los puntos se escondían. Ahora el recorrido va en dos filas de 3 pasos y nada se oculta."""
        i = H.index("@container o2t (min-width:881px) and (max-width:1400px){")
        angosto = H[i:H.index("\n}\n", i)]
        self.assertIn(".o2t-rec{display:grid;grid-template-columns:repeat(3,minmax(0,1fr))", angosto)
        self.assertIn(".o2t-rec-p:nth-child(4)::before{display:none}", angosto)
        self.assertNotIn("clip:rect(0 0 0 0)", CSS_V2)            # ninguna etiqueta del recorrido se esconde
        self.assertNotIn(".o2t-rec-l{position:absolute", CSS_V2)

    def test_el_subtexto_no_se_repite_con_el_recorrido(self):
        """2026-10-19: «En sitio desde 10:05» salía en el subtexto de la pastilla Y en la etapa del recorrido."""
        self.assertIn("and not (_rec and _rec.texto) %}", TABLA)
        self.assertLess(TABLA.index("{% set _rec = f.recorrido %}"), TABLA.index('<div class="ot2-estsub">{{ f.estado_sub }}</div>'))
        # El anexo es UN chip en «Cliente y trabajo»; la etapa no lo repite como aviso (ver _ot2_recorrido).
        self.assertEqual(TABLA.count(">Sin anexo</span>"), 1)

    def test_celular_la_casilla_tiene_su_columna(self):   # Daniel: «un check que está arriba de un anexo»
        self.assertIn(".ot2-tabla-wrap{container:o2t / inline-size}", H)
        i = H.index("@container o2t (max-width:880px){")
        celular = H[i:H.index("/* La tira de chips de agenda vive fuera del contenedor", i)]
        self.assertIn('grid-template-areas:"num tipo sel" "cli cli cli" "eta eta eta" "ag tec tec"', celular)
        self.assertIn(".ot2-tabla tbody tr.o2t-con-sel{grid-template-columns:minmax(0,1fr) minmax(0,1fr) 44px}", celular)
        self.assertIn(".ot2-tabla td.ot2-seltd{grid-area:sel;position:static;width:44px;height:44px", celular)
        self.assertIn(".ot2-tabla td.ot2-go{display:none}", celular)
        self.assertIn(".ot2-tabla tbody tr.ot2-det{display:none}", celular)
        # 2026-10-19: lo único en position:absolute es la zona de toque (::after) del chip que se abre; ningún control.
        for m in re.finditer(r"([^\n{}]*)\{[^}]*position:absolute", celular):
            self.assertIn("summary::after", m.group(1), m.group(0))
        # Toques de 44px (REGLA #3): el summary (21px de chip) completa su zona con ::after; los chips crecen a ~28px.
        self.assertIn(".o2t-more summary::after{content:\"\";position:absolute;left:0;right:0;top:-9px;bottom:-9px}", celular)
        self.assertIn(".o2t-chips .o2t-chip,.o2t-chips .ot2-sinsaldo,.o2t-chips .ot2-clasif{padding:5px 10px;font-size:.76rem}", celular)
        # La casilla en position:absolute (y el padding-right de la celda vecina) ya no existen en ninguna parte.
        self.assertNotIn("td.ot2-seltd{position:absolute", H)
        self.assertNotIn("td.ot2-seltd + td{padding-right", H)
        # El bloque viejo de tarjetas queda solo como respaldo para navegadores sin container queries.
        self.assertIn("@supports not (container-type:inline-size){\r\n@media (max-width:760px){".replace("\r\n", "\n"), H)
        self.assertIn("{{ ' o2t-con-sel' if puede_lote }}", TABLA)

    def test_nombres_completos_sin_truncar(self):   # REGLA #15
        self.assertIn(".o2t-cli{font-weight:800;font-size:.9rem;color:#0a0a0a;line-height:1.3;overflow-wrap:anywhere}", H)
        self.assertIn('<div class="o2t-cli">{{ f.razon_social or \'Trabajo interno\' }}</div>', TABLA)
        self.assertNotIn("line-clamp", CSS_V2)
        self.assertNotIn("text-overflow:ellipsis", CSS_V2)
        self.assertIn('<span class="nom">{{ f.tecnico_nombre }}</span>', TABLA)

    def test_mas_n_se_abre_dentro_de_la_fila(self):
        self.assertIn('<details class="o2t-more" onclick="event.stopPropagation()">', TABLA)
        self.assertIn("{% for _d in f.docs %}<span class=\"it\">{{ _d }}</span>{% endfor %}", TABLA)
        self.assertIn('<span class="it">Argumento: {{ _fc.argumento }}</span>', TABLA)
        self.assertIn('<span class="it">Autorizó: {{ _fc.autorizado_por }}</span>', TABLA)


class TestNadaSeQuita(unittest.TestCase):
    """REGLA #4.2: cada elemento de la tabla de hoy sigue en la nueva (elemento de hoy → dónde quedó)."""

    def test_seleccion_masiva_y_marcar_todo(self):   # REGLA #14
        for k in ('id="ot2SelAll"', 'id="ot2SelAllMobile"', 'id="ot2SelMobile"', 'class="ot2-sel"',
                  'data-numero=', 'data-estado="{{ f.estado or \'\' }}"', 'data-fase="{{ f.estado_fase or \'cancel\' }}"',
                  '<td class="ot2-seltd" data-label="" onclick="event.stopPropagation()">'):
            self.assertIn(k, TABLA, k)
        # El bloque de celular vive DENTRO del contenedor (mismo id, mismo JS).
        self.assertLess(TABLA.index('<div class="ot2-tabla-wrap">'), TABLA.index('id="ot2SelMobile"'))
        self.assertEqual(H.count("permissions.superadmin or permissions.mant_eliminar"), 3)   # las 3 condiciones iguales

    def test_numero_cliente_y_chips(self):
        for k in ('{{ f.numero_sufijo }}<small>OT-{{ f.numero_prefijo }}</small>', "VS-{{ '%05d'|format(f.id) }}<small>Automática</small>",
                  "{{ f.numero_ticket }}", "Anexo {{ f.anexo_numero }} · {{ 'firmado' if f.anexo_estado == 'firmado' else ('borrador' if f.anexo_estado == 'borrador' else 'por firmar') }}",
                  "{% elif f.tecnico_tipo == 'externo' and f.estado_fase in ('pend','ejec') %}", ">Sin anexo</span>",
                  "{% if f.fac_numero or f.fac_estado %}", "{{ f.fac_numero or 's/n' }}",
                  "{{ f.visita_comuna }}", "{{ f.n_equipos }} equipo{{ 's' if f.n_equipos != 1 }}", "{{ f.docs[0] }}",
                  'class="ot2-sinsaldo {{ _fa.estado }}"', "Saldo insuficiente", "Tuvo factura sin saldo",
                  "{% if f.fin_clasif and f.fin_clasif.texto %}", "{{ _fc.texto }}", "{{ f.titulo }}",
                  "{{ f.prioridad_label }}"):
            self.assertIn(k, TABLA, k)

    def test_tipo_estado_avance_agenda_tecnico_flecha(self):
        for k in ('<i class="bi {{ f.tipo_icon }}"></i><span>{{ f.tipo_label }}</span>',
                  '<span class="ot2-est">', "{{ f.estado_label }}", '<div class="ot2-estsub">{{ f.estado_sub }}</div>',
                  '<span class="bar"><i style="width:{{ f.check_pct }}%"></i></span>', "{{ f.n_completas }}/{{ f.n_tareas }}",
                  "{{ f.rep_abiertos }} repuesto{{ 's' if f.rep_abiertos != 1 }} en gestión", "{{ f.rep_instalar }} a instalar",
                  "{{ f.agenda_dow }}", "{{ f.fecha_dia }}", "→ {{ f.agenda_hasta }}", "{{ f.agenda_horario }}",
                  '<span class="o2t-rel {{ f.agenda_rel_class }}">{{ f.agenda_rel }}</span>',
                  "{{ f.tecnico_ini }}", ">Sin asignar</span>", "{{ 'Ext' if f.tecnico_tipo == 'externo' else 'Int' }}",
                  '<td class="ot2-go">', 'aria-label="Abrir la OT {{ f.numero_ot or \'\' }}"'):
            self.assertIn(k, TABLA, k)

    def test_detalle_oculto_paginacion_orden_y_encabezados(self):
        self.assertIn('<tr class="ot2-det f-{{ f.estado_fase or \'cancel\' }}">', TABLA)
        self.assertIn('<td colspan="{{ 8 if puede_lote else 7 }}">', TABLA)      # 7 columnas (+1 con casilla)
        for k in ("{{ f.salud_label }}", "{{ f.prioridad_label }}", "Mostrando <b>", "per_page_opciones", "Anterior</a>",
                  "Página <b>{{ page }}</b> de {{ total_paginas }}", "Siguiente <i"):
            self.assertIn(k, TABLA, k)
        for col in (("ot", "N° OT"), ("cliente", "Cliente y trabajo"), ("tipo", "Tipo"), ("estado", "Estado y avance"),
                    ("fecha", "Agenda"), ("tecnico", "Técnico")):
            self.assertIn("th('%s','%s'" % col, TABLA, col)
        self.assertIn('<th class="ot2-goth" aria-label="Abrir"></th>', TABLA)
        # El estado vacío ya avisa con cualquier filtro y ofrece Limpiar.
        self.assertIn("<h5>No hay OT con estos filtros</h5>", TABLA)
        self.assertIn("Limpiar filtros", TABLA)

    def test_kpi_filtros_superadmin_y_vistas_siguen(self):
        for k in ('class="ot2-kpi k-pend', 'class="ot2-kpi k-ejec', 'class="ot2-kpi k-comp', 'class="ot2-kpi k-cerr',
                  '<nav class="ot2-sa-bar"', 'name="tipo"', 'name="tecnico"', 'name="interna"', 'name="anexo"', 'name="pago_prov"',
                  "Creadas por personas", "Automáticas", 'id="ot2Bar"', 'class="ot2-filtros-btn"',
                  "vista='kanban'", "vista='calendario'", "{% elif vista == 'kanban' %}", 'id="ot2SelBar"', 'id="ot2ModalLote"'):
            self.assertIn(k, H, k)


class TestReglas(unittest.TestCase):
    def test_sin_alert_confirm_prompt_nativos_en_lo_nuevo(self):   # REGLA #1
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", TABLA))
        self.assertIsNone(re.search(r"(?<![\w.])(alert|confirm|prompt)\(", H[I_V2:I_TABLA]))

    def test_no_se_toca_kanban_calendario_ni_el_modal_de_crear(self):
        kan = H[I_KANBAN:]
        self.assertNotIn("o2t-", kan)
        self.assertTrue(os.path.exists(os.path.join(RAIZ, "templates", "ot2", "_modal_crear.html")))
        self.assertNotIn("o2t-", _leer("templates/ot2/_modal_crear.html"))

    def test_chips_financieros_siguen_dentro_de_su_if(self):   # REGLA #26: el dato se quita en el servidor
        for k in ("{% if f.fac_numero or f.fac_estado %}", "{% if f.fac_alerta %}", "{% if f.fin_clasif and f.fin_clasif.texto %}"):
            self.assertIn(k, TABLA, k)
        # Ningún dato financiero nuevo en la fila: el recorrido y el semáforo son operativos.
        for k in ("costo", "margen", "monto", "precio"):
            self.assertNotIn(k, TABLA.lower().split("{# el detalle que se despliega")[0][TABLA.lower().index("o2t-eta-td"):], k)

    def test_fechas_sin_iso_ni_ingles(self):   # REGLA #6
        self.assertNotIn("isoformat", TABLA)
        self.assertNotIn("strftime", TABLA)


if __name__ == "__main__":
    unittest.main()
