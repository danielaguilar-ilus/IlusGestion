# -*- coding: utf-8 -*-
"""🎨 5-nov-2026 — la lista de Cotizaciones usa el formato ILUS de la bandeja de Tickets (Daniel: «no se ve bien, fiel al formato y colores»):
tarjeta con borde fino, semáforo por estado en el borde izquierdo, cliente con su RUT, fecha en dos líneas y estados con punto de color.
Nada se quita (REGLA #4.2): las 8 columnas y todas las acciones siguen.
Correr:  python -m pytest tests/test_cotizaciones_formato_ilus_5nov.py -q -p no:warnings"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(*ruta):
    with open(os.path.join(RAIZ, *ruta), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


HTML = _leer("templates", "tickets", "cotizaciones.html")
CSS = _leer("static", "tickets_cotizaciones.css")


class TestPlantilla(unittest.TestCase):
    def test_la_plantilla_compila(self):
        from jinja2 import Environment
        Environment().parse(HTML)

    def test_la_fila_lleva_su_semaforo_por_estado_y_vencida(self):
        self.assertIn('class="cot-row cot-st-{{ c.estado }}{% if c.vencida %} cot-st-vencida{% endif %}"', HTML)

    def test_cliente_con_rut_formateado_y_fecha_en_dos_lineas(self):
        self.assertIn('<strong class="cot-cli-nom">', HTML)
        self.assertIn("c.rut | rut_fmt", HTML)  # REGLA #7
        self.assertIn('class="cot-fec-d"', HTML)
        self.assertIn("chile_fmt", HTML)  # REGLA #6: siempre hora Chile

    def test_la_tarjeta_de_la_lista_tiene_su_clase(self):
        self.assertIn('class="ilus-card cot-lista-card"', HTML)

    def test_no_se_quito_ninguna_columna_ni_accion(self):
        for col in ("Número", "Cliente", "Estado", "Fecha", "Total", "Documentos ERP", "Ticket", "Acciones"):
            self.assertIn("<th", HTML)
            self.assertRegex(HTML, r"<th[^>]*>%s</th>" % re.escape(col))
        for acc in ("pdf", "enviar", "aprobar", "rechazar", "eliminar", "recotizar", "calculo", "historial",
                    "ticket_nuevo", "ticket_asociar", "ticket_quitar"):
            self.assertIn('data-acc="%s"' % acc, HTML)


class TestCss(unittest.TestCase):
    def test_semaforo_de_cada_estado(self):
        for estado, color in (("draft", "#9ca3af"), ("sent", "#3b82f6"), ("approved", "#16a34a"),
                              ("rejected", "#dc2626")):
            self.assertIn(".cot-lista .cot-st-%s{--cot-st:%s}" % (estado, color), CSS)
        self.assertIn(".cot-lista .cot-st-expired,.cot-lista .cot-st-vencida{--cot-st:#f59e0b}", CSS)

    def test_borde_izquierdo_en_escritorio_y_en_tarjeta(self):
        self.assertIn(".cot-lista tbody tr.cot-row > td:first-child{border-left:4px solid var(--cot-st,#d1d5db)}", CSS)
        self.assertIn("tr.cot-row{border-left:5px solid var(--cot-st,#d1d5db)}", CSS)

    def test_letra_minima_12px(self):
        # REGLA #61: nada del bloque nuevo baja de .75rem (12 px).
        bloque = CSS[CSS.index("FORMATO ILUS DE LA LISTA"):]
        for tam in re.findall(r"font-size:\s*([0-9.]+)rem", bloque):
            self.assertGreaterEqual(float(tam), 0.75, tam)

    def test_los_anchos_de_la_regla_61_siguen(self):
        self.assertIn('.cot-lista td[data-label="Acciones"]{width:184px;min-width:184px;text-align:center}', CSS)
        self.assertIn("@media (max-width:1239.98px){", CSS)


if __name__ == "__main__":
    unittest.main()
