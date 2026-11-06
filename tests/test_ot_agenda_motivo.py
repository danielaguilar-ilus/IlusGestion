# -*- coding: utf-8 -*-
"""El MOTIVO de la OT en el correo de agenda del técnico (Daniel, 6-nov-2026: «sería bueno agregar el motivo a las OT de los técnicos» → «en los correos»).

Cada fila de la agenda dice, en una línea corta, el motivo con que se creó la OT; sin plata ni proveedores (REGLAS #19 y #26) y oculto mientras el Anexo
de Servicios del proveedor no esté firmado. Datos de mentira (REGLA #22). Correr:  python -m pytest tests/test_ot_agenda_motivo.py -q -p no:warnings
"""
import unittest

import ot_correos_tecnicos as O

ITEM = {"id": 171, "numero_ot": "OT-2026-00171", "cliente": "Aurum Fit Girls Spa", "tipo_label": "Instalación", "direccion": "Av. Apoquindo 4500, Las Condes",
        "equipos": ["Trotadora", "Bicicleta"], "hora_inicio": "10:00", "hora_fin": "12:00", "fecha": "2026-11-10",
        "descripcion": "Garantía: revisar la correa de la trotadora y coordinar con el administrador del club."}


class TestMotivoEnLaAgenda(unittest.TestCase):
    def test_la_fila_trae_el_motivo(self):
        html = O.agenda_html([ITEM], "https://x.test")
        self.assertIn("Motivo:", html)
        self.assertIn("Garantía: revisar la correa de la trotadora", html)

    def test_sin_motivo_no_aparece_la_fila(self):
        html = O.agenda_html([dict(ITEM, descripcion="")], "https://x.test")
        self.assertNotIn("Motivo:", html)

    def test_es_una_linea_corta(self):
        largo = dict(ITEM, descripcion="Revisar equipo. " * 40)
        html = O.agenda_html([largo], "https://x.test")
        motivo = html.split("Motivo:</span> ")[1].split("<")[0]
        self.assertLessEqual(len(motivo), 140)

    def test_nunca_lleva_plata_ni_proveedores(self):
        it = dict(ITEM, descripcion="Instalar la trotadora. Cobrar $350.000 al cliente. Proveedor Fit Tech pagará la OC 4455.")
        html = O.agenda_html([it], "https://x.test")
        for prohibido in ("350.000", "Proveedor", "OC 4455"):
            self.assertNotIn(prohibido, html)
        self.assertIn("Instalar la trotadora", html)

    def test_con_el_anexo_sin_firmar_no_se_muestra(self):
        html = O.agenda_html([dict(ITEM, oculta_detalle=True)], "https://x.test")
        self.assertNotIn("Motivo:", html)
        self.assertNotIn("Garantía", html)

    def test_el_motivo_se_escapa(self):
        html = O.agenda_html([dict(ITEM, descripcion="<script>alert(1)</script> revisar")], "https://x.test")
        self.assertNotIn("<script>", html)


if __name__ == "__main__":
    unittest.main()
