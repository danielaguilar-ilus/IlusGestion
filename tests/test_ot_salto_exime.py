"""Un equipo «No pude revisarlo» / «No está» / dado de baja / fuera de servicio NO exige el checklist (2026-10-21).

Daniel: «el botón de no se pudo revisar el equipo… debería eximir de realizar el checklist y liberar para que se pueda cerrar, y
dejar constancia». El servidor ya lo eximía (_ot_maquinas_excluidas_cierre), pero al dibujarse su checklist la pantalla reescribía
data-obl/data-oblok con el conteo crudo (0/8) y el botón «Firmar como técnico» volvía a quedar bloqueado: un ciclo sin salida.

Correr con:  py -m unittest tests.test_ot_salto_exime
"""
import os
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _leer(rel):
    with open(os.path.join(RAIZ, rel), encoding="utf-8") as fh:
        return fh.read().replace("\r\n", "\n")


class TestSaltoExime(unittest.TestCase):
    D = _leer("templates/ot2/detalle.html")
    A = _leer("app.py")

    def test_la_tarjeta_marca_el_equipo_exento(self):
        self.assertIn('''data-excluido="{{ '1' if _excluido_cierre else '' }}"''', self.D)
        self.assertIn("{% if _excluido_cierre %}No exige checklist{% else %}", self.D)
        self.assertIn("Este equipo no exige el checklist.", self.D)

    def test_el_checklist_no_reescribe_el_conteo_de_un_exento(self):
        self.assertIn("const _exento = !!(_itemEx && _itemEx.dataset.excluido === '1');", self.D)
        self.assertIn("item.dataset.oblok = String(_exento ? 1 : oblOk);", self.D)
        self.assertIn("item.dataset.obl = String(_exento ? 1 : oblTot);", self.D)
        self.assertIn("const completo = _exento || (oblTot > 0 && oblOk >= oblTot);", self.D)

    def test_el_servidor_no_lo_cuenta_como_pendiente(self):
        self.assertIn('e["excluido_cierre"] = _rev in ("saltado", "falla_detectada")', self.A)
        self.assertIn('''        if e["excluido_cierre"]:
            e["n_obl_falta"] = 0''', self.A)
        # misma regla que el candado real de la firma y el cierre
        self.assertIn("AND estado_revision IN ('saltado','falla_detectada')", self.A)


if __name__ == "__main__":
    unittest.main()
