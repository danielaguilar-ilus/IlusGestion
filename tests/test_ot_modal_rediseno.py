# -*- coding: utf-8 -*-
"""2026-10-24 · Daniel: «rediseña el modal también». Rediseño del asistente «Generar OT»: SOLO capa visual (cabecera, pantalla de origen, pie)
+ atajo de teclado 1-5 en el origen. No cambia pasos, campos, permisos ni reglas. Respeta la paleta ILUS, los toques de 44 px y los nombres completos.
"""
import os
import re
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(RAIZ, "templates", "ot2", "_modal_crear.html"), encoding="utf-8") as _fh:
    M = _fh.read()

CSS = M[M.index("🎨 REDISEÑO 2026-10-24"):M.index("</style>\n\n<div class=\"modal fade o2m\" id=\"ot2ModalCrear\"")]


class TestCapaVisual(unittest.TestCase):
    def test_va_al_final_de_los_estilos_y_sin_important(self):
        sin_comentarios = re.sub(r"/\*.*?\*/", "", "/*" + CSS, flags=re.S)       # (el comentario de cabecera lo menciona)
        self.assertNotIn("!important", sin_comentarios)
        self.assertGreater(M.index("🎨 REDISEÑO 2026-10-24"), M.index(".o2m-op.sel-warn .ic"))

    def test_paleta_ilus(self):
        self.assertIn("var(--ilus-red,#dc2626)", CSS)
        for ajeno in ("#6366f1", "#8b5cf6", "#3b82f6", "#10b981", "purple", "indigo"):
            self.assertNotIn(ajeno, CSS)

    def test_origen_tarjetas_con_icono_flecha_y_atajo(self):
        for pista in (".o2m-body.solo .o2m-op .ic{width:54px", ".o2m-body.solo .o2m-op::after{content:'\\f138'", ".o2m-body.solo .o2m-op:nth-child(5)::before{content:'5'}",
                      "@keyframes o2mOpEntra"):
            self.assertIn(pista, CSS)

    def test_movimiento_respeta_prefers_reduced_motion(self):
        i = CSS.index("@media (prefers-reduced-motion:reduce)")
        self.assertIn(".o2m-body.solo .o2m-op{animation:none;transition:none}", CSS[i:i + 400])

    def test_celular_dos_columnas_y_sin_atajo(self):
        i = CSS.index("@media (max-width:575.98px)")
        f = CSS[i:i + 700]
        self.assertIn("grid-template-columns:1fr 1fr", f)
        self.assertIn(".o2m-body.solo .o2m-op::before{display:none}", f)
        self.assertIn("#o2mHint.o2m-hint-atajo{display:none}", f)

    def test_cierre_de_44px(self):
        self.assertIn(".o2m-head .x{width:44px;height:44px", CSS)

    def test_nada_oculta_texto(self):
        # REGLA #15: los nombres y las descripciones se ven completos (ningún recorte con puntos suspensivos en las tarjetas de origen).
        self.assertNotIn("text-overflow:ellipsis", CSS)
        self.assertNotIn("line-clamp", CSS)


class TestAtajoDeTeclado(unittest.TestCase):
    def setUp(self):
        i = M.index("document.addEventListener('keydown', function(ev){\n    if (fase !== 'origen'")
        self.f = M[i:i + 900]

    def test_solo_en_origen_y_con_el_asistente_como_unico_modal(self):
        self.assertIn("if (fase !== 'origen' || ev.ctrlKey || ev.metaKey || ev.altKey) return;", self.f)
        self.assertIn("document.querySelectorAll('.modal.show').length !== 1", self.f)

    def test_no_roba_las_teclas_de_los_campos(self):
        for tag in ("INPUT", "TEXTAREA", "SELECT", "isContentEditable"):
            self.assertIn(tag, self.f)

    def test_elige_la_tarjeta_n(self):
        self.assertIn("setOrigen(ORIGENES[n - 1].v);", self.f)
        self.assertIn("n >= 1 && n <= ORIGENES.length", self.f)

    def test_la_pista_se_pone_en_origen_y_se_quita_en_el_formulario(self):
        self.assertIn("_h.classList.add('o2m-hint-atajo')", M)
        self.assertIn("_hAt.classList.remove('o2m-hint-atajo')", M)


class TestNoSeRompeNada(unittest.TestCase):
    def test_siguen_los_cinco_origenes_y_los_pasos(self):
        for v in ("cliente", "ticket", "cotizacion", "documento", "interno"):
            self.assertIn("{v:'%s'" % v, M)
        self.assertEqual(len(re.findall(r"onclick=\"OT2C\.setOrigen", M)), 1)
        self.assertIn("function pintarOrigen(b){", M)
        self.assertIn('id="o2mSig"', M)
        self.assertIn('id="o2mAtras"', M)


if __name__ == "__main__":
    unittest.main()
