import ast
import pathlib
import re
import unittest
import unicodedata


def load_address_helper():
    source = pathlib.Path("app.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_tr_label_display_address"
    )
    namespace = {"re": re, "unicodedata": unicodedata}
    module = ast.Module(body=[function], type_ignores=[])
    exec(compile(module, "app.py", "exec"), namespace)
    return namespace["_tr_label_display_address"]


class TransportLabelAddressTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.clean = staticmethod(load_address_helper())

    def test_removes_postal_code_and_repeated_location(self):
        self.assertEqual(
            "Colón 1265",
            self.clean(
                "Colón 1265, 8380483 Independencia, Región Metropolitana",
                "8380483",
                "Independencia",
                "Región Metropolitana",
            ),
        )

    def test_keeps_full_street_detail(self):
        self.assertEqual(
            "Avenida Presidente Jorge Alessandri 1847, Edificio B, Portón Norte",
            self.clean(
                "Avenida Presidente Jorge Alessandri 1847, Edificio B, "
                "Portón Norte, San Pedro de la Paz, Región del Biobío, Chile",
                "4130000",
                "San Pedro de la Paz",
                "Región del Biobío",
            ),
        )

    def test_empty_address_stays_empty(self):
        self.assertEqual("", self.clean("", "8380483", "Independencia", "RM"))

    def test_does_not_remove_unconfirmed_seven_digit_number(self):
        self.assertEqual(
            "Ruta Industrial 7654321",
            self.clean("Ruta Industrial 7654321", "", "", ""),
        )

    def test_removes_legacy_postal_only_when_followed_by_commune(self):
        self.assertEqual(
            "Colón 1265",
            self.clean(
                "Colón 1265, 8380483 Independencia, Región Metropolitana",
                "",
                "Independencia",
                "Región Metropolitana",
            ),
        )


if __name__ == "__main__":
    unittest.main()
