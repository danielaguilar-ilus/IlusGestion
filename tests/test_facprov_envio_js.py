# -*- coding: utf-8 -*-
"""📧 Enviar lotes por correo — las funciones PURAS de static/facprov_envio.js, ejecutadas con Node (Daniel, 28-oct-2026). Sin navegador:
lo que se fija es la cuenta de lo marcado, el interruptor «marcar/desmarcar todo» (REGLA #14), la lista de correos y lo que se le pide al servidor
(el asunto y el saludo solo viajan si la persona los tocó). Si Node no está instalado, la prueba se omite."""
import json
import os
import shutil
import subprocess
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JS = os.path.join(RAIZ, "static", "facprov_envio.js")


def _node(expresion):
    exe = shutil.which("node")
    if not exe:
        raise unittest.SkipTest("Node no está instalado")
    prog = "const F=require(%s);process.stdout.write(JSON.stringify(%s));" % (json.dumps(JS.replace("\\", "/")), expresion)
    r = subprocess.run([exe, "-e", prog], capture_output=True, text=True, timeout=60, encoding="utf-8")
    if r.returncode != 0:
        raise AssertionError(r.stderr)
    return json.loads(r.stdout)


class TestFuncionesPuras(unittest.TestCase):
    def test_pesos_chilenos(self):
        self.assertEqual(_node("[F.clp(4750000),F.clp(0),F.clp(null),F.clp(7730000.4),F.clp(-1500)]"),
                         ["$4.750.000", "$0", "$0", "$7.730.000", "-$1.500"])

    def test_lo_marcado_suma(self):
        self.assertEqual(_node("F.resumen([{monto:4750000},{monto:2230000},{monto:750000}])"), {"n": 3, "total": 7730000})
        self.assertEqual(_node("F.resumen([])"), {"n": 0, "total": 0})
        self.assertEqual(_node("F.resumen(null)"), {"n": 0, "total": 0})

    def test_marcar_o_desmarcar_todo_es_un_solo_control(self):
        # si ALGÚN lote visible está sin marcar → marca todos; si ya están todos → desmarca
        self.assertEqual(_node("F.alternarTodo([{marcado:true},{marcado:false}])"), "marcar")
        self.assertEqual(_node("F.alternarTodo([{marcado:false},{marcado:false}])"), "marcar")
        self.assertEqual(_node("F.alternarTodo([{marcado:true},{marcado:true}])"), "desmarcar")

    def test_correos_escritos_a_mano(self):
        self.assertEqual(_node("F.partirCorreos('Nico@SPHS.cl, dani@sphs.cl;  nico@sphs.cl\\nx@y.cl')"),
                         ["nico@sphs.cl", "dani@sphs.cl", "x@y.cl"])
        self.assertEqual(_node("F.agregarCorreo('a@sphs.cl', 'b@sphs.cl')"), "a@sphs.cl, b@sphs.cl")
        self.assertEqual(_node("F.agregarCorreo('a@sphs.cl', 'A@sphs.cl')"), "a@sphs.cl")
        self.assertEqual(_node("F.agregarCorreo('', '')"), "")

    def test_el_pedido_no_manda_asunto_ni_saludo_si_la_persona_no_los_toco(self):
        f = ("{ids:[21,22],para:'a@sphs.cl',cc:'',asunto:'Cierre',saludo:'Hola A,',intro:'i',cierre:'c',conTotal:true,"
             "lineas:[{id:21,etiqueta:'Felcarm',monto_txt:'$1'},{id:22,etiqueta:'Pulgar'}]}")
        p = _node("F.armarPedido(%s,{})" % f)
        self.assertNotIn("asunto", p)
        self.assertNotIn("saludo", p)
        self.assertEqual(p["lineas"], [{"id": 21, "etiqueta": "Felcarm"}, {"id": 22, "etiqueta": "Pulgar"}])   # solo id y etiqueta
        self.assertEqual((p["ids"], p["para"], p["con_total"]), ([21, 22], "a@sphs.cl", True))
        p2 = _node("F.armarPedido(%s,{asunto:true,saludo:true})" % f)
        self.assertEqual((p2["asunto"], p2["saludo"]), ("Cierre", "Hola A,"))


if __name__ == "__main__":
    unittest.main()
