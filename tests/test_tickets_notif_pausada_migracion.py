"""
TEST FILE -- NO ES PRODUCCION. NO IMPORTAR DESDE app.py.

Pausa de notificaciones al cliente en tickets migrados de Triple A
(2026-08-20).

PEDIDO DE DANIEL (voz a texto, textual): "la migracion sigue diciendo mi
correo y no trae el del cliente... detener el primer correo automatico
cuando se asigna un ticket, para evitar duplicidad... que esto empiece a
comunicarse hasta que se asigne".

EL RIESGO REAL que esto evita: los 749 tickets importados tenian el correo
de PRUEBA de Daniel en `email` a proposito, para que ningun envio se le
fuera a un cliente real durante la migracion. Corregir eso (poner el correo
real) sin nada mas habria dejado el sistema listo para mandarle un correo de
verdad a un cliente real la primera vez que alguien tocara el `estado` de
cualquiera de esos 749 tickets -- y encima duplicado, porque el sistema de
Triple A sigue vivo y atendiendo esos mismos clientes durante la marcha
blanca.

LA SOLUCION: notif_pausada nace en 1 para todo ticket importado.
_tk_notificar_lifecycle (el que le manda correos al CLIENTE por cambios de
estado) respeta esa pausa y no envia nada mientras este activa -- pero SI
deja registro en Actividad, para que no se pierda que "aca correspondia
avisar". La pausa se levanta SOLA la primera vez que alguien asigna el
ticket dentro de ILUS (no cuando llega ya asignado desde el CSV -- eso es
dato historico de Triple A, no una accion tomada en ILUS).

QUE VERIFICA:
 1. Los 749 tickets YA importados no van a disparar un correo de sorpresa
    a un cliente real la proxima vez que alguien cambie su estado.
 2. La pausa se levanta automaticamente al asignar por primera vez DENTRO
    de ILUS (el evento real disponible en el codigo), no se queda pausado
    para siempre.
 3. Los tickets NUEVOS (no migrados) no se ven afectados -- notif_pausada
    nace en 0 para ellos, comportamiento identico al de antes de este
    cambio.
 4. El correo real del cliente SI se restaura y queda visible (no solo en
    notas_internas como texto suelto).
 5. Que el codigo real tiene las piezas -- no solo la logica aislada de
    este test.

Se corre igual que el resto de la bateria:
    python3 tests/test_tickets_notif_pausada_migracion.py
"""

import re
import sys

fallos = []


def check(cond, msg):
    if cond:
        print(f"  OK   {msg}")
    else:
        print(f"  FALLA {msg}")
        fallos.append(msg)


# ══════════════════════════════════════════════════════════════════
# Simulacion de la tabla tk_tickets -- suficiente para probar la LOGICA
# de _tk_notificar_lifecycle y del gate de asignacion sin una BD real.
# ══════════════════════════════════════════════════════════════════
class TicketsFalsos:
    def __init__(self):
        self.filas = {}
        self.enviados = []   # (tid, estado_slug) que SI mandaron correo
        self.logs = []       # (tid, tipo, detalle) de _tk_log

    def crear(self, tid, email, notif_pausada, asignado_a=None):
        self.filas[tid] = {
            "id": tid, "numero_ticket": f"TAA-{tid}", "email": email,
            "empresa": "Cliente SPA", "nombre_contacto": "Cliente de Prueba",
            "notif_pausada": notif_pausada, "asignado_a": asignado_a,
        }

    def notificar_lifecycle(self, tid, estado_slug):
        """Replica la logica real de _tk_notificar_lifecycle."""
        t = self.filas.get(tid)
        if not t or not (t.get("email") or "").strip():
            return
        if int(t.get("notif_pausada") or 0) == 1:
            self.logs.append((tid, "notif_pausada", f"Aviso de '{estado_slug}' NO enviado"))
            return
        self.enviados.append((tid, estado_slug))

    def asignar(self, tid, nuevo_asignado, actor="ejecutivo1"):
        """Replica el fragmento real del PATCH: solo reanuda si pasaba de
        SIN asignado a CON asignado (primera asignacion real dentro de ILUS)."""
        t = self.filas[tid]
        prev_asignado = t.get("asignado_a")
        if (nuevo_asignado or "").strip() and not (prev_asignado or "").strip():
            if int(t.get("notif_pausada") or 0) == 1:
                t["notif_pausada"] = 0
        t["asignado_a"] = nuevo_asignado


print("\n1. Un ticket migrado NO le manda correo de sorpresa a un cliente real")
db = TicketsFalsos()
db.crear(101, "clientereal@gmail.com", notif_pausada=1, asignado_a="Juan Pablo Martinez")
# "asignado_a" viene poblado del CSV (dato historico de Triple A) pero la
# pausa sigue en 1 porque eso NO es una asignacion hecha dentro de ILUS.
db.notificar_lifecycle(101, "resuelto")
check((101, "resuelto") not in db.enviados,
      "ticket migrado con notif_pausada=1 no envia aunque el CSV ya trajera un ejecutivo asignado")
check(any(l[0] == 101 and l[1] == "notif_pausada" for l in db.logs),
      "queda registrado en Actividad que el aviso se salto por la pausa")

print("\n2. La pausa se levanta SOLA al asignar por primera vez DENTRO de ILUS")
db2 = TicketsFalsos()
db2.crear(102, "clientereal@gmail.com", notif_pausada=1, asignado_a=None)
db2.asignar(102, "Alison Mellado")   # accion real tomada en ILUS
db2.notificar_lifecycle(102, "resuelto")
check((102, "resuelto") in db2.enviados,
      "tras asignar dentro de ILUS, el siguiente cambio de estado SI notifica al cliente")

print("\n3. Reasignar un ticket YA asignado no reactiva nada (no es 'primera vez')")
db3 = TicketsFalsos()
db3.crear(103, "clientereal@gmail.com", notif_pausada=0, asignado_a="Alison Mellado")
db3.asignar(103, "Jaizer Araujo")   # reasignacion, no primera asignacion
check(db3.filas[103]["notif_pausada"] == 0,
      "reasignar un ticket que ya tenia dueño no toca notif_pausada (ya estaba en 0, sigue en 0)")

print("\n4. Los tickets NUEVOS (no migrados) funcionan exactamente igual que antes")
db4 = TicketsFalsos()
db4.crear(104, "clientenuevo@gmail.com", notif_pausada=0)  # default de un ticket nuevo
db4.notificar_lifecycle(104, "creacion")
check((104, "creacion") in db4.enviados,
      "un ticket nuevo (notif_pausada=0 por default) notifica normal, sin cambios de comportamiento")

print("\n5. El codigo REAL tiene las piezas (no solo esta simulacion)")
with open("tickets_module.py", encoding="utf-8") as f:
    fuente = f.read()
check("ADD COLUMN email_cliente_real VARCHAR(190) NULL" in fuente,
      "existe la columna email_cliente_real (correo real, para mostrar)")
check("ADD COLUMN notif_pausada TINYINT(1) NOT NULL DEFAULT 0" in fuente,
      "existe notif_pausada, con DEFAULT 0 -- los tickets NUEVOS no se ven afectados")
check("_ensure_tk_taa_email_backfill" in fuente,
      "existe el backfill de una sola vez para los 749 tickets ya importados antes del fix")
check("WHERE legacy_taa_id IS NOT NULL AND email_cliente_real IS NULL" in fuente,
      "el backfill es idempotente por condicion (no re-toca filas ya arregladas)")
check('if int(t.get("notif_pausada") or 0) == 1:' in fuente,
      "_tk_notificar_lifecycle respeta la pausa")
check('mysql_execute(\n                        "UPDATE tk_tickets SET notif_pausada=0 WHERE id=%s AND notif_pausada=1"'.replace("\n                        ", " ") in fuente.replace("\n                        ", " "),
      "el PATCH de asignacion reanuda la pausa")
check("email_orig or None, email_orig or None," in fuente,
      "el importador (para futuras corridas) ya guarda el correo real directo, no el de pruebas")
check('"Email original del cliente:\\\\s*([^\\\\s|]+@[^\\\\s|]+)"' in fuente or
      "Email original del cliente:" in fuente,
      "el backfill sabe extraer el correo real desde las notas de los 749 ya importados")

print("\n" + "=" * 60)
if fallos:
    print(f"RESULTADO: {len(fallos)} verificacion(es) FALLARON")
    for f_ in fallos:
        print(f"  - {f_}")
    sys.exit(1)
print("RESULTADO: todas las verificaciones pasaron")
sys.exit(0)
