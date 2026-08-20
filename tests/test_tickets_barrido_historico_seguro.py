"""
TEST FILE -- NO ES PRODUCCION. NO IMPORTAR DESDE app.py.

Salvaguardas del barrido HISTORICO de correo (2026-08-20), encontradas por
una revision adversarial (40 agentes, 25 hallazgos confirmados) antes de
correr el barrido real contra los ~279 correos de Triple A en produccion.

Dos defectos, si no se corregian, habrian danado datos reales de clientes:

 1. REAPERTURA EN MASA: el codigo llamaba _tk_reabrir_si_cerrado() para
    CUALQUIER correo ingerido, sin mirar su fecha. Los 749 tickets
    migrados estan mayoritariamente resolved/closed; el barrido historico
    los habria reabierto en bloque, con SLA vencido falso (el reloj usa
    created_at, la fecha real del CSV, que puede ser de hace 6 meses) y
    saltando al tope de la bandeja.

 2. ORDEN AL REVES: `ids = todos[-max_correos:]` toma los correos MAS
    RECIENTES del buzon. Para un barrido que busca rescatar el HISTORICO,
    eso es exactamente lo opuesto: los correos viejos de Triple A quedaban
    sistematicamente fuera de la ventana cada vez que el total superaba
    max_correos.

QUE VERIFICA:
 1. El guardia de antiguedad: un correo reciente SI reabre/sube en la
    bandeja; uno viejo SOLO se ingiere (no reabre, no sube).
 2. oldest_first invierte la seleccion sin romper el comportamiento normal
    (autopoll/cron siguen queriendo los mas recientes).
 3. El codigo real tiene ambas piezas conectadas en el punto correcto.

Se corre igual que el resto de la bateria:
    python3 tests/test_tickets_barrido_historico_seguro.py
"""

import sys
from datetime import datetime, timezone, timedelta

fallos = []


def check(cond, msg):
    if cond:
        print(f"  OK   {msg}")
    else:
        print(f"  FALLA {msg}")
        fallos.append(msg)


_TK_REABRIR_DIAS_MAX = 15  # mismo valor que tickets_module.py


def correo_es_reciente(msg_date):
    """Replica exacta de la condicion usada en _tk_leer_correo_entrante."""
    return (
        msg_date is None
        or (datetime.now(timezone.utc).replace(tzinfo=None) - msg_date)
        <= timedelta(days=_TK_REABRIR_DIAS_MAX)
    )


def seleccionar_ids(todos, max_correos, oldest_first):
    """Replica exacta de la linea `ids = ...` en _tk_leer_correo_entrante."""
    return todos[:max_correos] if oldest_first else todos[-max_correos:]


AHORA = datetime.now(timezone.utc).replace(tzinfo=None)

print("\n1. El guardia de antiguedad decide bien quien reabre y quien no")
CASOS = [
    (AHORA - timedelta(hours=2), True, "correo de hace 2 horas"),
    (AHORA - timedelta(days=1), True, "correo de ayer"),
    (AHORA - timedelta(days=14), True, "correo de hace 14 dias (limite -1)"),
    (AHORA - timedelta(days=16), False, "correo de hace 16 dias (pasa el limite)"),
    (AHORA - timedelta(days=90), False, "correo de hace 3 meses"),
    (AHORA - timedelta(days=187), False,
     "correo de hace 187 dias (el mas viejo real de Triple A)"),
    (None, True, "sin fecha (Date ilegible) -- se trata como reciente, no se pierde"),
]
for fecha, esperado_reciente, etiqueta in CASOS:
    resultado = correo_es_reciente(fecha)
    check(resultado == esperado_reciente,
          f"{etiqueta} -> {'reabre/sube' if esperado_reciente else 'NO reabre/NO sube'} "
          f"(obtenido: {resultado})")

print("\n2. Un correo VIEJO de Triple A NO reabre un ticket cerrado")
# Escenario concreto: el ticket TAA-567 esta 'closed' desde hace 4 meses.
# El barrido historico encuentra el correo original de esa conversacion.
correo_taa_viejo = AHORA - timedelta(days=120)
check(not correo_es_reciente(correo_taa_viejo),
      "un correo de 120 dias de un ticket TAA cerrado no dispara la reapertura")

print("\n3. Una respuesta REAL y reciente de un ticket migrado SI reabre")
# El principio de continuidad (Daniel, 2026-07-18) sigue vivo para el caso
# que fue pensado: alguien responde HOY un ticket migrado que esta cerrado.
correo_reciente = AHORA - timedelta(hours=5)
check(correo_es_reciente(correo_reciente),
      "una respuesta de HOY a un ticket TAA cerrado SI lo reabre (continuidad intacta)")

print("\n4. oldest_first prioriza los correos viejos sin romper el modo normal")
# IMAP SEARCH devuelve ids en orden ASCENDENTE (mas viejo primero).
todos_los_ids = [str(i).encode() for i in range(1, 301)]  # simula 300 correos
# Autopoll/cron (oldest_first=False, comportamiento de siempre): los 50 MAS
# RECIENTES -- el final de la lista ascendente.
normal = seleccionar_ids(todos_los_ids, 50, oldest_first=False)
check(normal == todos_los_ids[-50:],
      "modo normal (autopoll/cron): sigue tomando los 50 mas RECIENTES, sin cambios")
check(normal[0] == b"251" and normal[-1] == b"300",
      "modo normal: rango correcto (251..300, los mas nuevos)")
# Barrido historico (oldest_first=True): los 50 MAS VIEJOS -- el inicio.
historico = seleccionar_ids(todos_los_ids, 50, oldest_first=True)
check(historico == todos_los_ids[:50],
      "barrido historico: toma los 50 mas VIEJOS, no los mas recientes")
check(historico[0] == b"1" and historico[-1] == b"50",
      "barrido historico: rango correcto (1..50, los mas viejos -- los que se "
      "busca rescatar)")

print("\n5. Con TODO el buzon (279 correos) y el limite de la migracion, "
      "oldest_first SI alcanza el historico completo")
buzon_completo = [str(i).encode() for i in range(1, 280)]  # 279 correos
seleccion = seleccionar_ids(buzon_completo, 400, oldest_first=True)
check(len(seleccion) == 279,
      "con max_correos=400 (> 279 correos reales), entran TODOS sin truncar, "
      "sea oldest_first o no")

print("\n6. El codigo REAL tiene ambas salvaguardas conectadas")
with open("tickets_module.py", encoding="utf-8") as f:
    fuente = f.read()
check("_TK_REABRIR_DIAS_MAX" in fuente,
      "existe la constante _TK_REABRIR_DIAS_MAX")
check("_TK_REABRIR_DIAS_MAX = 15" in fuente,
      "el limite es de 15 dias (continuidad real, no backlog historico)")
check("_correo_reciente" in fuente,
      "existe la variable que decide si el correo es reciente")
check("if _correo_reciente:" in fuente and "_tk_reabrir_si_cerrado(ticket[\"id\"]" in fuente,
      "_tk_reabrir_si_cerrado() queda DENTRO del if de antiguedad, no incondicional")
check("updated_at=updated_at WHERE id=%s" in fuente,
      "el correo viejo usa el truco updated_at=updated_at para no subir en la "
      "bandeja (mismo patron que tk_api_marcar_leido)")
check("oldest_first" in fuente,
      "existe el parametro oldest_first")
check('todos[:max_correos] if oldest_first else todos[-max_correos:]' in fuente,
      "la seleccion de ids respeta oldest_first en el codigo real, no solo en "
      "este test")

print("\n" + "=" * 60)
if fallos:
    print(f"RESULTADO: {len(fallos)} verificacion(es) FALLARON")
    for f_ in fallos:
        print(f"  - {f_}")
    sys.exit(1)
print("RESULTADO: todas las verificaciones pasaron")
sys.exit(0)
