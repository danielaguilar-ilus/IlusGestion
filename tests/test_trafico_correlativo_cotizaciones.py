"""
TEST FILE -- NO ES PRODUCCION. NO IMPORTAR DESDE app.py.

Prueba de concurrencia SOLICITADA por Daniel (2026-07-22) para el flujo de
Cotizaciones recien construido: crear el correlativo `numero_cotizacion`
(tk_settings.cotiz_ultimo_correlativo) en paralelo sin que dos cotizaciones
choquen con el mismo numero.

Codigo real espejado (NO copiado 1:1, ver por que abajo):
    tickets_module.py, funcion tk_api_cotizacion_desde_erp, bloque
    aproximado lineas 2859-2960 (2026-07-22).

────────────────────────────────────────────────────────────────────────
POR QUE SQLITE Y NO MYSQL REAL (leer antes de confiar en el resultado)
────────────────────────────────────────────────────────────────────────
Esta maquina tiene un servicio "MySQL81" escuchando en 127.0.0.1:3306,
pero SIN credenciales conocidas (se probaron root/'', root/root,
root/password, root/mysql -- las 4 fallaron con Access denied). NO se
intento forzar/resetear esa contraseña: eso implicaria tocar un servicio
de Windows que no sabemos si pertenece a otro proyecto de Daniel, lo cual
cuenta como "modificar configuracion de sistema" -- fuera de alcance sin
autorizacion explicita. Tampoco hay Docker instalado. Por lo tanto NO
existe en este entorno una base MySQL de prueba disponible (misma
conclusion a la que se llego en una sesion previa documentada en la
memoria "concurrencia_numero_pool_repeatable_read.md").

Esta prueba por lo tanto:
  - SI ejecuta concurrencia REAL (threads reales, base de datos real en
    disco, sin mockear el resultado).
  - NO ejecuta contra MySQL/InnoDB -- usa SQLite como sustituto, con
    `BEGIN IMMEDIATE` como analogo de `SELECT ... FOR UPDATE`. Este
    analogo es MAS conservador que el original (bloquea TODA la base en
    vez de solo la fila), asi que si el ALGORITMO (leer bajo lock,
    incrementar en Python, escribir, commit; rollback ante excepcion)
    tuviera una condicion de carrera real, esta prueba tambien la
    encontraria. Lo que esta prueba NO puede confirmar es una falla
    especifica del motor MySQL (ej. tabla no-InnoDB, o autocommit mal
    configurado) -- eso se verifico por separado, de forma ESTATICA:

      * tk_settings.clave es PRIMARY KEY .......... tickets_module.py:1039
      * tk_settings ENGINE=InnoDB (locks de fila) .. tickets_module.py:1044
      * get_mysql() -> conexion DEDICADA,
        autocommit=False ..................................... app.py:1536-1546
      * tk_cotizaciones.numero_cotizacion UNIQUE
        (defensa en profundidad si el lock fallara) .. tickets_module.py:899
      * En la creacion real: mismo cursor/conexion para
        SELECT..FOR UPDATE + UPDATE + INSERT, commit unico,
        rollback+close en except ................ tickets_module.py:2859-2960

No se commitea este archivo -- queda para que Daniel decida si lo conserva.
"""
import os
import sqlite3
import threading
import time
import traceback
from collections import Counter

DB_PATH = os.path.join(os.path.dirname(__file__), "_tmp_correlativo_test.sqlite3")
N_THREADS = 40
SEED_INICIAL = 177  # mismo numero real que Daniel confirmo para Triple A


def _reset_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE tk_settings (clave TEXT PRIMARY KEY, valor TEXT)")
    conn.execute("CREATE TABLE tk_cotizaciones (id INTEGER PRIMARY KEY AUTOINCREMENT, "
                 "numero_cotizacion TEXT UNIQUE)")
    conn.execute("INSERT INTO tk_settings (clave, valor) VALUES ('cotiz_ultimo_correlativo', ?)",
                 (str(SEED_INICIAL),))
    conn.commit()
    conn.close()


def _crear_cotizacion_worker(resultados, errores, idx, seed_missing_mode=False):
    """Espeja tickets_module.py:2859-2960:
       conn = get_mysql() -> cursor -> SELECT ... FOR UPDATE
       -> (seed si falta fila) -> UPDATE correlativo -> INSERT cotizacion
       -> commit  |  except: rollback -> return error  |  finally: close
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
        conn.execute("BEGIN IMMEDIATE")  # analogo a "FOR UPDATE" (mas estricto)
        cur = conn.cursor()

        cur.execute("SELECT valor FROM tk_settings WHERE clave='cotiz_ultimo_correlativo'")
        fila = cur.fetchone()
        if fila is None:
            # Blindaje espejado de tickets_module.py:2874-2889 (semilla
            # ausente -- se siembra DENTRO de la misma transaccion).
            cur.execute(
                "INSERT OR IGNORE INTO tk_settings (clave, valor) VALUES ('cotiz_ultimo_correlativo', '0')")
            cur.execute("SELECT valor FROM tk_settings WHERE clave='cotiz_ultimo_correlativo'")
            fila = cur.fetchone()

        try:
            ultimo = int((fila or [0])[0] or 0)
        except (TypeError, ValueError):
            ultimo = 0
        correlativo = ultimo + 1
        cur.execute(
            "UPDATE tk_settings SET valor=? WHERE clave='cotiz_ultimo_correlativo'",
            (str(correlativo),))
        numero = f"COT-{correlativo:06d}"
        cur.execute(
            "INSERT INTO tk_cotizaciones (numero_cotizacion) VALUES (?)", (numero,))
        conn.commit()
        resultados[idx] = numero
    except Exception as e:
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        errores[idx] = f"{type(e).__name__}: {e}"
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _run(n_threads, seed_missing=False, label=""):
    _reset_db()
    if seed_missing:
        # Escenario adversarial adicional: la fila del correlativo NO existe
        # (boot no corrio la semilla) -- N threads llegan simultaneamente.
        conn = sqlite3.connect(DB_PATH)
        conn.execute("DELETE FROM tk_settings WHERE clave='cotiz_ultimo_correlativo'")
        conn.commit()
        conn.close()

    resultados = [None] * n_threads
    errores = [None] * n_threads
    threads = [threading.Thread(target=_crear_cotizacion_worker, args=(resultados, errores, i))
               for i in range(n_threads)]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - t0

    exitosos = [n for n in resultados if n]
    fallidos = [e for e in errores if e]

    print(f"\n=== {label} ({n_threads} threads concurrentes, seed_missing={seed_missing}) ===")
    print(f"Completados OK: {len(exitosos)} / {n_threads}")
    print(f"Con excepcion:  {len(fallidos)}")
    print(f"Tiempo total:   {elapsed:.2f}s")

    dup_encontrados = False
    if exitosos:
        contador = Counter(exitosos)
        dups = [n for n, c in contador.items() if c > 1]
        if dups:
            dup_encontrados = True
            print(f"!!! DUPLICADOS: {dups}")
        else:
            print("Numeros unicos: OK (0 duplicados)")

    if fallidos:
        print("--- Errores (hasta 5) ---")
        for e in fallidos[:5]:
            print(" ", e)

    os.remove(DB_PATH)
    return {
        "ok": len(exitosos),
        "errores": len(fallidos),
        "duplicados": dup_encontrados,
        "elapsed": elapsed,
    }


def main():
    r1 = _run(N_THREADS, seed_missing=False, label="Caso normal (semilla ya existe, como en produccion)")
    r2 = _run(N_THREADS, seed_missing=True, label="Caso adversarial (semilla ausente, boot no corrio)")

    print("\n=== RESUMEN ===")
    fallo_algo = r1["duplicados"] or r2["duplicados"] or r1["errores"] or r2["errores"] or \
        r1["ok"] != N_THREADS or r2["ok"] != N_THREADS
    if fallo_algo:
        print("RESULTADO: se encontraron problemas -- revisar arriba.")
    else:
        print(f"RESULTADO: {N_THREADS}+{N_THREADS} creaciones concurrentes, "
              f"0 duplicados, 0 excepciones, en ambos escenarios.")


if __name__ == "__main__":
    main()
