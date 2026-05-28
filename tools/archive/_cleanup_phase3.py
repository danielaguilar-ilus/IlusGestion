"""
Limpieza FASE 3 — Archivar módulo Evaluaciones desde app.py
================================================================

Eliminamos del archivo app.py los siguientes bloques:

1. Línea 2768          → `    init_eval_tables()` (llamada al init)
2. Línea 9563          → `PREG_GEN_TABLE = "eval_preguntas_genericas"` (constante)
3. Líneas 9651-9665    → CREATE TABLE eval_preguntas_genericas dentro de init_hrm_tables()
4. Líneas 9992-10088   → Rutas /admin/preguntas-genericas/ (4 rutas)
5. Líneas 10090-10493  → Módulo Evaluaciones completo (constantes + 10 rutas + helpers)

Los bloques 2-5 son archivados al safepoint git
`safepoint-deadcode-cleanup-2026-05-28` (recover via
`git checkout safepoint-deadcode-cleanup-2026-05-28 -- app.py`).

Las 3 tablas (eval_evaluaciones, eval_preguntas, eval_preguntas_genericas)
SIGUEN EN BD — no se dropean. Si Daniel reactiva el módulo, los datos
históricos están intactos.

Uso:
    python tools/archive/_cleanup_phase3.py
"""
import io
import sys

PATH = "app.py"

# Rangos 1-indexed inclusive a eliminar
# Importante: SE EJECUTAN EN ORDEN INVERSO para que los números no cambien
RANGES_TO_DELETE = [
    (10090, 10493),  # Módulo Evaluaciones completo
    (9992,  10088),  # Rutas /admin/preguntas-genericas/
    (9650,  9665),   # CREATE TABLE eval_preguntas_genericas (incluye cur.execute(f""")
    (9563,  9563),   # PREG_GEN_TABLE constante
    (2768,  2768),   # init_eval_tables() en init_db
]

# Marcadores de seguridad: el primer/último texto que esperamos encontrar
# en cada rango. Si no coincide, abortamos para no borrar lo que no es.
SAFETY_CHECKS = [
    (10090, "# "),                                       # comentario barra
    (10492, '"estado": nuevo'),                           # último return del módulo
    (9992,  '@app.route("/admin/preguntas-genericas/")'), # primera ruta /admin/preguntas-genericas
    (10085, '"ok": True'),                                # último return de las rutas preguntas-genericas
    (9650,  "cur.execute("),                              # inicio CREATE TABLE eval_preguntas_genericas
    (9665,  '"""'),                                       # cierre triple-quote del CREATE
    (9563,  'PREG_GEN_TABLE'),                            # constante
    (2768,  'init_eval_tables()'),                        # call en init_db
]


def main():
    with io.open(PATH, "r", encoding="utf-8") as fh:
        lines = fh.readlines()

    n_before = len(lines)

    # Safety checks
    for line_num, expected in SAFETY_CHECKS:
        if line_num > len(lines):
            print(f"[ERR] Line {line_num} out of range ({len(lines)} total)")
            sys.exit(1)
        actual = lines[line_num - 1].rstrip("\n")
        if expected not in actual:
            print(f"[ERR] Line {line_num} safety check failed.")
            print(f"   Expected substring: {expected!r}")
            print(f"   Actual line       : {actual!r}")
            print("   Aborting to avoid wrong deletion.")
            sys.exit(1)

    print("[OK] All safety checks passed")

    # Delete ranges in reverse so line numbers stay valid
    deleted = 0
    for start, end in RANGES_TO_DELETE:
        # 1-indexed inclusive → 0-indexed half-open
        s, e = start - 1, end
        n = e - s
        del lines[s:e]
        deleted += n
        print(f"[OK] Deleted lines {start}-{end} ({n} lines)")

    with io.open(PATH, "w", encoding="utf-8", newline="\n") as fh:
        fh.writelines(lines)

    n_after = len(lines)
    print(f"\nFASE 3 done:")
    print(f"  app.py before: {n_before} lines")
    print(f"  app.py after : {n_after} lines")
    print(f"  Deleted      : {deleted} lines ({100.0 * deleted / n_before:.1f}% of file)")


if __name__ == "__main__":
    main()
