"""
Limpieza FASE 4 — Archivar módulo Colaboradores standalone desde app.py
=========================================================================

Eliminamos del archivo app.py todos los bloques de Colaboradores (módulo
HRM standalone, oculto en menú). Cero consumidores activos verificados:
- /mantenciones/api/colaboradores-search se usaba SOLO desde
  templates/mantenciones/tecnicos.html (template DEPRECATED, ruta /mantenciones/tecnicos
  redirige a /admin/users?rol=tecnico)
- /colaboradores/* no aparece en menú activo (comentado en base.html)
- hrm_* tablas no son JOINeadas por ninguna otra parte de la app

Bloques eliminados (en orden INVERSO para preservar numeración):

1. L31960-31983 : endpoint /mantenciones/api/colaboradores-search
2. L9957-9959   : comentario huérfano "MÓDULO: PREGUNTAS GENÉRICAS"
                  (ya borradas las rutas en FASE 3)
3. L9719-9954   : 5 rutas /colaboradores/* (index, nuevo, ficha, editar, eliminar)
4. L9598-9716   : init_hrm_tables() + helpers (_get_or_create_area,
                  _get_or_create_cargo, _save_colab_foto)
5. L9563-9596   : constantes solo usadas por colaboradores
                  (CHILE_REGIONES, GENEROS, ESTADOS_COLAB)
6. L9559-9561   : constantes HRM_*_TABLE
7. L2767        : llamada init_hrm_tables() en init_db()
8. L697         : os.makedirs(COLABS_FOLDER, ...)
9. L660         : COLABS_FOLDER constante

Templates a archivar después (con git mv, no en este script):
- templates/colaboradores/*  (3 archivos)
- templates/mantenciones/tecnicos.html        (DEPRECATED)
- templates/mantenciones/tecnico_ficha.html   (DEPRECATED)

Las 3 tablas hrm_* PERMANECEN en BD (regla #6 del proyecto). Datos
históricos intactos. Rollback via:
  git checkout safepoint-deadcode-cleanup-2026-05-28 -- app.py templates/

Uso:
    python tools/archive/_cleanup_phase4.py
"""
import io
import sys

PATH = "app.py"

# Rangos 1-indexed inclusive a eliminar, EN ORDEN INVERSO
RANGES_TO_DELETE = [
    (31960, 31983),  # endpoint colaboradores-search
    (9957,  9959),   # comentario huérfano "MÓDULO: PREGUNTAS GENÉRICAS"
    (9719,  9954),   # rutas /colaboradores/*
    (9598,  9716),   # init_hrm_tables + helpers
    (9563,  9596),   # CHILE_REGIONES + GENEROS + ESTADOS_COLAB
    (9559,  9561),   # HRM_*_TABLE constantes
    (2767,  2767),   # init_hrm_tables() en init_db
    (697,   697),    # os.makedirs(COLABS_FOLDER, ...)
    (660,   660),    # COLABS_FOLDER constante
]

# Safety: substring esperado en línea X. Si no coincide → aborta.
SAFETY_CHECKS = [
    (31960, '@app.route("/mantenciones/api/colaboradores-search'),
    (31983, '[dict(r) for r in rows]'),                # último return del endpoint
    (9957,  '# '),                                      # comentario barra
    (9959,  '# '),                                      # comentario barra fin
    (9719,  'Listado colaboradores'),                   # comentario que precede al primer @app.route
    (9954,  'return redirect(url_for("colab_index"))'), # último return de colaboradores
    (9598,  'def init_hrm_tables():'),
    (9716,  'return fname'),                            # último return de _save_colab_foto
    (9563,  'CHILE_REGIONES = ['),
    (9595,  '}'),                                       # cierre de ESTADOS_COLAB (línea 9596 está vacía)
    (9559,  'HRM_AREAS_TABLE'),
    (9561,  'HRM_COLAB_TABLE'),
    (2767,  'init_hrm_tables()'),
    (697,   'os.makedirs(COLABS_FOLDER'),
    (660,   'COLABS_FOLDER'),
]


def main():
    with io.open(PATH, "r", encoding="utf-8") as fh:
        lines = fh.readlines()

    n_before = len(lines)

    for line_num, expected in SAFETY_CHECKS:
        if line_num > len(lines):
            print(f"[ERR] Line {line_num} out of range ({len(lines)} total)")
            sys.exit(1)
        actual = lines[line_num - 1].rstrip("\n")
        if expected not in actual:
            print(f"[ERR] Line {line_num} safety check failed.")
            print(f"   Expected substring: {expected!r}")
            print(f"   Actual line       : {actual!r}")
            sys.exit(1)

    print("[OK] All safety checks passed")

    deleted = 0
    for start, end in RANGES_TO_DELETE:
        s, e = start - 1, end
        n = e - s
        del lines[s:e]
        deleted += n
        print(f"[OK] Deleted lines {start}-{end} ({n} lines)")

    with io.open(PATH, "w", encoding="utf-8", newline="\n") as fh:
        fh.writelines(lines)

    n_after = len(lines)
    print(f"\nFASE 4 done:")
    print(f"  app.py before: {n_before} lines")
    print(f"  app.py after : {n_after} lines")
    print(f"  Deleted      : {deleted} lines ({100.0 * deleted / n_before:.1f}% of file)")


if __name__ == "__main__":
    main()
