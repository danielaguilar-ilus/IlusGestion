"""
Corte quirúrgico de módulos muertos en app.py — por fases, con anclas de contenido.
=================================================================================

Re-lee app.py fresco en cada corrida, así los números de línea no importan
(usa substrings-ancla). Cada corte verifica que la ancla existe antes de borrar.

PRESERVA SIEMPRE: RESETS_TABLE, _require_superadmin, permiso hrm scaffolding.
NO toca tablas MySQL (eso es a nivel BD, no acá).

Uso:
    python tools/archive/_cut_modules.py 2   # Preguntas Genéricas
    python tools/archive/_cut_modules.py 3   # Evaluaciones
    python tools/archive/_cut_modules.py 4   # Colaboradores
"""
import io
import sys

PATH = "app.py"


def load():
    with io.open(PATH, "r", encoding="utf-8") as fh:
        return fh.readlines()


def save(lines):
    with io.open(PATH, "w", encoding="utf-8", newline="\n") as fh:
        fh.writelines(lines)


def find(lines, substr, start=0):
    for i in range(start, len(lines)):
        if substr in lines[i]:
            return i
    raise SystemExit(f"[ERR] ancla no encontrada: {substr!r}")


def find_exact(lines, exact, start=0):
    """Busca una línea cuyo strip() == exact (para distinguir call de def)."""
    for i in range(start, len(lines)):
        if lines[i].strip() == exact:
            return i
    raise SystemExit(f"[ERR] linea exacta no encontrada: {exact!r}")


def cut_range(lines, start_substr, end_substr_exclusive, label):
    """Borra [start, end) donde end es la PRIMERA línea con end_substr tras start."""
    s = find(lines, start_substr)
    e = find(lines, end_substr_exclusive, s + 1)
    n = e - s
    del lines[s:e]
    print(f"[OK] {label}: borradas {n} lineas (desde ancla {start_substr!r})")
    return n


def cut_one(lines, exact, label):
    """Borra una sola línea exacta."""
    i = find_exact(lines, exact)
    del lines[i:i + 1]
    print(f"[OK] {label}: borrada 1 linea ({exact!r})")
    return 1


def fase2(lines):
    """Preguntas Genéricas: borra las 4 rutas. Preserva _require_superadmin."""
    total = 0
    # Rutas preg_gen: desde la primera @app.route hasta el comentario del módulo Evaluaciones
    total += cut_range(
        lines,
        '@app.route("/admin/preguntas-genericas/")',
        "MÓDULO: GESTIÓN DE EVALUACIONES",
        "preg_gen routes",
    )
    # Ese corte deja '# ═══' colgando justo antes del comentario eval. Lo dejamos:
    # el comentario eval + su barra se borran completos en FASE 3.
    return total


def fase3(lines):
    """Evaluaciones: borra bloque completo (consts+init+helpers+rutas) + call init_db."""
    total = 0
    # Bloque eval: desde la barra '# ═══' que precede a 'MÓDULO: GESTIÓN DE EVALUACIONES'
    # hasta la barra '# ═══' que precede a 'MÓDULO CUBICADOR'.
    eval_hdr = find(lines, "MÓDULO: GESTIÓN DE EVALUACIONES")
    start = eval_hdr - 1  # la línea '# ═══════' de arriba
    if "═" not in lines[start]:
        # fallback: si no es barra, empezar en el header mismo
        start = eval_hdr
    cub_hdr = find(lines, "MÓDULO CUBICADOR", start + 1)
    end = cub_hdr - 1  # la barra '# ═══' de arriba de CUBICADOR (la preservamos)
    if "═" not in lines[end]:
        end = cub_hdr
    n = end - start
    del lines[start:end]
    total += n
    print(f"[OK] eval block: borradas {n} lineas")
    # Call en init_db
    total += cut_one(lines, "init_eval_tables()", "call init_eval_tables en init_db")
    return total


def fase4(lines):
    """Colaboradores + HRM: consts, init, helpers, rutas, colab-search, call init_db, COLABS_FOLDER."""
    total = 0
    # 1) Constantes HRM (HRM_AREAS..PREG_GEN_TABLE) + CHILE_REGIONES, preservando RESETS_TABLE
    total += cut_range(lines, "HRM_AREAS_TABLE  =", "RESETS_TABLE =", "HRM consts + CHILE_REGIONES")
    # 2) GENEROS + ESTADOS_COLAB + init_hrm_tables + helpers + rutas colab, preservando _require_superadmin
    total += cut_range(lines, "GENEROS = {", "def _require_superadmin", "GENEROS..rutas colab")
    # 3) Call init_hrm_tables en init_db
    total += cut_one(lines, "init_hrm_tables()", "call init_hrm_tables en init_db")
    # 4) Endpoint colaboradores-search (hasta el siguiente @app.route POST tecnicos)
    total += cut_range(
        lines,
        '@app.route("/mantenciones/api/colaboradores-search"',
        '@app.route("/mantenciones/api/tecnicos", methods=["POST"])',
        "colaboradores-search endpoint",
    )
    # 5) COLABS_FOLDER: def + makedirs (dos líneas sueltas en el top del archivo)
    i = find(lines, "COLABS_FOLDER   = os.path.join")
    del lines[i:i + 1]
    total += 1
    print("[OK] COLABS_FOLDER def borrada")
    i = find(lines, "os.makedirs(COLABS_FOLDER")
    del lines[i:i + 1]
    total += 1
    print("[OK] COLABS_FOLDER makedirs borrada")
    return total


def fase5(lines):
    """5 endpoints muertos verificados (0 consumidores en templates/static)."""
    total = 0
    # comm: diagnostico-completo + test-rapido (contiguos), incl. comment header '# ════'
    hdr = find(lines, "DIAGNÓSTICO COMPLETO DE COMUNICACIONES")
    start = hdr - 1 if "═" in lines[hdr - 1] else hdr
    end = find(lines, '@app.route("/api/erp/health"', hdr)
    n = end - start
    del lines[start:end]
    total += n
    print(f"[OK] comm diagnostico-completo + test-rapido: borradas {n} lineas")
    # mantenciones: erp-rut, ultimo-cliente, buscar-erp
    total += cut_range(lines, '@app.route("/mantenciones/api/erp-rut"',
                       '@app.route("/mantenciones/api/agente-contrato"', "erp-rut")
    total += cut_range(lines, '@app.route("/mantenciones/api/ultimo-cliente"',
                       '@app.route("/mantenciones/clientes/<int:cid>")', "ultimo-cliente")
    total += cut_range(lines, '@app.route("/mantenciones/api/buscar-erp"',
                       '@app.route("/mantenciones/api/clientes/<int:cid>/documentos-erp")', "buscar-erp")
    return total


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("2", "3", "4", "5"):
        raise SystemExit("Uso: python tools/archive/_cut_modules.py [2|3|4|5]")
    fase = sys.argv[1]
    lines = load()
    before = len(lines)
    total = {"2": fase2, "3": fase3, "4": fase4, "5": fase5}[fase](lines)
    save(lines)
    print(f"\nFASE {fase}: -{total} lineas | app.py {before} -> {len(lines)}")


if __name__ == "__main__":
    main()
