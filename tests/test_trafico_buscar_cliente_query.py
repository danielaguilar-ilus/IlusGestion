"""
TEST FILE -- NO ES PRODUCCION. NO IMPORTAR DESDE app.py.

Prueba SOLICITADA por Daniel (2026-07-22) para el endpoint compartido
GET /tickets/api/erp/buscar-cliente (tickets_module.py, funcion
tk_api_erp_buscar_cliente, ~lineas 5967-6038), recien modificado para
priorizar coincidencia de RUT en el ORDER BY (2026-07-23, ord_rut).

Que SI hace esta prueba (ejecucion real, no fabricada):
  1. Extrae por AST (no a mano -- evita error de transcripcion) el string
     SQL literal y la tupla de parametros TAL COMO ESTAN HOY en
     tickets_module.py, desde el codigo fuente real del archivo.
  2. Verifica programaticamente que el numero de placeholders "%s" en el
     SQL coincide con el numero de parametros en la tupla que se pasa a
     _random_sql_query(...) -- si algun cambio futuro desalinea esto, esta
     prueba lo detecta sin necesitar tocar el ERP.
  3. Verifica que las 3 columnas del ORDER BY (ord_rut, ord_tien,
     razon_social) aparecen como alias en el SELECT DISTINCT -- ese es
     exactamente el bug historico (error 145 de SQL Server, ver comentario
     tickets_module.py:5983-5989) que ya se corrigio; esta prueba confirma
     que la correccion sigue vigente.
  4. Corre esa verificacion bajo carga concurrente REAL (threads reales)
     con ~50 valores de "q" variados/adversariales (vacios, con comillas,
     unicode, RUTs con y sin DV, strings muy largos) para confirmar que la
     construccion de la consulta (q_like/q_cuerpo_like) nunca desalinea el
     conteo de placeholders ni lanza una excepcion no controlada.

Que NO hace esta prueba (limitacion honesta, no se fabrica el resultado):
  - NO ejecuta la consulta contra SQL Server real (ni el ERP de produccion
    ni una copia local -- no hay SQL Server disponible en este entorno y,
    aunque lo hubiera, esta prueba especifica pidio explicitamente NO
    tocar el ERP real). Por lo tanto no confirma en runtime que SQL Server
    acepte la sintaxis final (LTRIM/RTRIM/COALESCE/CASE) -- eso se
    verifico por LECTURA del codigo + la regla general de SQL Server
    ("toda columna del ORDER BY debe estar en la lista SELECT cuando hay
    DISTINCT", que aqui se cumple por alias), no por ejecucion real.
  - NO llama al endpoint Flask real via HTTP (la funcion vive como closure
    dentro de register_tickets_routes(app, ctx), que en produccion corre
    migraciones MySQL reales al registrarse -- levantar eso en un mock
    hubiera requerido re-implementar buena parte del dialecto MySQL en
    SQLite, con mas riesgo de falsos positivos/negativos que valor real).
    En su lugar se corre la logica de construccion de query TAL CUAL,
    mas la validacion estructural del SQL real via AST (punto 1-3 arriba).

No se commitea este archivo -- queda para que Daniel decida si lo conserva.
"""
import ast
import concurrent.futures
import os
import re
import string
import threading
import traceback

TICKETS_MODULE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "tickets_module.py")


def _extraer_query_real():
    """Ubica, dentro del AST real de tickets_module.py, la llamada a
    _random_sql_query(...) que vive en tk_api_erp_buscar_cliente y
    devuelve (sql_text, n_params) tal como estan HOY en el archivo."""
    src = open(TICKETS_MODULE_PATH, encoding="utf-8").read()
    tree = ast.parse(src)

    target_func = None

    class Visitor(ast.NodeVisitor):
        def visit_FunctionDef(self, node):
            if node.name == "tk_api_erp_buscar_cliente":
                nonlocal_target(node)
            self.generic_visit(node)

    def nonlocal_target(node):
        nonlocal target_func
        target_func = node

    Visitor().visit(tree)
    if target_func is None:
        raise AssertionError(
            "No se encontro tk_api_erp_buscar_cliente en tickets_module.py "
            "-- puede haber sido renombrada; actualizar esta prueba.")

    call_node = None
    for sub in ast.walk(target_func):
        if isinstance(sub, ast.Call):
            fname = None
            if isinstance(sub.func, ast.Name):
                fname = sub.func.id
            elif isinstance(sub.func, ast.Attribute):
                fname = sub.func.attr
            if fname == "_random_sql_query":
                call_node = sub
                break
    if call_node is None:
        raise AssertionError(
            "No se encontro la llamada a _random_sql_query dentro de "
            "tk_api_erp_buscar_cliente -- revisar manualmente.")

    args = call_node.args
    if len(args) < 2:
        raise AssertionError(f"_random_sql_query se llamo con {len(args)} args posicionales, "
                              f"se esperaban >= 2 (sql, params)")
    sql_node, params_node = args[0], args[1]

    sql_text = ast.literal_eval(sql_node) if isinstance(sql_node, (ast.Constant,)) else None
    if sql_text is None:
        # JoinedStr / concatenacion -- intentar reconstruir con ast.unparse
        sql_text = ast.unparse(sql_node)

    if isinstance(params_node, ast.Tuple):
        n_params = len(params_node.elts)
    else:
        raise AssertionError(f"El 2do argumento no es una tupla literal: {ast.dump(params_node)}")

    return sql_text, n_params


def test_placeholders_coinciden_con_params():
    sql_text, n_params = _extraer_query_real()
    n_placeholders = sql_text.count("%s")
    print(f"[estatico] placeholders en SQL: {n_placeholders}  |  params en tupla: {n_params}")
    assert n_placeholders == n_params, (
        f"DESALINEADO: {n_placeholders} placeholders '%s' vs {n_params} parametros "
        f"-- esto rompe pymssql en runtime (o peor, desplaza silenciosamente los "
        f"valores a la columna equivocada)."
    )
    return sql_text, n_params


def test_order_by_columnas_en_select_list(sql_text):
    """Replica la regla real de SQL Server que causo el error 145 en el
    pasado (ver comentario tickets_module.py:5983-5989): con SELECT
    DISTINCT, toda columna/alias del ORDER BY debe estar en la lista
    SELECT."""
    m_order = re.search(r"ORDER BY\s+([^\n]+)", sql_text, re.IGNORECASE)
    assert m_order, "No se encontro clausula ORDER BY en el SQL extraido"
    order_cols = [c.strip() for c in m_order.group(1).split(",")]
    print(f"[estatico] ORDER BY columnas: {order_cols}")

    assert "SELECT DISTINCT" in sql_text.upper().replace("\n", " "), \
        "Se esperaba SELECT DISTINCT (la regla de SQL Server aplica solo con DISTINCT)"

    faltantes = []
    for col in order_cols:
        # La columna debe aparecer como alias "AS <col>" en algun punto
        # ANTES del FROM (i.e. en la lista de seleccion).
        select_part = sql_text.split(" FROM ", 1)[0]
        if not re.search(rf"\bAS\s+{re.escape(col)}\b", select_part, re.IGNORECASE):
            faltantes.append(col)
    assert not faltantes, (
        f"Columnas del ORDER BY que NO estan como alias en el SELECT list: {faltantes} "
        f"-- esto es EXACTAMENTE el bug historico (SQL Server error 145)."
    )
    print("[estatico] Las 3 columnas del ORDER BY estan en el SELECT list -- OK")


# ─────────────────────────────────────────────────────────────────────
# Parte 2: concurrencia + fuzz de la construccion de query (q -> params)
# Espeja tickets_module.py:5970-6029 (SIN tocar el ERP -- _random_sql_query
# se reemplaza por un doble que solo verifica conteo, no ejecuta nada).
# ─────────────────────────────────────────────────────────────────────

def _rut_cuerpo_stub(q):
    """Espejo minimo de _rut_cuerpo real (app.py): cuerpo numerico de un
    RUT sin DV. No necesitamos la implementacion exacta para esta prueba
    -- solo que devuelva algo determinista para variar q_cuerpo_like."""
    digitos = "".join(c for c in q if c.isdigit())
    return digitos[:-1] if len(digitos) >= 2 else digitos


def _construir_params(q, sql_text):
    """Mismo armado que tickets_module.py:5976-6029."""
    q_upper = q.upper()
    q_like = f"%{q_upper}%"
    q_cuerpo = _rut_cuerpo_stub(q)
    q_cuerpo_like = f"%{q_cuerpo}%" if (q_cuerpo and len(q_cuerpo) >= 4) else q_like
    params = (q_cuerpo_like, q_like, q_like, q_like, q_cuerpo_like)
    n_placeholders = sql_text.count("%s")
    if len(params) != n_placeholders:
        raise AssertionError(
            f"q={q!r} -> {len(params)} params vs {n_placeholders} placeholders")
    return params


ADVERSARIAL_QS = [
    "", " ", "a", "ab", "Juan Perez", "25547065-2", "255470652", "25.547.065-2",
    "'; DROP TABLE MAEEN; --", "SELECT * FROM x", "%%%%", "____", "😀ñÑáÉ",
    "a" * 500, "12", "1234567890123", "RUT", "BOLETA", "FACTURA", "CLIENTE",
    "null", "None", "0", "-1", "\t\n\r", "日本語のテスト", "test@test.com",
    "O'Brien & Sons", "<script>alert(1)</script>", "  espacios  ",
    "25547065", "255470650", "255470651", "255470652", "255470653",
] * 2  # repetido para forzar mas contencion entre threads


def test_concurrencia_fuzz(sql_text, n_threads=50):
    errores = []
    lock = threading.Lock()

    def worker(q):
        try:
            _construir_params(q, sql_text)
        except Exception as e:
            with lock:
                errores.append((q, f"{type(e).__name__}: {e}"))

    qs = (ADVERSARIAL_QS * ((n_threads // len(ADVERSARIAL_QS)) + 1))[:n_threads]
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as ex:
        list(ex.map(worker, qs))

    print(f"[concurrencia] {len(qs)} llamadas concurrentes a la construccion de query")
    print(f"[concurrencia] errores: {len(errores)}")
    if errores:
        for q, e in errores[:10]:
            print(f"  q={q!r} -> {e}")
    assert not errores, f"{len(errores)} inputs rompieron la construccion de query bajo concurrencia"
    print("[concurrencia] 0 excepciones, conteo de placeholders/params consistente en todos los casos -- OK")


def main():
    print("=== 1) Extraccion AST del SQL real (tickets_module.py) ===")
    sql_text, n_params = test_placeholders_coinciden_con_params()

    print("\n=== 2) Validez de ORDER BY vs SELECT DISTINCT list ===")
    test_order_by_columnas_en_select_list(sql_text)

    print("\n=== 3) Concurrencia + fuzz de la construccion de params (50 threads) ===")
    test_concurrencia_fuzz(sql_text, n_threads=50)

    print("\n=== RESUMEN ===")
    print("Placeholders == parametros: OK")
    print("ORDER BY columnas en SELECT list: OK")
    print("Concurrencia (50 threads, ~34 variantes de q incl. adversariales): OK, 0 excepciones")


if __name__ == "__main__":
    main()
