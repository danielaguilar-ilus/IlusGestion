"""
Smoke test FASE 0 — verifica que las rutas críticas no exploten al cargar.

Esto NO valida lógica de negocio. Solo verifica que:
- el endpoint existe y responde
- no hay errores de imports rotos
- no hay errores de template missing
- no hay url_for roto que tire BuildError

Sin BD configurada, algunas rutas darán 5xx (esperado), otras 302 redirect
a login (esperado para rutas protegidas). El test pasa si:
- no hay 500 por url_for roto
- no hay 500 por template missing
- no hay 500 por import roto
"""
import os
import sys
import traceback

# Forzar UTF-8 para Windows
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
sys.path.insert(0, ".")

from app import app

# Rutas críticas a smoke-testear
ROUTES = [
    # Públicas / sin login
    ("/ping",                       "GET"),
    ("/_health",                    "GET"),
    ("/login",                      "GET"),
    ("/welcome",                    "GET"),
    ("/forgot-password",            "GET"),

    # Core (deberían redirigir a login si no hay sesión)
    ("/",                           "GET"),
    ("/products/",                  "GET"),
    ("/etiquetas/",                 "GET"),
    ("/cubicador",                  "GET"),
    ("/asignar",                    "GET"),

    # Módulos pesados
    ("/transporte/couriers",        "GET"),
    ("/mantenciones",               "GET"),
    ("/mantenciones/clientes",      "GET"),
    ("/mantenciones/tecnicos",      "GET"),   # deprecated redirect
    ("/comunicaciones",             "GET"),
    ("/retiros",                    "GET"),
    ("/retiros/dashboard",          "GET"),
    ("/admin/users",                "GET"),
    ("/admin/performance",          "GET"),

    # Rutas que YA NO existen (deberían dar 404)
    ("/evaluaciones/",              "GET"),
    ("/colaboradores/",             "GET"),
    ("/admin/preguntas-genericas/", "GET"),
]


def main():
    client = app.test_client()
    results = []
    for path, method in ROUTES:
        try:
            with app.test_request_context():
                resp = client.open(path, method=method)
                status = resp.status_code
                # Esperamos: 200 OK | 302 redirect | 401/403 sin permisos | 404 muerto
                # NO esperamos: 500 (template missing, url_for roto, etc.)
                ok = status < 500
                results.append((path, status, ok, None))
        except Exception as e:
            results.append((path, 0, False, str(e)[:120]))

    # Reporte
    print("\n" + "=" * 78)
    print(f"{'ROUTE':<42}{'STATUS':>8}  RESULT")
    print("=" * 78)
    for path, status, ok, err in results:
        mark = "[OK]" if ok else "[FAIL]"
        line = f"{path:<42}{status:>8}  {mark}"
        if err:
            line += f" -> {err}"
        print(line)

    # Resumen
    failed = [r for r in results if not r[2]]
    total = len(results)
    print("=" * 78)
    print(f"Total: {total} | OK: {total - len(failed)} | FAIL: {len(failed)}")
    if failed:
        print("\nFAILED routes:")
        for path, status, _, err in failed:
            print(f"  - {path} (status={status}) {err or ''}")
        sys.exit(1)
    else:
        print("\n[OK] All critical routes responded < 500 (no BuildError / TemplateNotFound / ImportError)")


if __name__ == "__main__":
    main()
