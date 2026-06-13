# Prompt de alineación de sesiones — ILUS

Pega **este bloque al inicio** de cualquier sesión nueva de Claude Code en el proyecto ILUS,
para que todas trabajen igual y no se pisen entre sí.

---

```
Trabajamos en el proyecto ILUS Sport & Health. Antes de tocar código, lee CLAUDE.md
(reglas no negociables) y revisa tu memoria del proyecto. Reglas de cómo operamos:

1. ERP Random = READ-ONLY ABSOLUTO. Solo SELECT vía _random_sql_query (SQL Server) o
   erp_engine.fetch_* (REST). Jamás INSERT/UPDATE/DELETE ni tocar sus tablas.

2. Antes de cualquier consulta nueva al ERP, lee los diccionarios en docs/erp/
   (244 tablas, 600 campos). Gotcha clave: el ENDO del documento es el KOEN (código de
   entidad), NO el RUT → joinear MAEEN por KOEN. Montos = VANELI (valor neto de línea).
   Comuna = TABCM por (KOCM, KOCI). SKU de flete = ZZENVIO.

3. main = fuente única de verdad. Parte SIEMPRE de un worktree nuevo desde origin/main
   actualizado. NO trabajes sobre la rama/worktree de otra sesión ni commitees su trabajo
   sin commitear. Para producción: merge a main vía PR (GitHub Actions auto-despliega;
   docs/** y *.md NO despliegan).

4. Confirma con Daniel antes de CADA merge/push/deploy/cambio en GCP. La aprobación de una
   acción NO se extiende a la siguiente.

5. UI: prohibidos alert()/confirm()/prompt() nativos → usa ilusAlert/ilusConfirm/ilusToast/
   ilusPrompt.

6. NUNCA borres ni ocultes features existentes (links, columnas, botones, módulos) sin "sí"
   explícito de Daniel. Agregar y mejorar: siempre. Quitar: solo con permiso.

7. Háblale a Daniel en español neutro (tú, no vos), claro y sin tecnicismos. No es programador.

8. Antes de pushear: valida sintaxis  ->  python -c "import ast; ast.parse(open('app.py',encoding='utf-8').read())"
   y el Jinja de los templates que tocaste. Migraciones idempotentes.

Empieza resumiendo en 2 líneas qué vas a hacer, y espera mi OK antes de cualquier acción
de producción (merge/push/deploy).
```

---

_Mantenido por las sesiones de Claude. Si cambia una convención, actualiza este archivo y CLAUDE.md._
