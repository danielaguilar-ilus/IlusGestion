# Resumen de cambios — 8 mayo 2026

Sesión de trabajo en Cowork. Todos los cambios están en los archivos
del proyecto. Si tu navegador no los ve, es porque OneDrive aún no
sincronizó al disco local — fuerza sync y reinicia `start.bat`.

---

## 1. Limpieza de carpeta (hecha)

Eliminados (existe respaldo en `Backup_Etiquetas_2026-05-08_2155.zip`):
- `Etiquetas Produccion/` (carpeta duplicada del 27 abr)
- `etiquetas.db` (BD SQLite legacy, no se usa)
- `importar.py` (script SQLite legacy)
- `check.py` (script SQLite legacy)
- `__pycache__/` (caché Python, se regenera solo)
- `server.err.log` y `server.out.log` (vacíos)

---

## 2. Comunicaciones — 4 pasos completos

### Paso 1: Persistencia arreglada (3 bugs)
- `_get_smtp_cfg()`: ahora la BD manda sobre las variables de entorno.
  Lo que guardes en pantalla siempre se respeta.
- `comm_smtp_save()`: arreglado bug `prev = cfg` que perdía la
  contraseña al dejar el campo en `••••••••`.
- `init_comunicaciones_tables()`: al arrancar normaliza filas
  duplicadas a id=1 fijo en `comm_smtp_config`, `comm_client_config`,
  `comm_whatsapp_config` y `comm_resend_config`.

### Paso 2: Limpieza UI
**Backend (eliminado):**
- Ruta `/comunicaciones/smtp/gmail-quick`
- Rutas `/comunicaciones/resend/config`, `/verify`, `/domains*`, `/test`
- Funciones `_send_via_resend`, `comm_resend_*`, `comm_smtp_gmail_quick`
- `_get_resend_cfg()` convertida a stub (devuelve siempre vacío)
- `_send_ilus_email_real()` ya no intenta Resend, va directo a SMTP

**Frontend (eliminado del template `comunicaciones/index.html`):**
- Hero "Configuración rápida — Gmail / Workspace" + sus estilos CSS
- Banner Resend
- Sub-tabs Resend/SMTP (queda solo SMTP)
- Card "Dominios verificados"
- Modal "Test Resend"
- Funciones JS: `gmail*`, `verificarResend`, `guardarResend`,
  `abrirTestResend`, `enviarTestResend`, `cargarDominios`,
  `renderDominio`, `agregarDominio`, `verificarDominio`,
  `eliminarDominio`, `switchEmailTab`

### Paso 3: Plantilla del preview en TODOS los emails
- `comm_email_enviar` siempre envuelve con `_comm_render_email_document`
- `comm_log_reintentar` envuelve con plantilla
- Reporte mantención (`mant_reportes/<id>/enviar`) envuelve con plantilla
- Email manual mantenciones (`mant_email_manual`) envuelve con plantilla

### Paso 4: Datos de empresa persistentes
- `comm_client_save` ahora UPSERT con id=1
- `comm_wa_save` ahora UPSERT con id=1
- `_get_client_cfg` lee `WHERE id=1`
- `_get_wa_cfg` lee `WHERE id=1`

---

## 3. Auth — limpieza menor

- Eliminadas funciones duplicadas `_send_recovery_email` y
  `_send_invitation_email` legacy (tenías 2 versiones, prevalecía la
  segunda).
- Eliminado código muerto `if False:` en `reset_password`.
- Eliminado `if True:` redundante en `_send_password_access_email`.

El sistema de permisos ya estaba bien:
- `/admin/users` y `/admin/users/<id>/password-link` requieren
  permiso `admin` (solo superadmin/admin).
- Usuario normal NO puede cambiar su propia contraseña en sesión —
  solo via "Olvidé contraseña" desde el login.
- Todos los emails usan `_ilus_email_html` (plantilla del preview).

---

## 4. Marca temporal en plantilla

Para verificar visualmente que Flask carga la versión nueva, agregué
un **banner verde** al inicio de `/comunicaciones/`:

```
✓ COMUNICACIONES — VERSIÓN LIMPIA (8 MAY 2026 22:55) — Si ves este recuadro verde, los cambios están activos.
```

Cuando confirmes que la versión nueva está cargando, **borra ese
banner** del archivo `templates/comunicaciones/index.html` (las 4
líneas que dicen "MARCA DE VERIFICACIÓN").

---

## Números

| Archivo | Antes | Después | Diferencia |
|---------|-------|---------|------------|
| `app.py` | 13.974 líneas | 13.563 líneas | -411 |
| `templates/comunicaciones/index.html` | 2.506 líneas | 1.714 líneas | -792 |
| Carpeta total | ~21 MB | ~19 MB | -2 MB |

---

## Respaldos disponibles (en `C:\Users\DANIE\OneDrive\Escritorio\Claude\`)

| Archivo | Estado capturado |
|---------|------------------|
| `Backup_Etiquetas_2026-05-08_2155.zip` | Original limpiado |
| `Backup_AntesDeMejoras_2026-05-08_2213.zip` | Antes del Paso 1 |
| `Backup_AntesPaso2_2026-05-08_2230.zip` | Después del Paso 1 |
| `Backup_PostComunicaciones_2026-05-08_2241.zip` | Después de los 4 pasos |
| `Backup_PostAuthYComunicaciones_2026-05-08_2244.zip` | Estado actual final |

Para restaurar: descomprime el zip dentro de la carpeta `Etiquetas/`
sobreescribiendo, y reinicia `start.bat`.

---

## Pendiente para próxima sesión

1. **Mantenciones** — Daniel necesita entregar el lunes. No detecté
   bugs evidentes pero requiere revisión específica de cosas que él
   sabe que están mal.

2. **Reorganización general** — Daniel siente la app muy grande para
   solo tener 1-2 módulos terminados. Decisión pendiente: qué módulos
   ocultar del menú lateral, cuáles consolidar.

3. **Seguridad pendiente** (no urgente pero importante):
   - Mover credenciales de `config.py` a `.env`
   - Cambiar `app.secret_key` de `"ilus-etiquetas-2026"` a una llave
     aleatoria
   - Agregar Flask-Limiter para evitar fuerza bruta en login
   - Activar SESSION_COOKIE_SECURE / HTTPONLY / SAMESITE

Detalles completos en `Informe_Sistema_Etiquetas.docx`.
