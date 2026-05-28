# PROMPT — Sesión nueva para arreglar autocompletado de direcciones (Dropit Service)

> Copia y pega TODO lo de abajo (desde "## CONTEXTO" hasta el final) en una sesión nueva de Claude Code con `cd C:\Users\DANIE\Desktop\ChatGPT\Dropit`.

---

## CONTEXTO

Estoy trabajando en **Dropit Service**, una plataforma TMS (Transport Management System) para fletes en Chile. Monorepo en `C:\Users\DANIE\Desktop\ChatGPT\Dropit` con `apps/api` (Express/Node 20) + `apps/web` (React/Vite/Tailwind), deployado en Railway.

## OBJETIVO

Que el autocomplete de direcciones del wizard admin (`apps/web/src/components/StreetAutocomplete.jsx`) muestre sugerencias **al nivel de Google Maps** — texto bold + comuna gris + cobertura completa de Chile incluyendo direcciones específicas como **"Caupolicán 960, Quilicura"**.

## ESTADO ACTUAL CONFIRMADO

- ✅ Componente con cascada `Google Places → HERE Maps → Photon+Nominatim` ya implementado
- ✅ Variable `VITE_GOOGLE_MAPS_API_KEY=AIzaSyDqhmm8IAXFG1ll3rEf0SKp4rkl5BB86us` configurada en Railway
- ✅ SDK de Google carga (verificado en consola del navegador)
- ❌ Google devuelve `REQUEST_DENIED` (visto en logs del navegador) porque billing **NO** está habilitado en Google Cloud Console
- ⚠️ Photon/Nominatim devuelven resultados parciales (OSM tiene gaps en direcciones chilenas — "Caupolicán 960 Quilicura" simplemente no está indexado en OpenStreetMap)
- ❌ HERE Maps fue rechazado por el usuario (aunque ofrece 250k/mes sin tarjeta)

## RESTRICCIÓN INNEGOCIABLE DEL USUARIO

**NO quiere registrar tarjeta de crédito en Google Cloud Console.** Ni siquiera para tener el tier gratis de $200/mes que Google ofrece. Argumento: temor a cobros automáticos / es un proyecto en bootstrap.

## LO QUE NECESITO QUE HAGAS

### Fase 1 — VERIFICACIÓN HONESTA (con documentación oficial)

Confirma con documentación oficial (links a docs.google.com, mapbox.com, tomtom.com, etc.) las siguientes preguntas. **NO ESPECULES** — si no encuentras la respuesta documentada, dilo:

1. **¿Existe alguna manera 2024/2025/2026 de usar Google Maps Places API sin habilitar billing en Google Cloud Console?**
   - Pista: la respuesta probable es NO desde junio 2018, pero verifica si hay alguna excepción, modo gratuito sin tarjeta, o programa educacional/startup que aplique.
   - Si no aplica, decirlo claro al usuario.

2. **¿Qué proveedores de geocoding ofrecen tier gratuito SIN tarjeta de crédito y con cobertura razonable en Chile?**
   - Investigar y comparar:
     - TomTom (¿realmente sin tarjeta? ¿cuántos requests gratis?)
     - MapTiler Cloud
     - OpenCage Data
     - Geoapify
     - LocationIQ
     - Mapbox (¿requiere tarjeta?)
     - Photon (ya en uso, basado en OSM, gaps confirmados en CL)
     - Nominatim (ya en uso, mismo problema)
   - Para cada uno, verificar **con su documentación oficial**:
     - Si requiere tarjeta de crédito o solo email
     - Requests/día o /mes en tier gratis
     - Calidad/cobertura específica en Chile (no marketing — busca reviews reales o prueba la API en el chat)
     - Si es basado en OSM (entonces tendrá los mismos gaps que Photon/Nominatim) o tiene dataset propio

3. **¿Hay algún dataset abierto o API gubernamental chilena que pueda complementar el geocoding?**
   - Servel (mesas electorales tienen direcciones)
   - INE (censo)
   - Correos de Chile / SII (probable que no tengan API pública)
   - Reportar lo que encuentres con honestidad.

### Fase 2 — DECISIÓN INFORMADA

Con la información de Fase 1, dale al usuario **2-3 opciones reales** con trade-offs claros:

```
Opción A: TomTom (sin tarjeta, 2.5k req/día)
  - Pro: cobertura propia Chile decente
  - Con: 2.5k/día = ~75k/mes, suficiente para arranque
  - Verificar: requiere registro con email + ¿realmente sin tarjeta?

Opción B: Google Maps con billing activado (RECHAZADO por usuario)
  - Pro: mejor cobertura del mundo, $200 crédito gratis mensual
  - Con: requiere tarjeta de crédito (que el usuario NO quiere dar)

Opción C: Mantener cascada actual + agregar provider X + dataset CL manual
  - Pro: gratis 100%
  - Con: nunca va a igualar Google para direcciones específicas

Opción D: ... (lo que descubras)
```

**NO le digas al usuario que "Photon es suficiente" si los logs muestran que no encuentra Caupolicán 960. Eso ya está descartado.**

### Fase 3 — IMPLEMENTACIÓN (solo después de que el usuario decida)

Si el usuario elige TomTom (probable):

1. Crear cuenta en https://developer.tomtom.com con su email (sin tarjeta)
2. Obtener API key gratis (2.5k req/día)
3. Configurar `VITE_TOMTOM_API_KEY` en Railway
4. Modificar `apps/web/src/components/StreetAutocomplete.jsx`:
   - Quitar Google Maps Places (no sirve sin billing)
   - Cascada nueva: **TomTom Search API → Photon → Nominatim**
   - Mantener formato de presentación: texto en bold + comuna en gris
   - Filtros: `countrySet=CL`, `language=es-CL`
   - Endpoint: `https://api.tomtom.com/search/2/search/{query}.json?key={key}&countrySet=CL&typeahead=true&limit=8`

Si el usuario elige otra opción, implementar esa.

## ARCHIVOS CLAVE A LEER PRIMERO

```
C:\Users\DANIE\Desktop\ChatGPT\Dropit\apps\web\src\components\StreetAutocomplete.jsx
C:\Users\DANIE\Desktop\ChatGPT\Dropit\apps\web\package.json
C:\Users\DANIE\Desktop\ChatGPT\Dropit\apps\api\.env (variables disponibles)
C:\Users\DANIE\Desktop\ChatGPT\Dropit\railway.toml o equivalente
```

## CRITERIO DE ÉXITO

El usuario escribe "Caupolicán 960" y el dropdown muestra:
```
Caupolicán 960
Quilicura, Región Metropolitana

Caupolicán 960 (otro match si hay)
San Bernardo, Región Metropolitana
```

Si TomTom (u otro proveedor sin tarjeta) NO puede mostrar esa dirección, el usuario tiene que saberlo CLARAMENTE — no le digas que "casi funciona".

## TONO

- **HONESTO**: si la respuesta es "sin Google Maps no vas a igualar la cobertura", dilo directo.
- **TÉCNICO**: con links a docs oficiales, no a blogs aleatorios.
- **CONCISO**: el usuario es no-programador, pero entiende lógica de negocio.
- **PRÁCTICO**: terminar con un plan de acción ejecutable, no con teoría.

## QUÉ NO HACER

- ❌ NO decir "Google Maps funciona sin billing" sin verificarlo (la respuesta más probable es NO)
- ❌ NO recomendar HERE Maps (rechazado por el usuario)
- ❌ NO insistir en OSM/Nominatim/Photon (ya probados, no cubren CL bien)
- ❌ NO inventar APIs que no existen (verifica TODO contra docs oficiales)
- ❌ NO ejecutar el código sin antes presentar las opciones al usuario y dejarlo decidir

---

## TL;DR para sesión nueva

1. Lee `StreetAutocomplete.jsx`
2. Verifica con documentación oficial cuál proveedor sin tarjeta tiene mejor cobertura en CL
3. Presenta 2-3 opciones reales al usuario con trade-offs
4. Espera decisión del usuario
5. Implementa la opción elegida
6. Test con "Caupolicán 960 Quilicura" como criterio de éxito

**Empieza por la Fase 1 (verificación honesta). No pases a Fase 3 sin que el usuario apruebe la decisión.**
