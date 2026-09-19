// ════════════════════════════════════════════════════════
//  Compresión de fotos + corrección de rotación EXIF, compartida.
//
//  2026-09-19 (Daniel, levantamiento OT2.0 vs clásica): OT 2.0 subía las
//  fotos crudas -- sin comprimir, sin corregir rotación EXIF -- mientras
//  que static/mantenciones_ot_ejecutar.js (pantalla clásica) sí lo hacía.
//  Este archivo es una COPIA de esos helpers (líneas ~109-336 de ese
//  archivo), no un reemplazo: mantenciones_ot_ejecutar.js sigue con sus
//  propios wrappers, intactos, para no arriesgar la pantalla clásica ya
//  probada. Este archivo es la fuente que usan los puntos de subida de
//  OT 2.0 (templates/ot2/detalle.html).
//
//  Uso: await ilusComprimirImagen(file) -- devuelve un Blob/File listo
//  para FormData. Si el archivo no es comprimible (HEIC, ya liviano,
//  error de decodificación) devuelve el mismo `file` sin tocar.
// ════════════════════════════════════════════════════════

// Probe de 2x1 px (rojo|azul) con EXIF Orientation=6 -- si el navegador
// auto-orienta, decodifica a 1x2 (swap de ejes); si no, queda 2x1 (crudo).
const _ILUS_EXIF_PROBE_B64 =
  '/9j/4AAQSkZJRgABAQAAAQABAAD/4QAiRXhpZgAATU0AKgAAAAgAAQESAAMAAAABAAYAAAAA' +
  'AAD/2wBDAAMCAgMCAgMDAwMEAwMEBQgFBQQEBQoHBwYIDAoMDAsKCwsNDhIQDQ4RDgsLEBYQ' +
  'ERMUFRUVDA8XGBYUGBIUFRT/2wBDAQMEBAUEBQkFBQkUDQsNFBQUFBQUFBQUFBQUFBQUFBQ' +
  'UFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBT/wAARCAABAAIDASIAAhEBAxEB/' +
  '8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9' +
  'AQIDAAQRBRIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY' +
  '3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmq' +
  'KjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx8vP09fb3+' +
  'Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREAAgECBAQDBAcFBAQA' +
  'AQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYkNOEl8RcYGRomJyg' +
  'pKjU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6goOEhYaHiImKkpOUlZ' +
  'aXmJmaoqOkpaanqKmqsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4uPk5ebn6Onq8vP09' +
  'fb3+Pn6/9oADAMBAAIRAxEAPwD4H8Q/8h/Uv+vmX/0M0UUV/ptkP/Ipwn/XuH/pKPAzr/kZ4' +
  'r/r5P8A9KZ//9k=';

function _ilusIcProbeBytes(){
  const bin = atob(_ILUS_EXIF_PROBE_B64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

let _ilusIcProbeBitmapCache = null;
function _ilusIcProbeAutoOrientaBitmap(){
  if (_ilusIcProbeBitmapCache) return _ilusIcProbeBitmapCache;
  _ilusIcProbeBitmapCache = (async () => {
    try {
      const blob = new Blob([_ilusIcProbeBytes()], { type: 'image/jpeg' });
      const bmp = await createImageBitmap(blob);
      const ok = (bmp.width === 1 && bmp.height === 2);
      if (bmp.close) bmp.close();
      return ok;
    } catch(_e){ return false; } // ante la duda, la rotamos nosotros (más seguro)
  })();
  return _ilusIcProbeBitmapCache;
}

let _ilusIcProbeImgCache = null;
function _ilusIcProbeAutoOrientaImg(){
  if (_ilusIcProbeImgCache) return _ilusIcProbeImgCache;
  _ilusIcProbeImgCache = (async () => {
    let url;
    try {
      const blob = new Blob([_ilusIcProbeBytes()], { type: 'image/jpeg' });
      url = URL.createObjectURL(blob);
      const img = await new Promise((resolve, reject) => {
        const im = new Image();
        im.onload = () => resolve(im);
        im.onerror = reject;
        im.src = url;
      });
      return (img.naturalWidth === 1 && img.naturalHeight === 2);
    } catch(_e){ return false; }
    finally { if (url) URL.revokeObjectURL(url); }
  })();
  return _ilusIcProbeImgCache;
}

/** Lee el tag EXIF Orientation (0x0112) de un JPEG. 1-8, default 1. */
function _ilusIcLeerOrientacionExif(file){
  return new Promise((resolve) => {
    if (!file || !/^image\/jpe?g$/i.test(file.type || '')){ resolve(1); return; }
    const reader = new FileReader();
    reader.onerror = () => resolve(1);
    reader.onload = (ev) => resolve(_ilusIcParseExifOrientation(ev.target.result));
    // El EXIF vive al principio del archivo -- no hace falta leerlo entero.
    reader.readAsArrayBuffer(file.slice(0, 128 * 1024));
  });
}

function _ilusIcParseExifOrientation(buffer){
  try {
    const view = new DataView(buffer);
    if (view.byteLength < 4 || view.getUint16(0, false) !== 0xFFD8) return 1; // no es JPEG
    let offset = 2;
    while (offset + 4 <= view.byteLength){
      const marker = view.getUint16(offset, false);
      if ((marker & 0xFF00) !== 0xFF00) break; // marcador inválido -- corta
      offset += 2;
      if (marker === 0xFFD8 || marker === 0xFFD9 || (marker >= 0xFFD0 && marker <= 0xFFD7)){
        continue; // SOI/EOI/RST -- sin campo de longitud
      }
      if (offset + 2 > view.byteLength) break;
      const segLen = view.getUint16(offset, false); // incluye los 2 bytes de longitud
      if (marker === 0xFFE1){ // APP1 -- candidato a EXIF
        const segStart = offset + 2;
        if (segStart + 6 <= view.byteLength && view.getUint32(segStart, false) === 0x45786966){
          const tiffStart = segStart + 6;
          if (tiffStart + 8 <= view.byteLength){
            const little = view.getUint16(tiffStart, false) === 0x4949;
            const firstIfdOffset = view.getUint32(tiffStart + 4, little);
            const dirStart = tiffStart + firstIfdOffset;
            if (dirStart + 2 <= view.byteLength){
              const numEntries = view.getUint16(dirStart, little);
              for (let i = 0; i < numEntries; i++){
                const entryOffset = dirStart + 2 + i * 12;
                if (entryOffset + 10 > view.byteLength) break;
                if (view.getUint16(entryOffset, little) === 0x0112){
                  const val = view.getUint16(entryOffset + 8, little);
                  return (val >= 1 && val <= 8) ? val : 1;
                }
              }
            }
          }
        }
        return 1; // APP1 sin EXIF válido -- es el único lugar donde vive
      }
      if (marker === 0xFFDA) break; // Start of Scan -- no hay más metadata después
      offset += segLen;
    }
  } catch(_e){ /* buffer corrupto/formato inesperado -- usar default */ }
  return 1;
}

/** Matriz de transformación canvas para cada uno de los 8 valores EXIF.
 *  sw,sh = tamaño (YA escalado) que se le pasará a drawImage, SIN rotar. */
function _ilusIcAplicarOrientacionCanvas(ctx, orientation, sw, sh){
  switch (orientation){
    case 2: ctx.transform(-1, 0, 0, 1, sw, 0); break;   // espejo horizontal
    case 3: ctx.transform(-1, 0, 0, -1, sw, sh); break; // 180°
    case 4: ctx.transform(1, 0, 0, -1, 0, sh); break;   // espejo vertical
    case 5: ctx.transform(0, 1, 1, 0, 0, 0); break;     // espejo + 90° CCW
    case 6: ctx.transform(0, 1, -1, 0, sh, 0); break;   // 90° CW
    case 7: ctx.transform(0, -1, -1, 0, sh, sw); break; // espejo + 90° CW
    case 8: ctx.transform(0, -1, 1, 0, 0, sw); break;   // 90° CCW
    default: break; // 1 (o desconocida): sin transformar
  }
}

/** Decodifica `file` y devuelve la orientación EFECTIVA a aplicar (1 si el
 *  navegador ya vino con la rotación puesta -- ver sonda _ilusIcProbe*
 *  arriba). Devuelve { source, sw, sh, orientation, isBitmap, _url }. */
async function _ilusIcDecodificarOrientado(file){
  const orientation = await _ilusIcLeerOrientacionExif(file);
  if (window.createImageBitmap){
    try {
      const [bmp, autoOrienta] = await Promise.all([
        createImageBitmap(file),
        _ilusIcProbeAutoOrientaBitmap(),
      ]);
      return { source: bmp, sw: bmp.width, sh: bmp.height,
                orientation: autoOrienta ? 1 : orientation, isBitmap: true };
    } catch(_e){ /* este archivo no lo soporta createImageBitmap -- cae a <img> */ }
  }
  const url = URL.createObjectURL(file);
  try {
    const [img, autoOrienta] = await Promise.all([
      new Promise((resolve, reject) => {
        const im = new Image();
        im.onload = () => resolve(im);
        im.onerror = reject;
        im.src = url;
      }),
      _ilusIcProbeAutoOrientaImg(),
    ]);
    return { source: img, sw: img.naturalWidth, sh: img.naturalHeight,
              orientation: autoOrienta ? 1 : orientation, isBitmap: false, _url: url };
  } catch(e){
    URL.revokeObjectURL(url);
    throw e;
  }
}

/** Pinta `decoded` en un <canvas> nuevo, ya orientado hacia arriba y
 *  reescalado para que el lado mayor mida como máximo maxDim. Libera el
 *  ImageBitmap/object URL usados. */
function _ilusIcPintarOrientadoEnCanvas(decoded, maxDim){
  const { source, sw: sw0, sh: sh0, orientation } = decoded;
  const scale = Math.min(1, maxDim / Math.max(sw0, sh0));
  const sw = Math.max(1, Math.round(sw0 * scale)); // tamaño para drawImage, SIN rotar
  const sh = Math.max(1, Math.round(sh0 * scale));
  const swapEjes = orientation >= 5 && orientation <= 8;
  const canvas = document.createElement('canvas');
  canvas.width  = swapEjes ? sh : sw;
  canvas.height = swapEjes ? sw : sh;
  const ctx = canvas.getContext('2d');
  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = 'high';
  _ilusIcAplicarOrientacionCanvas(ctx, orientation, sw, sh);
  ctx.drawImage(source, 0, 0, sw, sh);
  if (decoded.isBitmap && source.close) source.close();
  if (decoded._url) URL.revokeObjectURL(decoded._url);
  return canvas;
}

/**
 * Comprime una imagen en cliente antes de subirla.
 * - Si pesa <500KB o ya es WebP/AVIF/HEIC: skip.
 * - Si no: redibuja a max 1920px de ancho, JPG quality=0.85 -- YA orientada
 *   según su tag EXIF.
 * - Reduce ~70-90% el peso (iPhone 4032×3024 ≈ 4.5MB → ~400KB).
 *
 * @param {File} file  archivo input (image/*)
 * @param {number} [maxDim=1920]  lado mayor máximo en px
 * @param {number} [quality=0.85]  calidad JPEG 0-1
 * @returns {Promise<File|Blob>} archivo listo para FormData
 */
async function ilusComprimirImagen(file, maxDim, quality){
  maxDim = maxDim || 1920;
  quality = quality == null ? 0.85 : quality;
  if (!file || !file.type || !file.type.startsWith('image/')) return file;
  // Skip si pesa poco — ya está aceptable.
  if (file.size && file.size < 500 * 1024) return file;
  // Skip si ya es WebP/AVIF/HEIC (formatos modernos ya optimizados, y HEIC
  // además no lo puede decodificar un <canvas> en la mayoría de navegadores).
  const t = (file.type || '').toLowerCase();
  const nombreHeic = /\.hei[cf]$/i.test(file.name || '');
  if (t.includes('webp') || t.includes('avif') || t.includes('heic') || t.includes('heif') || nombreHeic){
    return file;
  }
  try {
    const decoded = await _ilusIcDecodificarOrientado(file);
    const canvas = _ilusIcPintarOrientadoEnCanvas(decoded, maxDim);
    const blob = await new Promise(res => canvas.toBlob(res, 'image/jpeg', quality));
    if (!blob) return file;
    // Si el resultado es MAYOR que el original (caso raro: PNG pequeño con
    // mucha transparencia que JPG no comprime bien), devolvemos el original.
    if (blob.size >= file.size) return file;
    try {
      const newName = (file.name || 'foto').replace(/\.[^.]+$/, '') + '.jpg';
      return new File([blob], newName, { type: 'image/jpeg', lastModified: Date.now() });
    } catch(_e){
      return blob;
    }
  } catch(err){
    console.warn('[ilus_img_compress] fallback (error)', err);
    return file;
  }
}
