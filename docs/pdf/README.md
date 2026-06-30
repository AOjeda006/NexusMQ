# Compilación de la documentación a PDF

Genera **`NexusMQ-documentacion-tecnica.pdf`** a partir de los Markdown de
[`docs/`](../): reúne la documentación técnica ([`docs/tecnica/`](../tecnica/)),
el catálogo de diagramas ([`docs/diagramas/`](../diagramas/), con Mermaid
renderizado) y el registro de ADR ([`docs/adr/`](../adr/)) en un único PDF con
portada, índice y numeración de páginas.

## Cómo se construye

El *pipeline* es **Markdown → HTML → PDF**, sin LaTeX:

1. [`markdown-it`](https://github.com/markdown-it/markdown-it) convierte cada `.md` a HTML.
2. Los bloques ` ```mermaid ` se renderizan a SVG con [Mermaid](https://mermaid.js.org/)
   dentro de **Chromium headless** (vía [Playwright](https://playwright.dev/)).
3. Chromium imprime el HTML resultante a PDF (A4, con pie de página numerado).

## Uso

```bash
cd docs/pdf
npm install          # markdown-it, markdown-it-anchor, mermaid, playwright
node build.cjs       # escribe NexusMQ-documentacion-tecnica.pdf en esta carpeta
```

`npm install` descarga Chromium para Playwright. Si ya tienes uno (p. ej. en CI),
apúntalo con la variable de entorno `PW_CHROMIUM_PATH=/ruta/al/chrome` y se usará
ese binario en lugar de descargarlo.

El script **falla con código ≠ 0** si algún diagrama Mermaid no renderiza, de modo
que sirve también como verificación de la sintaxis de los diagramas.

> El PDF generado se versiona en esta carpeta como entregable; `node_modules/`,
> `package-lock.json` y los HTML intermedios quedan fuera (ver `.gitignore`).
