---
name: compilar-informe-latex
description: 'Genera o actualiza el informe técnico en LaTeX (informe_cuentas_contables.tex) documentando el procesamiento de datos de un script Python de BI (ingresos vs gastos, margen operativo, cuentas contables). Usa cuando necesites compilar el PDF, documentar queries SQL, cuentas contables, lógica de negocio o distribucion de costos en LaTeX. Triggers: compilar PDF, generar informe, documentar script Python, informe cuentas contables, LaTeX BI.'
argument-hint: 'Script Python a documentar (ej: ingresos_vs_gastos.py) y carpeta destino (ej: informe_cuentas_contables_2026_04)'
---

# Compilar Informe LaTeX de Procesamiento de Datos BI

## Cuándo usar esta skill

- El usuario pide compilar o generar el PDF del informe de cuentas contables
- El usuario pide documentar la lógica de un script Python de extracción/procesamiento de datos
- El usuario modifica `ingresos_vs_gastos.py`, `margen_operativo.py`, `margen_2026.py` u otro script BI y quiere actualizar el informe
- El usuario pide crear un nuevo informe en una subcarpeta mensual (ej: `informe_cuentas_contables_2026_05/`)

---

## Paso 1 — Leer el script Python a documentar

Leer el archivo `.py` indicado. Extraer:

- **Conexión a la base de datos**: credenciales/variables usadas (`host`, `user`, `database` desde `tokens`)
- **Queries SQL**: tablas consultadas, filtros `WHERE`, joins, campos calculados, criterios de exclusión
- **Cuentas contables**: números de cuenta (`Numero`) y sus descripciones agrupadas por categoría (sueldos, cargas sociales, sindicato, egresos operativos)
- **Unidades de negocio**: cómo se clasifica cada registro (por `NroSucursal`, por campo, por cliente especial, etc.)
- **Fórmulas de distribución**: prorrateos o cálculos derivados (ej: cargas sociales distribuidas por proporción de sueldos)
- **Salida**: formato del archivo generado (Excel, hojas, columnas)

---

## Paso 2 — Mapear hallazgos a secciones LaTeX

Usar la estructura estándar del documento existente en `informe_cuentas_contables_2026_04/informe_cuentas_contables.tex` como referencia de estilo:

| Sección LaTeX | Contenido a extraer del script |
|---|---|
| Descripción General | Resumen del proceso (pasos numerados, herramientas) |
| Unidades de Negocio | Tabla de clasificación por sucursal/criterio |
| Ingresos | Query de ventas, fórmula de importe neto, criterios de exclusión |
| Egresos — Sueldos | Cuentas `213010xx`, asignación directa por UdN |
| Egresos — Cargas Sociales | Cuentas `213020xx`, fórmula de prorrateo |
| Egresos — Sindicato | Cuentas `213020x7-10`, fórmula de prorrateo |
| Cuentas Contables Bs.As. | Lista de cuentas `421010xx`, `115010xx` con descripción |
| Cuentas Contables Salta | Lista de cuentas `422010xx` con descripción |
| Consideraciones | Advertencias sobre activos incluidos como egresos, prorrateos, período de datos |

---

## Paso 3 — Actualizar o crear el archivo `.tex`

### Si el `.tex` ya existe (actualización)

- Leer el archivo completo antes de editar
- Actualizar solo las secciones que cambiaron respecto al script
- Mantener el preámbulo LaTeX, colores y estilo exactamente como está

### Si es un nuevo informe (nueva subcarpeta mensual)

- Crear la carpeta `informe_cuentas_contables_YYYY_MM/`
- Copiar el preámbulo estándar (ver referencia de estilo abajo)
- Completar todas las secciones con los datos del script

### Estilo obligatorio a respetar

```latex
\documentclass[12pt, a4paper]{article}
% Paquetes: inputenc, fontenc, babel(spanish), geometry, booktabs, array,
%           longtable, xcolor, colortbl, titlesec, parskip, lmodern,
%           fancyhdr, amsmath, microtype, hyperref
% Colores definidos: azuloscuro (31,73,125), azulclaro (189,215,238),
%                    grisclaro (242,242,242), verdeclaro (226,239,218)
% Tablas: usar \begin{tabular} para tablas cortas, \begin{longtable} para listas de cuentas
% Filas alternadas: \rowcolor{grisclaro} en filas impares
% Fórmulas: entorno \[ ... \] para fórmulas matemáticas de distribución
```

---

## Paso 4 — Compilar el PDF

Ejecutar la tarea VS Code correspondiente. Para la carpeta `informe_cuentas_contables_2026_04/`:

**Compilación inicial:**
```
pdflatex -interaction=nonstopmode -halt-on-error informe_cuentas_contables_2026_04/informe_cuentas_contables.tex
```

**Segunda compilación** (necesaria para resolver referencias internas, tabla de contenidos, etc.):
```
pdflatex -interaction=nonstopmode -halt-on-error informe_cuentas_contables_2026_04/informe_cuentas_contables.tex
```

Las tareas disponibles en el workspace son:
- `Compilar PDF informe_cuentas_contables_2026_04`
- `Recompilar PDF informe_cuentas_contables_2026_04`

Usar la herramienta `run_task` o `create_and_run_task` para ejecutarlas. Si la tarea no existe para una nueva carpeta mensual, crearla en `.vscode/tasks.json`.

---

## Paso 5 — Verificar resultado

- Confirmar que el PDF se generó sin errores (`pdflatex` exit code 0)
- Si hay errores de compilación, leer la salida del log y corregir el `.tex`
- Los archivos auxiliares (`.aux`, `.log`, `.out`) son normales y pueden ignorarse

---

## Referencia de estructura de tablas de cuentas contables

```latex
\begin{longtable}{>{\ttfamily}ll}
\toprule
\rowcolor{azulclaro}
\textbf{\normalfont Número} & \textbf{Concepto} \\
\midrule
\endfirsthead
\midrule
\rowcolor{azulclaro}
\textbf{\normalfont Número} & \textbf{Concepto} \\
\midrule
\endhead
\bottomrule
\endfoot
\rowcolor{verdeclaro}
& Ventas netas --- Bs.As. \\
21301001 & Sueldos y Jornales a Pagar Bs.As. \\
\rowcolor{grisclaro}
& Sindicato --- Bs.As. \textnormal{\small (prorrateo por sueldos)} \\
% ... resto de cuentas
\end{longtable}
```

---

## Archivos relevantes en el workspace

| Archivo | Descripción |
|---|---|
| `ingresos_vs_gastos.py` | Script principal: ventas + 4 categorías de egresos → Excel |
| `margen_operativo.py` | Variante de margen operativo |
| `margen_2026.py` | Versión 2026 del cálculo de margen |
| `informe_cuentas_contables_2026_04/informe_cuentas_contables.tex` | Documento LaTeX de referencia (abril 2026) |
