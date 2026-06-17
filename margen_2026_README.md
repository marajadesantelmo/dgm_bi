# margen_2026 — Informe de Margen Operativo DGM

## Descripción

`margen_2026.py` extrae datos del sistema contable de DGM (base MySQL) y genera un informe de
Pérdidas y Ganancias (PyG) mensual por unidad de negocio, exportado a `margen_2026.xlsx`.
El período cubierto va desde enero 2024 hasta el mes en curso.

---

## Fuentes de datos

| Fuente | Descripción |
|--------|-------------|
| MySQL — `facturasitems` / `facturas` | Ventas netas por sucursal |
| MySQL — `asientos` / `asientositems` | Sueldos, cargas sociales y sindicato |
| `DGM - Cuentas Contables.xlsx` | Maestro de cuentas contables (filtra qué cuentas incluir) |

Las dos **Unidades de Negocio** son:
- **Bs.As.** — sucursales 3 y 8 (facturación) y cuentas con "BSAS" en descripción (egresos)
- **Salta (PAT/Patogénicos)** — sucursal 4 (facturación) y cuentas con "PAT" en descripción (egresos)

---

## Estructura del archivo de salida

### Sheets de resumen: `Bs.As.` y `Salta`

Tabla pivoteada con las siguientes columnas:

| Columna | Descripción |
|---------|-------------|
| `Grupo` | `1.Ventas`, `2.RRHH`, `3.Egresos` |
| `Concepto` | Nombre de cuenta/categoría (con prefijo de ordenamiento `00-`, `01-`, `02-`) |
| `YYYY-MM` | Importe mensual en ARS (una columna por mes, desde `2024-01`) |

Filas: ~57 conceptos para Bs.As. / ~54 para Salta.

### Sheets de movimientos: `Bs.As. - movimientos` y `Salta - movimientos`

Detalle transaccional con las siguientes columnas:

| Columna | Descripción |
|---------|-------------|
| `Grupo` | Grupo al que pertenece (1.Ventas, 2.RRHH, 3.Egresos) |
| `Unidad de Negocios` | Bs.As. o Salta |
| `Fecha` | Fecha del comprobante |
| `Mes` | Período `YYYY-MM` |
| `Concepto` | Nombre de cuenta (igual que en el resumen) |
| `Numero` | Número de cuenta contable |
| `Importe` | Monto en ARS |
| `RazonSocial` | Cliente (ventas) / Proveedor (egresos) / Empleado (RRHH) |
| `Origen` | Tipo de transacción fuente (Ventas, Sueldos, Compras) |
| `Detalle` | Referencia del comprobante (número de factura de compra, etc.) |
| `TipoOrigen` | Subtipo (Recibo, ComprobanteFondo, Pago, Deposito, etc.) |

---

## Resumen de métricas clave

### Buenos Aires

| Período | Ventas | RRHH | Egresos | Margen |
|---------|--------|------|---------|--------|
| 2024 | $1.373M | $575M | $555M | $242M (17,6%) |
| 2025 | $1.714M | $978M | $678M | $59M (3,4%) |
| 2026 (ene–jun) | $997M | $576M | $375M | $46M (4,6%) |

### Salta

| Período | Ventas | RRHH | Egresos | Margen |
|---------|--------|------|---------|--------|
| 2024 | $2.368M | $520M | $494M | $1.354M (57,2%) |
| 2025 | $4.616M | $987M | $1.203M | $2.426M (52,6%) |
| 2026 (ene–jun) | $2.816M | $652M | $990M | $1.173M (41,7%) |

*Montos en miles de ARS.*

---

## Eventos de datos destacados

Los siguientes valores son **correctos y representan hechos reales del negocio**, no errores:

- **Salta — diciembre 2024 (~$1.150M en ventas)**: Pago extraordinario del Ministerio de Salud
  Pública de la Pcia. de Salta. Se abonaron múltiples facturas acumuladas en el mismo mes.
  Este evento explica el pico atípico en el año 2024 de Salta.

- **Salta — julio 2025 (RRHH = $0)**: No hubo liquidación de sueldos registrada en ese período.
  El importe de RRHH es cero por omisión en la corrida de sueldos, no un error de extracción.

- **Junio 2026 (montos bajos)**: La extracción se realizó a mediados del mes; los importes
  son parciales y no representan el total del período.

- **Meses con margen negativo** (ej. Bs.As. mayo 2025, agosto 2025): Responden a actualizaciones
  paritarias (ajustes de convenio colectivo) que elevan el RRHH por encima de las ventas del mes
  puntual. Es un comportamiento estacional normal en el contexto laboral argentino.

---

## Notas de interpretación

- **Inflación argentina**: Los montos crecen año a año principalmente por inflación
  (~120–180% anual en 2024). Las comparaciones interanuales deben realizarse en términos reales.

- **Margen operativo**: Se calcula como `Ventas − RRHH − Egresos`. No incluye impuestos a las
  ganancias, amortizaciones ni resultados financieros.

- **Asignación de RRHH**: Las cargas sociales y el sindicato se distribuyen entre unidades
  de forma proporcional a la participación de cada unidad en el total de sueldos del mes.

- **Salta vs. Bs.As.**: Salta presenta márgenes porcentuales consistentemente más altos,
  explicado por su modelo de negocio (tratamiento de residuos patogénicos con contratos
  gubernamentales de largo plazo y menor base de costos variables).

---

## Cómo correr el script

```bash
# Requiere acceso a la base MySQL del servidor de DGM y el archivo DGM - Cuentas Contables.xlsx
python margen_2026.py
```

El script genera `margen_2026.xlsx` en el directorio de trabajo.

Para visualizar el dashboard interactivo:

```bash
streamlit run margen_dashboard.py
```
