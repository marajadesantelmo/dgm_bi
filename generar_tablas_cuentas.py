# -*- coding: utf-8 -*-
"""
Genera los fragmentos LaTeX con el inventario de cuentas con movimientos, por
unidad de negocio, para el informe (informe_cuentas_contables.pdf).

Entradas:
  - diagnostico/cuentas_movimientos.csv   (lo produce diagnostico_cuentas_movimientos.py en el servidor)
  - DGM - Cuentas Contables.xlsx          (plan de cuentas + columna Considerar Si/No)

Salidas (en informe_cuentas_contables_2026_04/):
  - tabla_cuentas_bsas.tex
  - tabla_cuentas_salta.tex

Se ejecuta en local:  python generar_tablas_cuentas.py
"""

import os
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(BASE, "diagnostico", "cuentas_movimientos.csv")
XLSX = os.path.join(BASE, "DGM - Cuentas Contables.xlsx")
OUTDIR = os.path.join(BASE, "informe_cuentas_contables_2026_04")


def num_str(x):
    try:
        return str(int(float(x)))
    except Exception:
        return str(x).strip()


def tex_escape(s):
    s = str(s)
    repl = {'\\': r'\textbackslash{}', '&': r'\&', '%': r'\%', '$': r'\$',
            '#': r'\#', '_': r'\_', '{': r'\{', '}': r'\}', '~': r'\textasciitilde{}',
            '^': r'\textasciicircum{}'}
    for k, v in repl.items():
        s = s.replace(k, v)
    return s


def fmt_millones(x):
    try:
        return f"{x/1e6:,.1f}".replace(",", ".")
    except Exception:
        return "-"


def fmt_mes(x):
    if pd.isna(x):
        return "-"
    return str(x)[:7]


def cargar():
    mov = pd.read_csv(CSV, encoding="utf-8-sig")
    mov["Num"] = mov["Numero"].apply(num_str)
    mov["Ultimo_Mov"] = pd.to_datetime(mov["Ultimo_Mov"], errors="coerce")
    # La DB puede tener varias descripciones para el mismo número de cuenta;
    # se colapsan por número (sumando movimientos/importes, fecha máxima).
    mov = mov.groupby("Num", as_index=False).agg(
        Movs_Total=("Movs_Total", "sum"),
        Movs_2024=("Movs_2024", "sum"),
        Asientos_Total=("Asientos_Total", "sum"),
        Ultimo_Mov=("Ultimo_Mov", "max"),
        Debe_2024=("Debe_2024", "sum"),
        Haber_2024=("Haber_2024", "sum"),
    )
    mov["Neto_2024"] = mov["Debe_2024"].fillna(0) - mov["Haber_2024"].fillna(0)

    cc = pd.read_excel(XLSX)
    cc.columns = ["Numero", "Nivel", "Tipo", "Descripcion", "FechaCreacion",
                  "FechaModificacion", "TipoSaldo", "Imputable", "_", "Considerar"]
    cc["Num"] = cc["Numero"].apply(num_str)
    cc["Considerar"] = cc["Considerar"].astype(str).str.upper().str.strip().map(
        lambda v: "Sí" if v == "SI" else "No")
    # Deduplicar el plan de cuentas por número, priorizando "Sí"
    cc = cc.sort_values("Considerar", ascending=False).drop_duplicates("Num", keep="first")

    # Sólo cuentas levantadas del Excel; nombre desde el plan de cuentas
    df = mov.merge(cc[["Num", "Descripcion", "Considerar", "Imputable"]],
                   on="Num", how="inner")
    # Sólo cuentas con movimientos en el período del informe...
    df = df[df["Movs_2024"].fillna(0) > 0].copy()
    # ...y marcadas "Si" en la columna Considerar (las que entran al margen)
    df = df[df["Considerar"] == "Sí"].copy()

    desc_up = df["Descripcion"].astype(str).str.upper()
    es_bsas = desc_up.str.contains("BSAS|BS AS", regex=True, na=False)
    es_salta = (desc_up.str.contains("PAT|SALTA", regex=True, na=False)) & (~es_bsas)
    df["Unidad"] = "Otras"   # sin identificador regional -> se omiten
    df.loc[es_bsas, "Unidad"] = "Bs.As."
    df.loc[es_salta, "Unidad"] = "Salta"
    return df


def limpiar_concepto(desc, unidad):
    d = str(desc).strip()
    # Quitar el prefijo regional para ganar espacio
    for pref in ("BSAS ", "BS AS ", "PAT ", "SALTA "):
        if d.upper().startswith(pref):
            d = d[len(pref):]
            break
    if len(d) > 52:
        d = d[:51].rstrip() + "…"
    return d.title()


def miles(x):
    return f"{int(x):,}".replace(",", ".")


def tabla_tex(df_u, titulo_unidad):
    df_u = df_u.sort_values("Num")
    tot_movs = int(df_u["Movs_2024"].sum())
    n = len(df_u)
    filas = []
    for _, r in df_u.iterrows():
        concepto = tex_escape(limpiar_concepto(r["Descripcion"], titulo_unidad))
        num = tex_escape(r["Num"])
        movs = miles(r["Movs_2024"])
        neto = fmt_millones(r["Neto_2024"])
        ult = fmt_mes(r["Ultimo_Mov"])
        filas.append(f"\\texttt{{{num}}} & {concepto} & {movs} & {neto} & {ult} \\\\")
    cuerpo = "\n".join(filas)
    head = (
        r"\rowcolor{azulclaro}" "\n"
        r"\textbf{Número} & \textbf{Concepto} & \textbf{Mov.} & \textbf{Neto \$M} & \textbf{Últ.} \\"
    )
    total_txt = (f"Total: {n} cuentas consideradas con movimientos y {miles(tot_movs)} "
                 r"movimientos desde 2024.")
    tex = f"""% Generado por generar_tablas_cuentas.py — NO editar a mano
{{\\footnotesize
\\begin{{center}}
\\begin{{longtable}}{{r p{{6.6cm}} r r c}}
\\toprule
{head}
\\midrule
\\endfirsthead
\\toprule
{head}
\\midrule
\\endhead
\\midrule
\\multicolumn{{5}}{{r}}{{\\emph{{continúa en la página siguiente}}}} \\\\
\\endfoot
\\bottomrule
\\endlastfoot
{cuerpo}
\\end{{longtable}}
\\end{{center}}
}}
\\vspace{{0.3em}}
\\noindent\\emph{{{total_txt}}}
"""
    return tex


def main():
    df = cargar()
    for unidad, fname in [("Bs.As.", "tabla_cuentas_bsas.tex"),
                          ("Salta", "tabla_cuentas_salta.tex")]:
        df_u = df[df["Unidad"] == unidad]
        tex = tabla_tex(df_u, unidad)
        with open(os.path.join(OUTDIR, fname), "w", encoding="utf-8") as f:
            f.write(tex)
        print(f"{unidad}: {len(df_u)} cuentas -> {fname}")
    # resumen
    print("\nResumen por unidad (cuentas con movs 2024):")
    print(df["Unidad"].value_counts().to_string())


if __name__ == "__main__":
    main()
