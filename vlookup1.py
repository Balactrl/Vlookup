import pandas as pd
import numpy as np
from decimal import Decimal, ROUND_HALF_UP
import streamlit as st
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower().replace("  ", " ") for c in df.columns]
    return df


def read_sheet(file, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(file, sheet_name=sheet_name)
    return normalize_columns(df)


def make_excel_download(df: pd.DataFrame, sheet_name: str) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buffer.seek(0)
    return buffer.read()


def compute_outputs(df1_raw: pd.DataFrame, df2_raw: pd.DataFrame):
    issues = []

    col_re = 're'
    col_sumtax = 'sumtax'            # excel1
    col_acx_total = 'acxtotalamount' # excel1
    col_total_amount = 'totalamount' # excel2
    col_total_tax = 'total tax'      # excel2

    def ensure_cols(df: pd.DataFrame, required: list[str], label: str):
        missing = [c for c in required if c not in df.columns]
        if missing:
            issues.append(f"{label} missing columns: {', '.join(missing)}")

    ensure_cols(df1_raw, [col_re, col_sumtax, col_acx_total], 'Excel1 Sheet A')
    ensure_cols(df2_raw, [col_re, col_total_amount, col_total_tax], 'Excel2 Sheet A')

    df1 = df1_raw[[c for c in [col_re, col_sumtax, col_acx_total] if c in df1_raw.columns]].copy()
    df2 = df2_raw[[c for c in [col_re, col_total_amount, col_total_tax] if c in df2_raw.columns]].copy()

    left_join = df1.merge(df2, on=col_re, how='left', suffixes=('', '_r'))

    out1 = pd.DataFrame()
    out1['re'] = left_join.get(col_re)
    out1['sumtax'] = left_join.get(col_sumtax)
    out1['acxtotalamount'] = left_join.get(col_acx_total)

    tax_diff_series = (left_join[col_sumtax] - left_join[col_total_tax]) if col_total_tax in left_join.columns else pd.Series([np.nan] * len(left_join))
    amt_diff_series = (left_join[col_acx_total] - left_join[col_total_amount]) if col_total_amount in left_join.columns else pd.Series([np.nan] * len(left_join))

    no_match_mask_left = left_join[col_total_amount].isna() & left_join[col_total_tax].isna()
    out1['difference in tax'] = tax_diff_series.where(~no_match_mask_left, other='No Data Found')
    out1['difference in amount'] = amt_diff_series.where(~no_match_mask_left, other='No Data Found')

    # Vectorized half-up rounding for numeric values: floor(x + 0.5)
    def round_half_up_vectorized(series: pd.Series) -> pd.Series:
        numeric = pd.to_numeric(series, errors='coerce')
        rounded = np.floor(numeric + 0.5)
        result = series.copy()
        mask = ~numeric.isna()
        result.loc[mask] = rounded[mask].astype('Int64')
        return result

    out1['difference in amount'] = round_half_up_vectorized(out1['difference in amount'])

    right_join = df2.merge(df1, on=col_re, how='left', suffixes=('', '_r'))

    out2 = pd.DataFrame()
    out2['re'] = right_join.get(col_re)
    out2['totaltax'] = right_join.get(col_total_tax)
    out2['totalamount'] = right_join.get(col_total_amount)

    tax_diff_series_r = (right_join[col_total_tax] - right_join[col_sumtax]) if col_sumtax in right_join.columns else pd.Series([np.nan] * len(right_join))
    amt_diff_series_r = (right_join[col_total_amount] - right_join[col_acx_total]) if col_acx_total in right_join.columns else pd.Series([np.nan] * len(right_join))

    no_match_mask_right = right_join[col_sumtax].isna() & right_join[col_acx_total].isna()
    out2['difference in tax'] = tax_diff_series_r.where(~no_match_mask_right, other='No Data Found')
    out2['difference in amount'] = amt_diff_series_r.where(~no_match_mask_right, other='No Data Found')
    out2['difference in amount'] = round_half_up_vectorized(out2['difference in amount'])

    return out1, out2, issues


st.set_page_config(page_title='VLOOKUP Sheets A/B/C (Streamlit)', layout='wide')
st.title('Compare Excel Files - Sheets A / B / C')

with st.sidebar:
    st.header('Upload Files')
    file1 = st.file_uploader('Excel 1 (Sheets A/B/C: re, sumtax, acxtotalamount)', type=['xlsx', 'xls'])
    file2 = st.file_uploader('Excel 2 (Sheets A/B/C: re, totalamount, total tax)', type=['xlsx', 'xls'])
    sheets = st.multiselect('Sheets to compare', options=['A', 'B', 'C'], default=['A', 'B', 'C'])
    run = st.button('Compare')

if run:
    if not file1 or not file2:
        st.error('Please upload both Excel files.')
    elif not sheets:
        st.warning('Select at least one sheet to compare.')
    else:
        try:
            # Read both workbooks once (all selected sheets)
            wb1 = pd.read_excel(file1, sheet_name=sheets)
            wb2 = pd.read_excel(file2, sheet_name=sheets)

            # Normalize all selected sheets
            for s in list(wb1.keys()):
                wb1[s] = normalize_columns(wb1[s])
            for s in list(wb2.keys()):
                wb2[s] = normalize_columns(wb2[s])

            outputs_by_sheet = {}

            # Parallel processing per sheet
            def process_sheet(sheet_name: str):
                df1 = wb1.get(sheet_name)
                df2 = wb2.get(sheet_name)
                if df1 is None or df2 is None:
                    return sheet_name, None, None, [f"Sheet {sheet_name} missing in one of the files"]
                out1, out2, issues = compute_outputs(df1, df2)
                return sheet_name, out1, out2, issues

            max_workers = min(4, len(sheets)) or 1
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(process_sheet, s): s for s in sheets}
                for future in as_completed(futures):
                    sheet = futures[future]
                    try:
                        s, out1, out2, issues = future.result()
                        if issues:
                            for msg in issues:
                                st.warning(f"{s}: {msg}")
                        if out1 is not None and out2 is not None:
                            outputs_by_sheet[s] = {'excel1': out1, 'excel2': out2}
                    except Exception as se:
                        st.error(f"Error processing sheet {sheet}: {se}")

            # Render results in the order selected
            for sheet in sheets:
                if sheet not in outputs_by_sheet:
                    continue
                out1 = outputs_by_sheet[sheet]['excel1']
                out2 = outputs_by_sheet[sheet]['excel2']

                st.markdown(f"### Sheet {sheet}")
                tabs = st.tabs([f"Excel 1 view ({sheet})", f"Excel 2 view ({sheet})"])
                with tabs[0]:
                    st.dataframe(out1, use_container_width=True)
                    data_a = make_excel_download(out1, sheet_name=f'{sheet}_output_1')
                    st.download_button(
                        label=f'Download Excel 1 Output ({sheet})',
                        data=data_a,
                        file_name=f'excel1_output_{sheet}.xlsx',
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )
                with tabs[1]:
                    st.dataframe(out2, use_container_width=True)
                    data_b = make_excel_download(out2, sheet_name=f'{sheet}_output_2')
                    st.download_button(
                        label=f'Download Excel 2 Output ({sheet})',
                        data=data_b,
                        file_name=f'excel2_output_{sheet}.xlsx',
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )

            # Combined download for all selected sheets
            if outputs_by_sheet:
                buffer = BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    for sheet in sheets:
                        if sheet in outputs_by_sheet:
                            outputs_by_sheet[sheet]['excel1'].to_excel(writer, index=False, sheet_name=f'{sheet}_excel1')
                            outputs_by_sheet[sheet]['excel2'].to_excel(writer, index=False, sheet_name=f'{sheet}_excel2')
                buffer.seek(0)

                st.download_button(
                    label='Download Combined Excel (all selected sheets)',
                    data=buffer.read(),
                    file_name='combined_outputs.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
        except Exception as e:
            st.error(f'Error processing files: {e}')
