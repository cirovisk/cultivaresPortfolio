import pandas as pd
import logging

log = logging.getLogger(__name__)

def clean_inmet(dataframes: dict) -> pd.DataFrame:
    all_dfs = []
    for sid, df in dataframes.items():
        if df.empty: continue
        
        # Limpeza inicial
        df = df.copy()
        # Converter colunas numéricas (INMET retorna strings no JSON)
        num_cols = ["CHUVA", "TEM_MAX", "TEM_MIN", "TEM_INS", "UMD_INS"]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Data/Hora
        if "DT_MEDICAO" in df.columns:
            # INMET usa DT_MEDICAO YYYY-MM-DD e HR_MEDICAO HHMM
            df["dt_hora"] = pd.to_datetime(df["DT_MEDICAO"] + " " + df["HR_MEDICAO"].str.zfill(4), format="%Y-%m-%d %H%M", errors="coerce")
            df = df.dropna(subset=["dt_hora"])
            
            # Agregação Diária
            daily = df.groupby(df["dt_hora"].dt.date).agg(
                precipitacao_total_mm=("CHUVA", "sum"),
                temp_max_c=("TEM_MAX", "max"),
                temp_min_c=("TEM_MIN", "min"),
                temp_media_c=("TEM_INS", "mean"),
                umidade_media=("UMD_INS", "mean")
            ).reset_index()
            
            daily = daily.rename(columns={"dt_hora": "data"})
            daily["estacao_id"] = sid
            all_dfs.append(daily)

    if not all_dfs: return pd.DataFrame()
    
    final_df = pd.concat(all_dfs, ignore_index=True)
    # Converter para datetime tipo data
    final_df["data"] = pd.to_datetime(final_df["data"])
    return final_df
