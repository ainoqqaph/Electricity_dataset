import os
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine, types

# --- 1. Parse Weather XML Files ---
def parse_weather_xml(folder_path='.'):
    ns = {'cwa': 'urn:cwa:gov:tw:cwacommon:0.1'}
    # Mapping Chinese names to English for internal consistency
    station_map = {
        '\u81fa\u5357': 'Tainan',
        '\u81fa\u4e2d': 'Taichung',
        '\u65b0\u7af9': 'Hsinchu'
    }
    weather_data = []

    search_pattern = os.path.join(folder_path, '**', 'mn_Report_*.xml')
    xml_files = glob.glob(search_pattern, recursive=True)
    print(f"Status: Found {len(xml_files)} XML files.")
    
    for file in xml_files:
        month_str = os.path.basename(file).replace('mn_Report_', '').replace('.xml', '')
        try:
            tree = ET.parse(file)
            root = tree.getroot()
            for loc in root.findall('.//cwa:location', ns):
                raw_name = loc.find('.//cwa:station/cwa:StationName', ns).text
                if raw_name in station_map:
                    eng_name = station_map[raw_name]
                    stats = loc.find('cwa:stationObsStatistics', ns)
                    if stats is not None:
                        temp_elem = stats.find('.//cwa:AirTemperature/cwa:monthly/cwa:Mean', ns)
                        sun_elem = stats.find('.//cwa:SunshineDuration/cwa:monthly/cwa:Total', ns)
                        rain_elem = stats.find('.//cwa:Precipitation/cwa:monthly/cwa:Accumulation', ns)
                        
                        weather_data.append({
                            'YearMonth': month_str,
                            'StationName': eng_name,
                            'MeanTemp': float(temp_elem.text) if temp_elem is not None and temp_elem.text not in ['X', 'T', 'V'] else None,
                            'SunshineHrs': float(sun_elem.text) if sun_elem is not None and sun_elem.text not in ['X', 'T', 'V'] else None,
                            'Precipitation': float(rain_elem.text) if rain_elem is not None and rain_elem.text not in ['X', 'T', 'V'] else 0.0
                        })
        except Exception:
            continue
            
    return pd.DataFrame(weather_data).dropna()

def parse_power_csv(folder_path='.'):
    all_regional = []
    
    target_districts = [
        '\u65b0\u5e02', '\u5b89\u5b9a', '\u65b0\u7af9\u5e02', 
        '\u5bf6\u5c71', '\u897f\u5c6f\u5340'
    ]
    
    search_pattern = os.path.join(folder_path, '**', '*\u9109\u93ae\u5e02(\u90f5\u905e\u5340)\u5225\u7528\u96fb\u7d71\u8a08\u8cc7\u6599*.csv')
    csv_files = glob.glob(search_pattern, recursive=True)
    print(f"Status: Found {len(csv_files)} CSV files.")
    
    for file in csv_files:
        df = None
        for enc in ['utf-8-sig', 'cp950']:
            try:
                df = pd.read_csv(file, encoding=enc, dtype=str)
                break
            except Exception:
                continue

        if df is not None:
            col_map = {}
            for col in df.columns:
                if '\u884c\u653f\u5340' in col: col_map[col] = 'District'
                if '\u552e\u96fb\u5ea6' in col and '\u7d2f\u8a08' not in col: 
                    col_map[col] = 'Power_kWh'
                if '\u9805\u76ee' in col or '\u7528\u96fb\u7a2e\u985e' in col: col_map[col] = 'UsageType'
                if '\u5e74\u5ea6' in col: col_map[col] = 'Year_ROC'
                if '\u6708\u4efd' in col: col_map[col] = 'Month'
            
            df = df.rename(columns=col_map)
            
            if 'Year_ROC' in df.columns and 'Month' in df.columns and 'Power_kWh' in df.columns:
                df['YearMonth'] = (df['Year_ROC'].astype(int) + 1911).astype(str) + df['Month'].astype(str).str.zfill(2)
                
                mask = df['District'].isin(target_districts) & df['UsageType'].str.contains('\u9ad8\u58d3', na=False)
                df_filtered = df[mask].copy()
                df_filtered = df_filtered[~df_filtered['UsageType'].str.contains('\u5c0f\u8a08')]
                
                power_series = df_filtered['Power_kWh']
                if isinstance(power_series, pd.DataFrame):
                    power_series = power_series.iloc[:, 0]
                
                df_filtered.loc[:, 'Power_kWh_Numeric'] = pd.to_numeric(power_series.str.replace(',', ''), errors='coerce')
                
                grouped = df_filtered.groupby(['YearMonth', 'District'])['Power_kWh_Numeric'].sum().reset_index()
                grouped['PowerUsage_100M_kWh'] = round(grouped['Power_kWh_Numeric'] / 1e8, 4)
                
                all_regional.append(grouped[['YearMonth', 'District', 'PowerUsage_100M_kWh']])
            
    if all_regional:
        return pd.concat(all_regional, ignore_index=True)
    return pd.DataFrame()


def load_to_azure_sql(df_weather, df_power):
    server = 'noqqaph.database.windows.net'
    database = 'free-sql-db-0971966'
    username = 'noqqaph'
    password = 'no._.qq511050' 
    
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no&Connection+Timeout=30"
    engine = create_engine(conn_str)
    
    
    weather_dtypes = {
        'YearMonth': types.VARCHAR(6),
        'StationName': types.NVARCHAR(50),
        'MeanTemp': types.FLOAT,
        'SunshineHrs': types.FLOAT,
        'Precipitation': types.FLOAT
    }
    
    power_dtypes = {
        'YearMonth': types.VARCHAR(6),
        'District': types.NVARCHAR(50),
        'PowerUsage_100M_kWh': types.FLOAT
    }
    
    try:
        print("Progress: Uploading weather data (ClimateData)...")
        df_weather.to_sql('ClimateData', con=engine, if_exists='replace', index=False, dtype=weather_dtypes)
        
        print("Progress: Uploading power data (TechHubPower)...")
        df_power.to_sql('TechHubPower', con=engine, if_exists='replace', index=False, dtype=power_dtypes)
        print("Success: All data uploaded to Azure SQL with Unicode support.")
    except Exception as e:
        print(f"Error: Database upload failed. {e}")

if __name__ == "__main__":
    print("ETL Job Started.")
    data_folder = r"C:\Electricity_dataset" 
    
    df_w = parse_weather_xml(folder_path=data_folder)
    df_p = parse_power_csv(folder_path=data_folder)
    
    print(f"Stats: Extracted {len(df_w)} weather records.")
    print(f"Stats: Extracted {len(df_p)} power records.")
    
    if not df_w.empty and not df_p.empty:
        load_to_azure_sql(df_w, df_p)
    else:
        print("Abort: No data found.")