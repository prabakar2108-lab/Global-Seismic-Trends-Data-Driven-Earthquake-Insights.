import requests
import pandas as pd
from datetime import datetime
url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
all_records = []
start_year = datetime.now().year - 5
end_year = datetime.now().year
for year in range(start_year, end_year):
    for month in range(1, 13):
        start_date = f"{year}-{month:02d}-01"
        if month == 12:
            end_date = f"{year+1}-01-01"
        else:
            end_date = f"{year}-{month+1:02d}-01"
            
        params = {
             "format" : "geojson",
             "starttime": start_date,
              "endtime" : end_date,
             "minmagnitude" :3

         }
        response = requests.get(url,params=params)
        if response.status_code != 200:
            print(f" failed for {start_date} : {response.text[200]}")
            continue

        try:
            data = response.json()
        except Exception as e :
            continue
        for f in data.get("features",[]):
            p = f["properties"]
            g = f["geometry"]
            all_records.append({
                "id" : f.get("id"),
                "time" : pd.to_datetime(p.get("time"), unit = "ms"),
                "updated" : pd.to_datetime(p.get("updated"), unit = "ms"),
                "place" : p.get("place"),
                "mag" : p.get("mag"),
                "magtype" : p.get("magType"),
                "type" : p.get("type"),
                "latitude" : g["coordinates"][1] if g else None,
                "longitude" : g["coordinates"][0] if g else None,
                "depth_km" : g["coordinates"][2] if g else None,
                "status" : p.get("status"),
                "tsunami" : p.get("tsunami"),
                "alert" : p.get("alert"),
                "felt" : p.get("felt"),
                "cdi" : p.get("cdi"),
                "mmi" : p.get("mmi"),
                "sig" : p.get("sig"),
                "net" : p.get("net"),
                "code" : p.get("code"),
                "ids": p.get("ids"),
                "sources": p.get("sources"),
                "types": p.get("types"),
                "nst": p.get("nst"),
                "dmin": p.get("dmin"),
                "rms": p.get("rms"),
                "gap": p.get("gap"),
                "type": p.get("type")
            
            })
df = pd.DataFrame(all_records)     
df.to_csv("earthquake_data.csv", index=False)
            
import pandas as pd
df = pd.read_csv("C:/Users/prabh/earthquake_data.csv")
df['mmi'] = df['mmi'].fillna(df['mmi'].median())
df['cdi'] = df['cdi'].fillna(df['cdi'].median())
df['dmin'] = df['dmin'].fillna(df['dmin'].median())
df['rms'] = df['rms'].fillna(df['rms'].median())
df['alert'] = df['alert'].fillna(df['alert'].mode()[0])
df['nst'] = df['nst'].fillna(df['nst'].mode()[0])
df['felt'] = df['felt'].fillna(df['felt'].mode()[0])
df['gap'] = df['gap'].fillna(df['gap'].median())
df['rms'] = df['rms'].fillna(df['rms'].mean())
df.to_csv("earthquake_cleaneddata.csv", index = False)

import pandas as pd
from sqlalchemy import create_engine
csv_file = ("C:/Users/prabh/earthquake_cleaneddata.csv")
df = pd.read_csv(csv_file)

username = "root"
password = "12345"
host = "localhost"   
port = 3306
database = "earth_quake"
connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)
df['country'] = df['place'].apply(lambda x: x.split(',')[-1].strip() if ',' in str(x) else x)

df.to_sql("earthquake_data", con=engine, if_exists="replace", index=False)

df.to_sql("earthquakes", con=engine, if_exists="replace", index=False)

queries = {
    "Top 10 strongest earthquakes": """
        SELECT * FROM earthquake_data ORDER BY mag DESC LIMIT 10;
    """,
    "Top 10 deepest earthquakes": """
        SELECT * FROM earthquake_data ORDER BY depth_km DESC LIMIT 10;
    """,
    "Shallow earthquakes <50km and mag >7.5": """
        SELECT * FROM earthquake_data WHERE depth_km < 50 AND mag > 7.5;
    """,
    "Average magnitude per magType": """
        SELECT magType, AVG(mag) AS avg_mag FROM earthquake_data GROUP BY magType;
    """,
    "Year with most earthquakes": """
        SELECT YEAR(time) AS year, COUNT(*) AS quake_count 
        FROM earthquake_data GROUP BY year ORDER BY quake_count DESC LIMIT 1;
    """,
    "Month with highest number of earthquakes": """
        SELECT MONTH(time) AS month, COUNT(*) AS quake_count 
        FROM earthquake_data GROUP BY month ORDER BY quake_count DESC LIMIT 1;
    """,
    "Day of week with most earthquakes": """
        SELECT DAYNAME(time) AS day, COUNT(*) AS quake_count 
        FROM earthquake_data GROUP BY day ORDER BY quake_count DESC LIMIT 1;
    """,
    "Count of earthquakes per hour of day": """
        SELECT HOUR(time) AS hour, COUNT(*) AS quake_count 
        FROM earthquake_data GROUP BY hour ORDER BY quake_count DESC;
    """,
    "Most active reporting network": """
        SELECT net, COUNT(*) AS quake_count 
        FROM earthquake_data GROUP BY net ORDER BY quake_count DESC LIMIT 1;
    """,
    "Top 5 places with highest casualties": """
        SELECT place, SUM(sig) AS total_significance
          FROM earthquake_data
           GROUP BY place
           ORDER BY total_significance DESC
LIMIT 5;
    """,
    "Average economic loss by alert level": """
        SELECT alert, AVG(sig) AS avg_significance
        FROM earthquake_data
        GROUP BY alert;
    """,
    "Count of reviewed vs automatic earthquakes": """
        SELECT status, COUNT(*) AS count FROM earthquake_data GROUP BY status;
    """,
    "Count by earthquake type": """
        SELECT type, COUNT(*) AS count FROM earthquake_data GROUP BY type;
    """,
    "Number of earthquakes by data type": """
        SELECT types, COUNT(*) AS count FROM earthquake_data GROUP BY types;
    """,
    "Events with high station coverage (nst > 50)": """
        SELECT * FROM earthquake_data WHERE nst > 50;
    """,
    "Number of tsunamis triggered per year": """
        SELECT YEAR(time) AS year, COUNT(*) AS tsunami_count 
        FROM earthquake_data WHERE tsunami = 1 GROUP BY year ORDER BY year;
    """,
    "Count earthquakes by alert levels": """
        SELECT alert, COUNT(*) AS count FROM earthquake_data GROUP BY alert;
    """,
    "Top 5 countries with highest avg magnitude (past 10 years)": """
        SELECT country, AVG(mag) AS avg_mag 
        FROM earthquake_data 
        WHERE YEAR(time) >= YEAR(CURDATE()) - 10 
        GROUP BY country ORDER BY avg_mag DESC LIMIT 5;
    """,
    "Countries with shallow & deep quakes same month": """
        SELECT country, YEAR(time) AS year, MONTH(time) AS month
        FROM earthquake_data
        GROUP BY country, year, month
        HAVING SUM(CASE WHEN depth_km < 70 THEN 1 ELSE 0 END) > 0
           AND SUM(CASE WHEN depth_km > 300 THEN 1 ELSE 0 END) > 0;
    """,
    "Year-over-year growth rate in total earthquakes": """
        SELECT year, quake_count,
               (quake_count - LAG(quake_count) OVER (ORDER BY year)) / LAG(quake_count) OVER (ORDER BY year) * 100 AS growth_rate
        FROM (
            SELECT YEAR(time) AS year, COUNT(*) AS quake_count
            FROM earthquake_data GROUP BY year
        ) t;
    """,
    "3 most seismically active regions": """
        SELECT place, COUNT(*) AS quake_count, AVG(mag) AS avg_mag
        FROM earthquake_data GROUP BY place
        ORDER BY quake_count DESC, avg_mag DESC LIMIT 3;
    """,
    "Average depth per country within ±5° latitude": """
        SELECT country, AVG(depth_km) AS avg_depth
        FROM earthquake_data
        WHERE latitude BETWEEN -5 AND 5
        GROUP BY country;
    """,
    "Countries with highest shallow/deep ratio": """
        SELECT country,
               SUM(CASE WHEN depth_km < 70 THEN 1 ELSE 0 END) / SUM(CASE WHEN depth_km >= 300 THEN 1 ELSE 0 END) AS shallow_deep_ratio
        FROM earthquake_data GROUP BY country ORDER BY shallow_deep_ratio DESC;
    """,
    "Average magnitude difference (tsunami vs non-tsunami)": """
        SELECT 
            (AVG(CASE WHEN tsunami = 1 THEN mag END) - AVG(CASE WHEN tsunami = 0 THEN mag END)) AS mag_diff
        FROM earthquake_data;
    """,
    "Events with lowest data reliability (gap & rms)": """
        SELECT * FROM earthquake_data ORDER BY gap DESC, rms DESC LIMIT 10;
    """,
    "Regions with highest frequency of deep-focus earthquakes": """
        SELECT place, COUNT(*) AS deep_quake_count
        FROM earthquake_data
        WHERE depth_km > 300
        GROUP BY place ORDER BY deep_quake_count DESC LIMIT 10;
    """
}

df['country'] = df['place'].apply(lambda x: x.split(',')[-1].strip() if ',' in str(x) else x)

with engine.connect() as conn:
    for title, sql in queries.items():
        result = pd.read_sql(sql, conn)
        
    import streamlit as st
import pandas as pd
import sqlite3   

df = pd.read_csv("C:/Users/prabh/earthquake_cleaneddata.csv")
df["time"] = pd.to_datetime(df["time"], errors="coerce")
df["country"] = df["place"].str.split(",").str[-1].str.strip()

conn = sqlite3.connect(":memory:")
df.to_sql("earthquakes", conn, index=False, if_exists="replace")

st.title("Earthquake Analytics Dashboard")

query_map = {
    "Top 10 strongest earthquakes":
        "SELECT * FROM earthquakes ORDER BY mag DESC LIMIT 10",

    "Top 10 deepest earthquakes":
        "SELECT * FROM earthquakes ORDER BY depth_km DESC LIMIT 10",

     "Shallow earthquakes < 50 km and mag > 7.5" :
   
    "SELECT * FROM earthquakes WHERE depth_km < 50 AND mag > 7.5",

    "Average magnitude per magnitude type":
        "SELECT magtype, AVG(mag) AS avg_mag FROM earthquakes GROUP BY magtype",

    "Year with most earthquakes":
        "SELECT strftime('%Y', time) AS year, COUNT(*) AS quake_count "
        "FROM earthquakes GROUP BY year ORDER BY quake_count DESC LIMIT 1",

    "Month with highest number of earthquakes":
        "SELECT strftime('%m', time) AS month, COUNT(*) AS quake_count "
        "FROM earthquakes GROUP BY month ORDER BY quake_count DESC LIMIT 1",

    "Day of week with most earthquakes":
        "SELECT strftime('%w', time) AS day, COUNT(*) AS quake_count "
        "FROM earthquakes GROUP BY day ORDER BY quake_count DESC LIMIT 1",

    "Count of earthquakes per hour of day":
        "SELECT strftime('%H', time) AS hour, COUNT(*) AS quake_count "
        "FROM earthquakes GROUP BY hour ORDER BY quake_count DESC",

    "Most active reporting network":
        "SELECT net, COUNT(*) AS quake_count FROM earthquakes GROUP BY net "
        "ORDER BY quake_count DESC LIMIT 1",

    "Top 5 places with highest significance":
        "SELECT place, SUM(sig) AS total_significance FROM earthquakes "
        "GROUP BY place ORDER BY total_significance DESC LIMIT 5",

    "Average significance by alert":
        "SELECT alert, AVG(sig) AS avg_significance FROM earthquakes GROUP BY alert",

    "Count of reviewed vs automatic earthquakes":
        "SELECT status, COUNT(*) AS count FROM earthquakes GROUP BY status",

    "Count by earthquake type":
        "SELECT type, COUNT(*) AS count FROM earthquakes GROUP BY type",

    "Number of earthquakes by data type":
        "SELECT types, COUNT(*) AS count FROM earthquakes GROUP BY types",

    "Events with high station coverage (nst > 50)":
        "SELECT * FROM earthquakes WHERE nst > 50",

    "Number of tsunamis triggered per year":
        "SELECT strftime('%Y', time) AS year, COUNT(*) AS tsunami_count "
        "FROM earthquakes WHERE tsunami = 1 GROUP BY year",

    "Count earthquakes by alert levels":
        "SELECT alert, COUNT(*) AS count FROM earthquakes GROUP BY alert",

    "Top 5 countries with highest avg magnitude (past 10 years)":
        "SELECT country, AVG(mag) AS avg_mag FROM earthquakes "
        "WHERE strftime('%Y', time) >= strftime('%Y','now','-10 years') "
        "GROUP BY country ORDER BY avg_mag DESC LIMIT 5",

    "Countries with both shallow and deep earthquakes in same month":
        "SELECT country, strftime('%Y', time) AS year, strftime('%m', time) AS month "
        "FROM earthquakes GROUP BY country, year, month "
        "HAVING SUM(CASE WHEN depth_km < 70 THEN 1 ELSE 0 END) > 0 "
        "AND SUM(CASE WHEN depth_km > 300 THEN 1 ELSE 0 END) > 0",

    "Year-over-year growth rate in total earthquakes":
        "WITH yearly AS (SELECT strftime('%Y', time) AS year, COUNT(*) AS quake_count "
        "FROM earthquakes GROUP BY year) "
        "SELECT year, quake_count, "
        "ROUND(((quake_count - LAG(quake_count) OVER (ORDER BY year)) * 100.0 / LAG(quake_count) OVER (ORDER BY year)),2) AS growth_rate "
        "FROM yearly",

    "3 most seismically active regions":
        "SELECT place, COUNT(mag) AS quake_count, AVG(mag) AS avg_mag "
        "FROM earthquakes GROUP BY place ORDER BY quake_count DESC, avg_mag DESC LIMIT 3",

    "Average depth per country within ±5° latitude of equator":
        "SELECT country, AVG(depth_km) AS avg_depth FROM earthquakes "
        "WHERE latitude BETWEEN -5 AND 5 GROUP BY country",

    "Countries with highest shallow-to-deep ratio":
        "SELECT country, "
        "CAST(SUM(CASE WHEN depth_km < 70 THEN 1 ELSE 0 END) AS FLOAT) / "
        "NULLIF(SUM(CASE WHEN depth_km >= 300 THEN 1 ELSE 0 END),0) AS shallow_deep_ratio "
        "FROM earthquakes GROUP BY country HAVING shallow_deep_ratio IS NOT NULL "
        "ORDER BY shallow_deep_ratio DESC",

    "Average magnitude difference (tsunami vs non-tsunami)":
        "SELECT (AVG(CASE WHEN tsunami = 1 THEN mag END) - AVG(CASE WHEN tsunami = 0 THEN mag END)) AS mag_diff "
        "FROM earthquakes",

    "Events with lowest data reliability (highest gap & rms)":
        "SELECT * FROM earthquakes ORDER BY gap DESC, rms DESC LIMIT 10",

    "Regions with highest frequency of deep-focus earthquakes (>300 km)":
        "SELECT place, COUNT(*) AS deep_quake_count FROM earthquakes "
        "WHERE depth_km > 300 GROUP BY place ORDER BY deep_quake_count DESC LIMIT 10"
}

# Query box
selected_query = st.selectbox("Choose a query:", list(query_map.keys()))
sql_query = query_map[selected_query]

# Run SQL query
result = pd.read_sql(sql_query, conn)

st.subheader(f"Results: {selected_query}")
st.dataframe(result)

