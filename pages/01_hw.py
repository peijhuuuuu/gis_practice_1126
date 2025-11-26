import solara
import duckdb
import pandas as pd
import leafmap.foliumap as leafmap

# 下載 CSV 並存到 DuckDB
url = "https://data.gishub.org/duckdb/cities.csv"
df = pd.read_csv(url)

con = duckdb.connect("cities.duckdb")
con.execute("CREATE TABLE IF NOT EXISTS cities AS SELECT * FROM df")
df = con.execute("SELECT * FROM cities").df()

@solara.component
def Page():
    # 選單：選國家
    country_options = df["country"].unique().tolist()
    country = solara.Select(  # 注意大寫 S
        label="Select country", options=country_options, value=country_options[0]
    )

    # 滑動尺標：人口範圍
    min_pop = int(df["population"].min())
    max_pop = int(df["population"].max())
    population_range = solara.Slider( # 注意大寫 S
        label="Population", min=min_pop, max=max_pop, value=(min_pop, max_pop)
    )

    # 篩選資料
    filtered_df = df[
        (df["country"] == country)
        & (df["population"] >= population_range[0])
        & (df["population"] <= population_range[1])
    ]

    # 顯示地圖
    m = leafmap.Map(center=(0, 0), zoom=3)
    for _, row in filtered_df.iterrows():
        m.add_marker(location=(row["lat"], row["lon"]), popup=row["name"])

    return m
