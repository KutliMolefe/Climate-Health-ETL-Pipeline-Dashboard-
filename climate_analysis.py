

import pandas as pd
import sqlite3
from scipy.stats import pearsonr


df = pd.read_csv("Disease-Weather-Uganda.csv")


month_order = {
    "January": 1, "February": 2, "March": 3,
    "April": 4, "May": 5, "June": 6,
    "July": 7, "August": 8, "September": 9,
    "October": 10, "November": 11, "December": 12
}

df['MonthNumber'] = df['month'].map(month_order)


conn = sqlite3.connect('climate_disease.db')
cursor = conn.cursor()


create_table_query = '''
CREATE TABLE IF NOT EXISTS ClimateDisease (
    location INTEGER,
    month TEXT,
    disease TEXT,
    total INTEGER,
    preasure REAL,
    rain REAL,
    sun REAL,
    humidity REAL,
    mean_temp REAL,
    max_temp REAL,
    min_temp REAL,
    wind_gust REAL,
    mean_wind_spd REAL,
    MonthNumber INTEGER
);
'''
cursor.execute(create_table_query)
conn.commit()


df.to_sql('ClimateDisease', conn, if_exists='replace', index=False)
print("✅ Cleaned data inserted successfully.")


query_total_cases = '''
SELECT disease, SUM(total) AS total_cases
FROM ClimateDisease
GROUP BY disease
ORDER BY total_cases DESC;
'''
df_total_cases = pd.read_sql_query(query_total_cases, conn)
print("\n🔹 Total disease cases by type:\n", df_total_cases)


query_cases_month_location = '''
SELECT month, MonthNumber, location, SUM(total) AS total_cases
FROM ClimateDisease
GROUP BY month, MonthNumber, location
ORDER BY MonthNumber, location;
'''
df_cases_month_location = pd.read_sql_query(query_cases_month_location, conn)
print("\n🔹 Disease cases by month and location:\n", df_cases_month_location.head())


query_avg_weather = '''
SELECT disease,
       AVG(preasure) AS avg_pressure,
       AVG(rain) AS avg_rainfall,
       AVG(sun) AS avg_sunshine,
       AVG(humidity) AS avg_humidity,
       AVG(mean_temp) AS avg_mean_temp,
       AVG(max_temp) AS avg_max_temp,
       AVG(min_temp) AS avg_min_temp,
       AVG(wind_gust) AS avg_wind_gust,
       AVG(mean_wind_spd) AS avg_mean_wind_speed
FROM ClimateDisease
GROUP BY disease
ORDER BY disease;
'''
df_avg_weather = pd.read_sql_query(query_avg_weather, conn)
print("\n🔹 Average weather conditions per disease:\n", df_avg_weather.head())


weather_cols = ['preasure','rain','sun','humidity','mean_temp','max_temp','min_temp','wind_gust','mean_wind_spd']
target_col = 'total'


def correlation_pvalues(df, weather_cols, target_col):
    results = []
    for col in weather_cols:
        valid_data = df[[col, target_col]].dropna()
        if not valid_data.empty:
            corr_coef, p_value = pearsonr(valid_data[col], valid_data[target_col])
            results.append({
                'Feature': col,
                'Correlation': corr_coef,
                'P-value': p_value
            })
    return pd.DataFrame(results)

correlation_results = correlation_pvalues(df, weather_cols, target_col)
correlation_results['AbsCorrelation'] = correlation_results['Correlation'].abs()
correlation_results = correlation_results.sort_values(by='AbsCorrelation', ascending=False)

print("\n🔹 Correlation analysis results:\n", correlation_results[['Feature', 'Correlation', 'P-value']])


print("\nInterpretation:")
for _, row in correlation_results.iterrows():
    signif = "Significant" if row['P-value'] < 0.05 else "Not Significant"
    print(f"- {row['Feature']}: Corr={row['Correlation']:.2f}, p={row['P-value']:.4f} --> {signif}")


df_sorted = df.sort_values(by='MonthNumber')
df_sorted.to_csv("final_cleaned_dataset.csv", index=False)
print("\n✅ Final cleaned and sorted dataset exported to final_cleaned_dataset.csv")


conn.close()


