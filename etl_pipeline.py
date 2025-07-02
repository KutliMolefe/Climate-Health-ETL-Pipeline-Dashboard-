import pandas as pd


df = pd.read_csv("Disease-Weather-Uganda.csv")



print("\nData summary:")
print(df.info())

print("\nMissing values per column:")
print(df.isnull().sum())


df_clean = df.dropna()

columns_needed = [
    'location', 'month', 'disease', 'total',
    'preasure', 'rain', 'sun', 'humidity',
    'mean_temp', 'max_temp', 'min_temp',
    'wind_gust', 'mean_wind_spd'
]

df_final = df_clean[columns_needed]




df_final.to_csv("Disease-Weather-Uganda.csv", index=False)