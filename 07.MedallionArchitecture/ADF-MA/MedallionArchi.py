import pandas as pd

# Brozne - Load raw CSV
bronze_df = pd.read_csv("./datasets/samplesuperstore.csv")
bronze_df.to_parquet("bronze/superstore_raw.parquet")  # store raw bronze layer


# Silver - Cleaning
silver_df = bronze_df.copy()
silver_df['Order Date'] = pd.to_datetime(silver_df['Order Date'])
silver_df['Ship Date'] = pd.to_datetime(silver_df['Ship Date'])
silver_df = silver_df.drop_duplicates()
silver_df.to_parquet("silver/superstore_clean.parquet")

# Gold - Curated BI
gold_df = silver_df.groupby(['Order Date','Category','Region']).agg({
    'Sales':'sum',
    'Profit':'sum',
    'Discount':'mean',
    'Quantity':'sum'
}).reset_index()

gold_df.to_parquet("gold/superstore_aggregated.parquet")