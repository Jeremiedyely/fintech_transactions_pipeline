import pandas as pd

df = pd.read_csv("C:/Users/jerem/fintech_transactions_pipeline/data/raw/fraudTrain.csv")

# Clean data — fix column names and split date and time.

df = df.drop(columns=["Unnamed: 0"])
df.columns = df.columns.str.strip().str.replace(" ", "_").str.lower()
df["trans_date_trans_time"] = pd.to_datetime(df["trans_date_trans_time"])
df["transaction_date"] = df["trans_date_trans_time"].dt.date
df["transaction_time"] = df["trans_date_trans_time"].dt.time
df = df.drop(columns=["trans_date_trans_time"])

# Rename columns for clarity and readability
df.rename(
    columns={
        "cc_num": "credit_card_number",
        "merchant": "merchant_name",
        "category": "merchant_category",
        "amt": "transaction_amount",
        "first": "first_name",
        "last": "last_name",
        "street": "street_address",
        "zip": "zip_code",
        "lat": "latitude",
        "long": "longitude",
        "city_pop": "city_population",
        "dob": "date_of_birth",
        "trans_num": "transaction_id",
        "unix_time": "unix_timestamp",
        "merch_lat": "merchant_latitude",
        "merch_long": "merchant_longitude",
    },
    inplace=True,
)
# Reorder columns for better organization
column_order = [
    "credit_card_number",
    "first_name",
    "last_name",
    "gender",
    "date_of_birth",
    "job",
    "street_address",
    "city",
    "state",
    "zip_code",
    "latitude",
    "longitude",
    "city_population",
    "merchant_name",
    "merchant_category",
    "merchant_latitude",
    "merchant_longitude",
    "transaction_id",
    "transaction_amount",
    "transaction_date",
    "transaction_time",
    "unix_timestamp",
    "is_fraud",
]

df = df[column_order]

# Clean + mask card numbers (preserve zeros; privacy-safe)
df["credit_card_number"] = df["credit_card_number"].astype(str)
df["credit_card_number"] = df["credit_card_number"].str.replace(
    r"\D", "", regex=True
)
df["valid_length"] = df["credit_card_number"].str.len().between(13, 16)
df["masked_card"] = df["credit_card_number"].apply(
    lambda s: "*" * max(len(s) - 4, 0) + s[-4:]
)
df["credit_card_number"] = df["masked_card"]
df = df.drop(columns=["masked_card"])

# Short summary (no full numbers printed)
valid_count = df["valid_length"].sum()
invalid_count = (~df["valid_length"]).sum()
total = len(df)

# Drop invalid credit card numbers — keep a copy of the old dataframe. Keep only valid cards
df_old = df.copy()
df = df[df["valid_length"]]
df = df.drop(columns=["valid_length"]) 

# Clean merchant column - Remove 'fraud_' prefix, and trim spaces

df["merchant_name"] = (
    df["merchant_name"]
    .astype(str)
    .str.replace(r"^fraud_", "", regex=True)
    .str.replace(r"[^\w\s,&-]", "", regex=True)
    .str.replace(r"\s{2,}", " ", regex=True)
    .str.strip()
    .str.title()
)

#make merchant_category values more readable  #PEP8 STOP HERE
df["merchant_category"] = (
    df["merchant_category"]
    .astype(str)
    .str.replace(r"^fraud_", "", regex=True)
    .str.replace("_", " ")   
    .str.replace(r"[^\w\s&-]", "", regex=True)
    .str.replace(r"\s{2,}", " ", regex=True)
    .str.strip()
    .str.title()
)

# Clean first_name and last_name columns
df["first_name"] = (
    df["first_name"]
    .astype(str)
    .str.replace(r"[^\w\s'-]", "", regex=True)  
    .str.replace(r"\s{2,}", " ", regex=True)    
    .str.strip()                                
    .str.title()                                
)

df["last_name"] = (
    df["last_name"]
    .astype(str)
    .str.replace(r"[^\w\s'-]", "", regex=True)
    .str.replace(r"\s{2,}", " ", regex=True)
    .str.strip()
    .str.title()
)

# Clean and normalize job titles
df["job"] = (
    df["job"]
    .astype(str)
    .str.replace(r"[^\w\s/&-]", "", regex=True)   
    .str.replace(r"\s{2,}", " ", regex=True)      
    .str.strip()                                  
    .str.title()                                  
)

# Clean street_address column
df["street_address"] = (
    df["street_address"]
    .astype(str)                                 
    .str.replace(r"[^\w\s\.-]", "", regex=True)  
    .str.replace(r"\s{2,}", " ", regex=True)     
    .str.strip()                                 
    .str.title()                                
)

# Clean city column — remove invalid characters, fix spacing, and standardize casing.
df["city"] = (
    df["city"]
    .astype(str)
    .str.replace(r"[^\w\s'-]", "", regex=True)      
    .str.replace(r"\s{2,}", " ", regex=True)        
    .str.strip()
    .str.title()
)

# Replace missing or empty city values with a consistent label.
df["city"] = df["city"].replace("", pd.NA).fillna("Unknown City")

df["state"] = (
    df["state"]
    .astype(str)
    .str.strip()
    .str.upper()
)

df["zip_code"] = (
    df["zip_code"]
    .astype(str)                                
    .str.replace(r"\D", "", regex=True)         
    .str.zfill(5)                               
)
df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
df.loc[(df["latitude"] < -90) | (df["latitude"] > 90), "latitude"] = pd.NA
df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
df.loc[(df["longitude"] < -180) | (df["longitude"] > 180), "longitude"] = pd.NA 

df["city_population"] = pd.to_numeric(df["city_population"], errors="coerce")
df["city_population"] = df["city_population"].astype("Int64")

df.loc[
    (df["city_population"] < 50) |               
    (df["city_population"] > 15_000_000),        
    "city_population"
] = pd.NA

df.loc[df["city"] == "Unknown City", "city_population"] = pd.NA

# Clean merchant_latitude column Convert to numeric and remove invalid geolocation values.

df["merchant_latitude"] = pd.to_numeric(df["merchant_latitude"], errors="coerce")

df.loc[
    (df["merchant_latitude"] < -90) |
    (df["merchant_latitude"] > 90),
    "merchant_latitude"
] = pd.NA

# Clean merchant_longitude column Convert to numeric and enforce valid longitude range.
df["merchant_longitude"] = pd.to_numeric(df["merchant_longitude"], errors="coerce")

df.loc[
    (df["merchant_longitude"] < -180) |
    (df["merchant_longitude"] > 180),
    "merchant_longitude"
] = pd.NA

# Clean transaction_id column Normalize text, remove invalid characters, ensure consistent key formatting.
df["transaction_id"] = (
    df["transaction_id"]
    .astype(str)
    .str.strip()
    .str.replace(r"[^\w-]", "", regex=True) 
)

df["transaction_id"] = df["transaction_id"].replace("", pd.NA)
df["transaction_id"] = df["transaction_id"].astype("string") #

# Clean transaction_amount column Convert to numeric, remove invalid values, and create BI-friendly buckets.
df["transaction_amount"] = pd.to_numeric(df["transaction_amount"], errors="coerce")
df.loc[
    (df["transaction_amount"] <= 0) |
    (df["transaction_amount"] > 50_000),
    "transaction_amount"
] = pd.NA

# Clean transaction_date column Convert to datetime format for BI time-series analysis.
df["transaction_date"] = pd.to_datetime(
    df["transaction_date"],
    format="%m/%d/%Y",
    errors="coerce"
)

# Clean transaction_time column Convert to Python time format and extract hour for BI analysis.
df["transaction_time"] = pd.to_datetime(
    df["transaction_time"],
    format="%H:%M:%S",
    errors="coerce"
).dt.time

# Clean unix_timestamp column Convert to numeric and validate range.
df["unix_timestamp"] = pd.to_numeric(df["unix_timestamp"], errors="coerce")

df.loc[
    (df["unix_timestamp"] < 946684800) |    # Jan 1, 2000
    (df["unix_timestamp"] > 1735689600),   # Jan 1, 2025
    "unix_timestamp",
] = pd.NA

# Convert unix_timestamp to precise datetime.Create human-readable unix_datetime from raw UNIX seconds.
df["unix_datetime"] = pd.to_datetime(
    df["unix_timestamp"],
    unit="s",
    errors="coerce",
)

# Sort rows by card and time before computing differences.Order transactions per card chronologically.
df = df.sort_values(["credit_card_number", "unix_datetime"])

# Calculate seconds since last transaction (per card).Compute time gaps between consecutive swipes for each card.
df["seconds_since_last_txn"] = (
    df.groupby("credit_card_number")["unix_datetime"]
    .diff()
    .dt.total_seconds()
)
df = df.drop(columns=["unix_datetime"])

# Clean gender column. Action: Standardize values to M, F, or Unknown.

df["gender"] = (
    df["gender"]
    .astype(str)
    .str.strip()
    .str.lower()
)

# Map common variants to standard categories
gender_map = {
    "m": "M",
    "male": "M",
    "f": "F",
    "female": "F",
}

df["gender"] = df["gender"].map(gender_map).fillna("Unknown")

# Clean is_fraud column. Convert to numeric 0/1 and enforce binary values.
df["is_fraud"] = pd.to_numeric(df["is_fraud"], errors="coerce")
df.loc[~df["is_fraud"].isin([0, 1]), "is_fraud"] = pd.NA
df["is_fraud"] = df["is_fraud"].astype("Int64")

df["city_population_missing"] = df["city_population"].isna()
df["transaction_amount_missing"] = df["transaction_amount"].isna()
df["latlong_missing"] = df["latitude"].isna() | df["longitude"].isna()
df["seconds_since_last_txn_missing"] = df["seconds_since_last_txn"].isna()

# Reorder columns for better organization
column_order = [
    'credit_card_number', 'first_name', 'last_name', 'gender', 'date_of_birth', 'job',
    'street_address', 'city', 'state', 'zip_code', 'latitude', 'longitude', 'city_population',
    'merchant_name', 'merchant_category', 'merchant_latitude', 'merchant_longitude',
    'transaction_id', 'transaction_amount', 'transaction_date', 'transaction_time',
    'unix_timestamp', 'seconds_since_last_txn',
    'is_fraud'
]
df = df[column_order]

# LOAD STEP — Save cleaned data
clean_path = "C:/Users/jerem/fintech_transactions_pipeline/data/processed/fraudTrain_cleaned.csv"
df.to_csv(clean_path, index=False)
print(f"Cleaned dataset saved to: {clean_path}")

# Save summary file
summary_path = "C:/Users/jerem/fintech_transactions_pipeline/data/outputs/clean_summary.csv"
df.describe(include="all").to_csv(summary_path)
print(f"Summary saved to: {summary_path}")




