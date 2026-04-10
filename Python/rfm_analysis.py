import pandas as pd
import os

# --- Data Loading ---
file_path = 'customer_segments.csv'

if not os.path.exists(file_path):
    print(f"Error: The file '{file_path}' was not found.")
    print("Please ensure the 'customer_segments.csv' file is in the correct location.")
    exit()
else:
    df = pd.read_csv(file_path)
    print("DataFrame head:")
    print(df.head())
    print("\nDataFrame info:")
    df.info()

# --- Data Cleaning and Preparation ---
# Check for missing values
print("\nMissing values before cleaning:")
print(df.isna().sum())

# Drop rows with any missing values in relevant columns for RFM analysis
# Found 'recency' is the only column that might have NaNs
df_clean = df.dropna(subset=['recency']).copy()

print("\nMissing values after cleaning:")
print(df_clean.isna().sum())

# Create the RFM DataFrame
rfm = df_clean[['recency', 'frequency', 'monetary']]
print("\nRFM DataFrame head:")
print(rfm.head())

# Save the cleaned data and RFM features for the next step
# This makes the data available for the K-Means clustering script
df_clean.to_csv('cleaned_customer_data.csv', index=False)
rfm.to_csv('rfm_features.csv', index=False)

print("\nRFM data preparation complete. 'cleaned_customer_data.csv' and 'rfm_features.csv' created.")
