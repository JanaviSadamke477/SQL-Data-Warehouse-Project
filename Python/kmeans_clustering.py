import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import numpy as np

# Load prepared RFM data
try:
    rfm = pd.read_csv('rfm_features.csv')
    df_clean = pd.read_csv('cleaned_customer_data.csv')
except FileNotFoundError:
    print("Error: 'rfm_features.csv' or 'cleaned_customer_data.csv' not found.")
    print("Please ensure the RFM data preparation script has been run successfully in the same directory.")
    exit() # Exit if crucial data is missing

# Feature Scaling
scaler = StandardScaler()
rfm_scaled = scaler.fit_transform(rfm)
print("Scaled RFM data head (first 5 rows):\n", rfm_scaled[:5])

# Verify no NaNs after scaling
print("\nNaNs in scaled RFM data:", np.isnan(rfm_scaled).sum())

# Elbow Method to determine optimal K
wcss = []
# It's good practice to ensure KMeans uses the correct number of features
# For consistency, we'll iterate from 2 to 8 clusters (9 non-inclusive)
for k in range(2, 9):
    # n_init is set to 'auto' or an integer value (e.g., 10) for modern sklearn versions
    kmeans = KMeans(n_clusters=k, random_state=42, n_init='auto')
    kmeans.fit(rfm_scaled)
    wcss.append(kmeans.inertia_)

# Plotting the Elbow Method results
plt.figure(figsize=(8, 5))
plt.plot(range(2, 9), wcss, marker='o')
plt.xlabel('Number of clusters (K)')
plt.ylabel('Within-Cluster Sum of Squares (WCSS)')
plt.title('Elbow Method for Optimal K')
plt.grid(True)
plt.show()

# K-Means Clustering
# Based on the elbow plot, assume 5 clusters is optimal
n_clusters = 5
print(f"\nProceeding with K-Means clustering using {n_clusters} clusters.")
kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init='auto')
# Ensure 'cluster' column is added to df_clean, which is a copy
df_clean.loc[:, 'cluster'] = kmeans.fit_predict(rfm_scaled)

# Cluster Analysis and Segmentation
print("\nCluster counts:")
print(df_clean['cluster'].value_counts().sort_index())

# Profile the clusters
cluster_profile = (
    df_clean
    .groupby('cluster')[['recency', 'frequency', 'monetary']]
    .mean()
    .round(2)
)
print("\nCluster RFM Profile (mean values for each cluster):")
print(cluster_profile)

# Map clusters to meaningful segments
segment_map = {
    0: 'High-Value Occasional Buyers',   # Example: High Monetary, Lower Frequency
    1: 'Loyal Customers',                 # Example: Good Recency, Good Frequency, Mid Monetary
    2: 'Low-Value Regulars',               # Example: Low Monetary, Very High Frequency (frequent but small purchases)
    3: 'At Risk Low-Value',                # Example: High Recency (not purchased recently), Low Monetary
    4: 'Churned High-Value'                # Example: Very High Recency, High Monetary (purchased a lot in the past)
}
df_clean.loc[:, 'segment'] = df_clean['cluster'].map(segment_map)

print("\nSegment counts after mapping:")
print(df_clean['segment'].value_counts())

# Final segment profile
final_segment_profile = df_clean.groupby('segment')[['recency', 'frequency', 'monetary']].mean().round(2)
print("\nFinal Segment RFM Profile (mean values for each named segment):")
print(final_segment_profile)

# Save Final Results
final_df = df_clean[['customer_key','recency','frequency','monetary','segment']]
final_df.to_csv('customer_segments_clustered.csv', index=False, header=True)
print("\nCustomer segments with assigned names saved to 'customer_segments_clustered.csv'.")
