import pandas as pd
import plotly.express as px
import streamlit as st
from databricks import sql
import os
import numpy as np

# Load ZIP coordinate lookup from uscities Excel file
zip_lookup_raw = pd.read_excel("uscities.xlsx")

# Melt ZIP columns into long format
zip_columns = zip_lookup_raw.columns[7:]  # All columns from 8th onward
zip_lookup = zip_lookup_raw.melt(
    id_vars=["lat", "lng"],
    value_vars=zip_columns,
    var_name="zip_source_col",
    value_name="zipcode"
).dropna(subset=["zipcode"])

zip_lookup["zipcode"] = zip_lookup["zipcode"].astype(str).str.zfill(5)

# Databricks SQL connection config
connection = sql.connect(
    server_hostname="adb-2583620669710215.15.azuredatabricks.net",
    http_path="/sql/1.0/warehouses/148435e03690fbe3",
    access_token=os.getenv("DATABRICKS_TOKEN")
)

import pandas as pd
import streamlit as st
import plotly.express as px

# SQL query
query = """
SELECT DISTINCT
    receipt_id,
    completed_at,
    store,
    customer_zipcode,
    total_collected_post_discount_post_tax_post_fees
FROM analytics_dev.flowhub_silver.transactions
WHERE completed_at >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND transaction_type != 'void'
"""

cursor = connection.cursor()
cursor.execute(query)
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
cursor.close()
connection.close()

df = pd.DataFrame(rows, columns=columns)
df['completed_at'] = pd.to_datetime(df['completed_at'])
df['customer_zipcode'] = df['customer_zipcode'].astype(str).str.zfill(5)
df["total_collected_post_discount_post_tax_post_fees"] = pd.to_numeric(
    df["total_collected_post_discount_post_tax_post_fees"], errors="coerce"
)

# Process zip_lookup to create a proper lookup dictionary
# Create a consistent format for the lookup table with zipcode, lat, lng
processed_zip_lookup = pd.DataFrame(columns=['zipcode', 'lat', 'lng'])

# Assuming zip_lookup is wide format with zips1, zips2, ..., zips308 columns
# and corresponding lat and lng columns
zip_columns = [col for col in zip_lookup.columns if col.startswith('zips')]
for zip_col in zip_columns:
    # Get index number from column name (e.g., 'zips1' -> '1')
    col_index = zip_col[4:]
    
    # Check if corresponding lat/lng columns exist
    lat_col = f'lat{col_index}' if f'lat{col_index}' in zip_lookup.columns else 'lat'
    lng_col = f'lng{col_index}' if f'lng{col_index}' in zip_lookup.columns else 'lng'
    
    # Extract valid zipcodes with their coordinates
    valid_zips = zip_lookup[[zip_col, lat_col, lng_col]].dropna(subset=[zip_col])
    valid_zips = valid_zips.rename(columns={zip_col: 'zipcode', lat_col: 'lat', lng_col: 'lng'})
    
    # Append to our processed lookup
    processed_zip_lookup = pd.concat([processed_zip_lookup, valid_zips], ignore_index=True)

# Clean up the processed lookup
processed_zip_lookup['zipcode'] = processed_zip_lookup['zipcode'].astype(str).str.zfill(5)
processed_zip_lookup = processed_zip_lookup.drop_duplicates(subset=['zipcode'])

# Merge with main dataframe
df = df.merge(processed_zip_lookup, left_on="customer_zipcode", right_on="zipcode", how="left")

# Streamlit UI
st.title("Customer Purchase Density Map (Last 7 Days)")
selected_store = st.selectbox("Choose a Store", sorted(df['store'].dropna().unique()))
color_scale = st.selectbox("Choose a color scale", ["Jet", "Viridis", "Plasma", "Cividis", "Hot"])

filtered_df = df[df['store'] == selected_store].dropna(subset=["lat", "lng"])

# Create the density mapbox figure
fig = px.density_mapbox(
    filtered_df,
    lat="lat",
    lon="lng",
    z="total_collected_post_discount_post_tax_post_fees",
    radius=10,
    zoom=6,
    center={"lat": filtered_df["lat"].mean() if not filtered_df.empty else 39.8283, 
            "lon": filtered_df["lng"].mean() if not filtered_df.empty else -98.5795},  # US center if empty
    mapbox_style="open-street-map",
    color_continuous_scale=color_scale.lower(),
    opacity=0.7,
    title=f"Customer Purchase Density for {selected_store}",
)

# Update the layout with dragmode
fig.update_layout(
    dragmode="zoom",
    mapbox=dict(
        bearing=0,
        pitch=0
    )
)

# Display in Streamlit with config options including scrollZoom
st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": True})

# Add some statistics
st.subheader("Purchase Statistics")
if not filtered_df.empty:
    total_purchases = len(filtered_df)
    total_revenue = filtered_df["total_collected_post_discount_post_tax_post_fees"].sum()
    avg_purchase = filtered_df["total_collected_post_discount_post_tax_post_fees"].mean()
    mapped_percentage = (len(filtered_df) / len(df[df['store'] == selected_store])) * 100
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Purchases", f"{total_purchases:,}")
    col2.metric("Total Revenue", f"${total_revenue:,.2f}")
    col3.metric("Average Purchase", f"${avg_purchase:,.2f}")
    col4.metric("Mapped Purchases", f"{mapped_percentage:.1f}%")
else:
    st.warning("No data available for the selected store or all zip codes are unmapped.")