import streamlit as st  # Move this to the top
import pandas as pd
import plotly.express as px
from databricks import sql
import os
import numpy as np
import sys
print("Python executable:", sys.executable)
import zipcodes

# Databricks SQL connection config
connection = sql.connect(
    server_hostname="adb-2583620669710215.15.azuredatabricks.net",
    http_path="/sql/1.0/warehouses/148435e03690fbe3",
    access_token=os.getenv("DATABRICKS_TOKEN")
)

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

# Function to validate ZIP code and get coordinates
def get_zip_coords(zip_code):
    try:
        if zip_code and zip_code.isdigit() and len(zip_code) == 5:
            zip_info = zipcodes.matching(zip_code)
            if zip_info:
                return float(zip_info[0]['lat']), float(zip_info[0]['long'])
        return None, None
    except Exception as e:
        print(f"Error fetching coordinates for ZIP {zip_code}: {e}")
        return None, None

# Drop rows with null or invalid ZIP codes
df = df[df['customer_zipcode'].notnull() & df['customer_zipcode'].str.isdigit() & (df['customer_zipcode'].str.len() == 5)]

# Apply the function to get coordinates
df[['lat', 'lng']] = df.apply(
    lambda row: pd.Series(get_zip_coords(row['customer_zipcode'])), 
    axis=1
)

# Drop rows with null coordinates
df = df.dropna(subset=["lat", "lng"])

# Streamlit UI
st.title("Customer Purchase Density Map (Last 7 Days)")
selected_store = st.selectbox("Choose a Store", sorted(df['store'].dropna().unique()))
color_scale = st.selectbox("Choose a color scale", ["Jet", "Viridis", "Plasma", "Cividis", "Hot"])

# Filter data for the selected store
filtered_df = df[df['store'] == selected_store]

# Check if there is data to plot
if not filtered_df.empty:
    # Create the density mapbox figure (corrected from density_map to density_mapbox)
    fig = px.density_mapbox(
        filtered_df,
        lat="lat",
        lon="lng",
        z="total_collected_post_discount_post_tax_post_fees",
        radius=10,
        zoom=6,
        center={"lat": filtered_df["lat"].mean(), "lon": filtered_df["lng"].mean()},
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
    total_purchases = len(filtered_df)
    total_revenue = filtered_df["total_collected_post_discount_post_tax_post_fees"].sum()
    avg_purchase = filtered_df["total_collected_post_discount_post_tax_post_fees"].mean()
    
    # Calculate mapped percentage safely
    store_total = len(df[df['store'] == selected_store])
    mapped_percentage = (len(filtered_df) / store_total * 100) if store_total > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Purchases", f"{total_purchases:,}")
    col2.metric("Total Revenue", f"${total_revenue:,.2f}")
    col3.metric("Average Purchase", f"${avg_purchase:,.2f}")
    col4.metric("Mapped Purchases", f"{mapped_percentage:.1f}%")
else:
    st.warning("No data available for the selected store or all zip codes are unmapped.")