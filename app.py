import pandas as pd
import plotly.express as px
import streamlit as st
from databricks import sql
import os
import zipcodes  # You'll need to install this package

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

# Get coordinates for each zipcode
def get_zip_coords(zip_code):
    try:
        zip_info = zipcodes.matching(zip_code)
        if zip_info:
            return float(zip_info[0]['lat']), float(zip_info[0]['long'])
        return None, None
    except:
        return None, None

# Apply the function to get coordinates
df[['lat', 'lng']] = df.apply(
    lambda row: pd.Series(get_zip_coords(row['customer_zipcode'])), 
    axis=1
)

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