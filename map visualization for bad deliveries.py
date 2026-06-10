# Databricks notebook source
# MAGIC %md
# MAGIC First data generation method

# COMMAND ----------

import random
import string
import pandas as pd
from datetime import datetime, timedelta

# Function to generate random UK postcodes
def generate_postcode():
    outward = random.choice(string.ascii_uppercase) + random.choice(string.ascii_uppercase) + str(random.randint(1, 9))
    inward = str(random.randint(0, 9)) + random.choice(string.ascii_uppercase) + random.choice(string.ascii_uppercase)
    return f"{outward} {inward}"

# Function to generate random delivery statuses
def generate_delivery_status():
    return random.choice(["Delivered", "Failed", "Pending"])

# Function to generate random parcel types
def generate_parcel_type():
    return random.choice(["Small Box", "Medium Box", "Large Box", "Letter"])

# Function to generate random delivery scores
def generate_delivery_score():
    return round(random.uniform(0.0, 1.0), 2)

# Function to generate random delivery dates
def generate_delivery_date():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 5, 28)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_time = timedelta(seconds=random.randint(0, 86400))  # Random time within a day
    return start_date + timedelta(days=random_days) + random_time

# Generate synthetic data
def generate_synthetic_data(num_records):
    data = []
    for i in range(num_records):
        delivery_id = f"D{i+1:05d}"  # Unique delivery ID (e.g., D00001)
        postcode = generate_postcode()
        delivery_score = generate_delivery_score()
        compliance_status = delivery_score >= 0.5
        delivery_date = generate_delivery_date()
        delivery_status = generate_delivery_status()
        parcel_type = generate_parcel_type()
        
        data.append({
            "delivery_id": delivery_id,
            "postcode": postcode,
            "delivery_score": delivery_score,
            "compliance_status": compliance_status,
            "delivery_date": delivery_date,
            "delivery_status": delivery_status,
            "parcel_type": parcel_type
        })
    return data

# Generate 100 synthetic records
synthetic_data = generate_synthetic_data(500)

# Convert to a DataFrame for better visualization
df = pd.DataFrame(synthetic_data)

# Save to a CSV file (optional)
df.to_csv("synthetic_delivery_data.csv", index=False)

# Display the first few rows
df.head()

# COMMAND ----------

df.info()
df.describe()

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC 2nd data generated method

# COMMAND ----------

import random
import pandas as pd

# Example list of real London postcodes (you can expand this with a dataset)
london_postcodes = [
    "E1 6AN", "N1 2RY", "SE1 7PB", "SW1A 1AA", "W1D 2HP", "NW1 8BF", "EC1A 1BB", "WC2E 7HQ"
]

# Function to generate random delivery statuses
def generate_delivery_status():
    return random.choice(["Delivered", "Failed", "Pending"])

# Function to generate random parcel types
def generate_parcel_type():
    return random.choice(["Small Box", "Medium Box", "Large Box", "Letter"])

# Function to generate random delivery scores
def generate_delivery_score():
    return round(random.uniform(0.0, 1.0), 2)

# Function to generate random delivery dates
def generate_delivery_date():
    start_date = pd.Timestamp("2023-01-01")
    end_date = pd.Timestamp("2025-05-28")
    return start_date + (end_date - start_date) * random.random()

# Generate synthetic data
def generate_synthetic_data(num_records):
    data = []
    for i in range(num_records):
        delivery_id = f"D{i+1:05d}"  # Unique delivery ID (e.g., D00001)
        postcode = random.choice(london_postcodes)  # Use real London postcodes
        delivery_score = generate_delivery_score()
        compliance_status = delivery_score >= 0.5
        delivery_date = generate_delivery_date()
        delivery_status = generate_delivery_status()
        parcel_type = generate_parcel_type()
        
        data.append({
            "delivery_id": delivery_id,
            "postcode": postcode,
            "delivery_score": delivery_score,
            "compliance_status": compliance_status,
            "delivery_date": delivery_date,
            "delivery_status": delivery_status,
            "parcel_type": parcel_type
        })
    return data

# Generate 300 synthetic records
synthetic_data = generate_synthetic_data(500)

# Convert to a DataFrame for better visualization
df = pd.DataFrame(synthetic_data)

# Save to a CSV file (optional)
df.to_csv("real_london_postcodes_data.csv", index=False)

# Display the first few rows
df.head()


# COMMAND ----------

df1 = pd.read_csv("synthetic_delivery_data.csv")



# COMMAND ----------

unique_count1 = len(df1['postcode'].unique())
unique_count1

# COMMAND ----------

unique_count = len(df['postcode'].unique())

# COMMAND ----------

unique_count

# COMMAND ----------

# MAGIC %md
# MAGIC Map visual

# COMMAND ----------

df = pd.read_csv("real_london_postcodes_data.csv")

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC new schema based on lat and long

# COMMAND ----------

pip install folium

# COMMAND ----------

import random
import pandas as pd
import folium
import requests
from datetime import datetime, timedelta

# Function to fetch latitude and longitude for a given postcode using Postcodes.io API
def get_coordinates(postcode):
    url = f"https://api.postcodes.io/postcodes/{postcode}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data["result"]["latitude"], data["result"]["longitude"]
    else:
        return None, None

# List of real London postcodes (you can expand this list or fetch dynamically)
london_postcodes = [
    "EC1A 1BB", "W1D 2HP", "E1 6AN", "SW1A 1AA", "NW1 8BF",
    "SE1 7PB", "N1 2RY", "W3 6YJ", "NW5 1PL", "W5 4JJ",
    "SE8 4PB", "NW6 7EH", "N3 3HG", "NW11 6SR", "W7 2AE"
]

# Function to generate random delivery statuses
def generate_delivery_status():
    return random.choice(["Delivered", "Failed", "Pending"])

# Function to generate random parcel types
def generate_parcel_type():
    return random.choice(["Small Box", "Medium Box", "Large Box", "Letter"])

# Function to generate random delivery scores
def generate_delivery_score():
    return round(random.uniform(0.0, 1.0), 2)

# Function to generate random delivery dates
def generate_delivery_date():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 5, 28)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_time = timedelta(seconds=random.randint(0, 86400))  # Random time within a day
    return start_date + timedelta(days=random_days) + random_time

# Generate synthetic data
def generate_synthetic_data(num_records):
    data = []
    for i in range(num_records):
        delivery_id = f"D{i+1:05d}"  # Unique delivery ID (e.g., D00001)
        postcode = random.choice(london_postcodes)  # Randomly select a real London postcode
        latitude, longitude = get_coordinates(postcode)  # Fetch latitude and longitude
        if latitude is None or longitude is None:
            continue  # Skip if coordinates are not found
        delivery_score = generate_delivery_score()
        compliance_status = delivery_score >= 0.5
        delivery_date = generate_delivery_date()
        delivery_status = generate_delivery_status()
        parcel_type = generate_parcel_type()
        
        data.append({
            "delivery_id": delivery_id,
            "postcode": postcode,
            "latitude": latitude,
            "longitude": longitude,
            "delivery_score": delivery_score,
            "compliance_status": compliance_status,
            "delivery_date": delivery_date,
            "delivery_status": delivery_status,
            "parcel_type": parcel_type
        })
    return data

# Generate 500 synthetic records
synthetic_data = generate_synthetic_data(500)

# Convert data to a DataFrame
df = pd.DataFrame(synthetic_data)

df.to_csv("real_london_postcodes_lat_long.csv", index=False)


# COMMAND ----------

import random
import pandas as pd
import folium
import requests
from datetime import datetime, timedelta
df = pd.read_csv("real_london_postcodes_lat_long.csv")

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC Folium code

# COMMAND ----------

# Create a base map centered around London
m = folium.Map(location=[51.509865, -0.118092], zoom_start=11)

# Add markers to the map
for _, row in df.iterrows():
    # Marker color based on compliance status
    marker_color = "green" if row["compliance_status"] else "red"
    
    # Add a marker for each delivery
    folium.Marker(
        location=[row["latitude"], row["longitude"]],
        popup=(
            f"<b>Delivery ID:</b> {row['delivery_id']}<br>"
            f"<b>Postcode:</b> {row['postcode']}<br>"
            f"<b>Delivery Score:</b> {row['delivery_score']}<br>"
            f"<b>Compliance:</b> {'Compliant' if row['compliance_status'] else 'Non-Compliant'}<br>"
            f"<b>Parcel Type:</b> {row['parcel_type']}<br>"
            f"<b>Delivery Status:</b> {row['delivery_status']}<br>"
            f"<b>Delivery Date:</b> {row['delivery_date']}"
        ),
        icon=folium.Icon(color=marker_color, icon="info-sign")
    ).add_to(m)

# Save the map to an HTML file
m.save("delivery_compliance_map.html")

print("Map has been created and saved as 'delivery_compliance_map.html'.")

# COMMAND ----------

unique_count = len(df['postcode'].unique())

# COMMAND ----------

unique_count

# COMMAND ----------

len(df['latitude'].unique())

# COMMAND ----------

# MAGIC %md
# MAGIC Non compliant map only

# COMMAND ----------

from folium.plugins import MarkerCluster
# Filter only non-compliant deliveries
non_compliant_df = df[df["compliance_status"] == False]

# Create a base map centered around London
m = folium.Map(location=[51.509865, -0.118092], zoom_start=11)

# Create a MarkerCluster
marker_cluster = MarkerCluster().add_to(m)

# Add non-compliant deliveries to the map
for _, row in non_compliant_df.iterrows():
    folium.Marker(
        location=[row["latitude"], row["longitude"]],
        popup=(
            f"<b>Delivery ID:</b> {row['delivery_id']}<br>"
            f"<b>Postcode:</b> {row['postcode']}<br>"
            f"<b>Delivery Score:</b> {row['delivery_score']}<br>"
            f"<b>Compliance:</b> {'Compliant' if row['compliance_status'] else 'Non-Compliant'}<br>"
            f"<b>Parcel Type:</b> {row['parcel_type']}<br>"
            f"<b>Delivery Status:</b> {row['delivery_status']}<br>"
            f"<b>Delivery Date:</b> {row['delivery_date']}"
        ),
        icon=folium.Icon(color="red", icon="info-sign")
    ).add_to(marker_cluster)

# Save the map to an HTML file
m.save("non_compliant_deliveries_map.html")

print("Map has been created and saved as 'non_compliant_deliveries_map.html'.")



# COMMAND ----------

m

# COMMAND ----------

# MAGIC %md
# MAGIC Clusters in red color

# COMMAND ----------


# Filter only non-compliant deliveries
non_compliant_df = df[df["compliance_status"] == False]

# Create a base map centered around London
m = folium.Map(location=[51.509865, -0.118092], zoom_start=11)

# Define a custom JavaScript function for red cluster icons
custom_icon = """
function(cluster) {
    return L.divIcon({
        html: '<div style="background-color: rgba(255, 0, 0, 0.6); border-radius: 50%; color: white; width: 40px; height: 40px; display: flex; align-items: center; justify-content: center; font-size: 14px;">' + cluster.getChildCount() + '</div>',
        className: 'marker-cluster',
        iconSize: [40, 40]
    });
}
"""

# Create a MarkerCluster with the custom icon
marker_cluster = MarkerCluster(icon_create_function=custom_icon).add_to(m)

# Add non-compliant deliveries to the map
for _, row in non_compliant_df.iterrows():
    folium.Marker(
        location=[row["latitude"], row["longitude"]],
        popup=(
            f"<b>Delivery ID:</b> {row['delivery_id']}<br>"
            f"<b>Postcode:</b> {row['postcode']}<br>"
            f"<b>Delivery Score:</b> {row['delivery_score']}<br>"
            f"<b>Compliance:</b> {'Compliant' if row['compliance_status'] else 'Non-Compliant'}<br>"
            f"<b>Parcel Type:</b> {row['parcel_type']}<br>"
            f"<b>Delivery Status:</b> {row['delivery_status']}<br>"
            f"<b>Delivery Date:</b> {row['delivery_date']}"
        ),
        icon=folium.Icon(color="red", icon="info-sign")
    ).add_to(marker_cluster)

# Display the map in Jupyter Notebook or save it as an HTML file
# m.save("non_compliant_deliveries_red_clusters.html")
m

# COMMAND ----------

# MAGIC %md
# MAGIC Data generation with courier column

# COMMAND ----------

import random
import pandas as pd
import folium
import requests
from folium.plugins import MarkerCluster
from ipywidgets import interact
from datetime import datetime, timedelta

# Function to fetch latitude and longitude for a given postcode using Postcodes.io API
def get_coordinates(postcode):
    url = f"https://api.postcodes.io/postcodes/{postcode}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data["result"]["latitude"], data["result"]["longitude"]
    else:
        return None, None

# List of real London postcodes
london_postcodes = [
    "EC1A 1BB", "W1D 2HP", "E1 6AN", "SW1A 1AA", "NW1 8BF",
    "SE1 7PB", "N1 2RY", "W3 6YJ", "NW5 1PL", "W5 4JJ",
    "SE8 4PB", "NW6 7EH", "N3 3HG", "NW11 6SR", "W7 2AE"
]

# Function to generate random delivery statuses
def generate_delivery_status():
    return random.choice(["Delivered", "Failed", "Pending"])

# Function to generate random parcel types
def generate_parcel_type():
    return random.choice(["Small Box", "Medium Box", "Large Box", "Letter"])

# Function to generate random delivery scores
def generate_delivery_score():
    return round(random.uniform(0.0, 1.0), 2)

# Function to generate random delivery dates
def generate_delivery_date():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 5, 28)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_time = timedelta(seconds=random.randint(0, 86400))  # Random time within a day
    return start_date + timedelta(days=random_days) + random_time

# Generate synthetic data
def generate_synthetic_data(num_records):
    couriers = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Hannah", "Ivy", "Jack"]  # 10 couriers
    data = []
    for i in range(num_records):
        delivery_id = f"D{i+1:05d}"  # Unique delivery ID (e.g., D00001)
        postcode = random.choice(london_postcodes)  # Randomly select a real London postcode
        latitude, longitude = get_coordinates(postcode)  # Fetch latitude and longitude
        if latitude is None or longitude is None:
            continue  # Skip if coordinates are not found
        delivery_score = generate_delivery_score()
        compliance_status = delivery_score >= 0.5
        delivery_date = generate_delivery_date()
        delivery_status = generate_delivery_status()
        parcel_type = generate_parcel_type()
        courier = random.choice(couriers)  # Randomly assign a courier
        
        data.append({
            "delivery_id": delivery_id,
            "postcode": postcode,
            "latitude": latitude,
            "longitude": longitude,
            "delivery_score": delivery_score,
            "compliance_status": compliance_status,
            "delivery_date": delivery_date,
            "delivery_status": delivery_status,
            "parcel_type": parcel_type,
            "courier": courier  # Add courier column
        })
    return data

# Generate 500 synthetic records
synthetic_data = generate_synthetic_data(500)

# Convert data to a DataFrame
df = pd.DataFrame(synthetic_data)



# COMMAND ----------

df.head()

# COMMAND ----------

# Calculate compliance percentage for each courier
compliance_summary = df.groupby("courier")["compliance_status"].mean() * 100
df["courier_compliance"] = df["courier"].map(compliance_summary)

# Function to create and display the map
def create_map(compliance_threshold):
    # Filter couriers based on compliance percentage
    filtered_df = df[df["courier_compliance"] >= compliance_threshold]
    
    # Create a Folium map
    m = folium.Map(location=[51.509865, -0.118092], zoom_start=11)
    
    # Add MarkerCluster
    marker_cluster = MarkerCluster().add_to(m)
    
    # Add filtered data to the map
    for _, row in filtered_df.iterrows():
        folium.Marker(
            location=[row["latitude"], row["longitude"]],
            popup=(
                f"<b>Delivery ID:</b> {row['delivery_id']}<br>"
                f"<b>Postcode:</b> {row['postcode']}<br>"
                f"<b>Delivery Score:</b> {row['delivery_score']}<br>"
                f"<b>Compliance:</b> {'Compliant' if row['compliance_status'] else 'Non-Compliant'}<br>"
                f"<b>Parcel Type:</b> {row['parcel_type']}<br>"
                f"<b>Delivery Status:</b> {row['delivery_status']}<br>"
                f"<b>Courier:</b> {row['courier']}<br>"
                f"<b>Courier Compliance:</b> {row['courier_compliance']:.2f}%"
            ),
            icon=folium.Icon(color="red" if not row["compliance_status"] else "green", icon="info-sign")
        ).add_to(marker_cluster)
    
    # Display the map
    return m

# Add a slider to filter the map
interact(create_map, compliance_threshold=(0, 100, 1))


# COMMAND ----------

import random
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from ipywidgets import interact
from datetime import datetime, timedelta

# Function to fetch latitude and longitude for a given postcode using Postcodes.io API
def get_coordinates(postcode):
    url = f"https://api.postcodes.io/postcodes/{postcode}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data["result"]["latitude"], data["result"]["longitude"]
    else:
        return None, None

# List of real London postcodes
london_postcodes = [
    "EC1A 1BB", "W1D 2HP", "E1 6AN", "SW1A 1AA", "NW1 8BF",
    "SE1 7PB", "N1 2RY", "W3 6YJ", "NW5 1PL", "W5 4JJ",
    "SE8 4PB", "NW6 7EH", "N3 3HG", "NW11 6SR", "W7 2AE"
]

# Function to generate random delivery statuses
def generate_delivery_status():
    return random.choice(["Delivered", "Failed", "Pending"])

# Function to generate random parcel types
def generate_parcel_type():
    return random.choice(["Small Box", "Medium Box", "Large Box", "Letter"])

# Function to generate random delivery scores
def generate_delivery_score():
    return round(random.uniform(0.0, 1.0), 2)

# Function to generate random delivery dates
def generate_delivery_date():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 5, 28)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_time = timedelta(seconds=random.randint(0, 86400))  # Random time within a day
    return start_date + timedelta(days=random_days) + random_time

# Generate synthetic data
def generate_synthetic_data(num_records):
    couriers = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Hannah", "Ivy", "Jack"]  # 10 couriers
    data = []
    for i in range(num_records):
        delivery_id = f"D{i+1:05d}"  # Unique delivery ID (e.g., D00001)
        postcode = random.choice(london_postcodes)  # Randomly select a real London postcode
        latitude, longitude = get_coordinates(postcode)  # Fetch latitude and longitude
        if latitude is None or longitude is None:
            continue  # Skip if coordinates are not found
        delivery_score = generate_delivery_score()
        compliance_status = delivery_score >= 0.5
        delivery_date = generate_delivery_date()
        delivery_status = generate_delivery_status()
        parcel_type = generate_parcel_type()
        courier = random.choice(couriers)  # Randomly assign a courier
        
        data.append({
            "delivery_id": delivery_id,
            "postcode": postcode,
            "latitude": latitude,
            "longitude": longitude,
            "delivery_score": delivery_score,
            "compliance_status": compliance_status,
            "delivery_date": delivery_date,
            "delivery_status": delivery_status,
            "parcel_type": parcel_type,
            "courier": courier  # Add courier column
        })
    return data

# Generate 500 synthetic records
synthetic_data = generate_synthetic_data(500)

# Convert data to a DataFrame
df = pd.DataFrame(synthetic_data)

# Calculate compliance percentage for each courier
compliance_summary = df.groupby("courier")["compliance_status"].mean() * 100
df["courier_compliance"] = df["courier"].map(compliance_summary)

# Define a custom JavaScript function for green cluster icons
custom_icon = """
function(cluster) {
    return L.divIcon({
        html: '<div style="background-color: rgba(0, 128, 0, 0.6); border-radius: 50%; color: white; width: 40px; height: 40px; display: flex; align-items: center; justify-content: center; font-size: 14px;">' + cluster.getChildCount() + '</div>',
        className: 'marker-cluster',
        iconSize: [40, 40]
    });
}
"""

# Function to create and display the map
def create_map(compliance_threshold):
    # Filter couriers based on compliance percentage
    filtered_df = df[df["courier_compliance"] >= compliance_threshold]
    
    # Create a Folium map
    m = folium.Map(location=[51.509865, -0.118092], zoom_start=11)
    
    # Add MarkerCluster with custom green cluster icons
    marker_cluster = MarkerCluster(icon_create_function=custom_icon).add_to(m)
    
    # Add filtered data to the map
    for _, row in filtered_df.iterrows():
        folium.Marker(
            location=[row["latitude"], row["longitude"]],
            popup=(
                f"<b>Delivery ID:</b> {row['delivery_id']}<br>"
                f"<b>Postcode:</b> {row['postcode']}<br>"
                f"<b>Delivery Score:</b> {row['delivery_score']}<br>"
                f"<b>Compliance:</b> {'Compliant' if row['compliance_status'] else 'Non-Compliant'}<br>"
                f"<b>Parcel Type:</b> {row['parcel_type']}<br>"
                f"<b>Delivery Status:</b> {row['delivery_status']}<br>"
                f"<b>Courier:</b> {row['courier']}<br>"
                f"<b>Courier Compliance:</b> {row['courier_compliance']:.2f}%"
            ),
            icon=folium.Icon(color="red" if not row["compliance_status"] else "green", icon="info-sign")
        ).add_to(marker_cluster)
    
    # Display the map
    return m

# Add a slider to filter the map
interact(create_map, compliance_threshold=(0, 100, 1))
