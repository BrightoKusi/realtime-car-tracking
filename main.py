import threading
import time
import random
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
import json

# Global Vehicle ID for all streams
vehicle_id = f"GH{random.randint(1000, 9999)}"

# Coordinates for Accra and Kumasi
start_location = (5.6037, -0.1870)  # Accra
end_location = (6.6928, -1.5730)    # Kumasi

# Helper function to simulate GPS coordinates between start and end
def generate_coordinates(start, end, steps=20):
    lat_step = (end[0] - start[0]) / steps
    lon_step = (end[1] - start[1]) / steps
    return [(start[0] + i * lat_step, start[1] + i * lon_step) for i in range(steps + 1)]

# Callback for message delivery confirmation
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Kafka configuration for SerializingProducer
producer_config = {
    'bootstrap.servers': '',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': StringSerializer('utf_8'),
}

producer = SerializingProducer(producer_config)

# 1. Vehicle Information
def generate_vehicle_info():
    vehicle_types = ['Sedan', 'SUV', 'Truck', 'Bus']
    statuses = ['moving', 'stopped', 'idling']
    
    while True:
        vehicle_data = {
            "vehicle_id": vehicle_id,
            "type": random.choice(vehicle_types),
            "speed": random.randint(40, 120),
            "status": random.choice(statuses),
            "timestamp": time.time()
        }
        producer.produce(
            topic='vehicle_info', 
            key=vehicle_id,
            value=json.dumps(vehicle_data),
            on_delivery=delivery_report
        )
        producer.flush()  # Ensure the message is sent
        print(f"Vehicle Info Sent: {vehicle_data}")
        time.sleep(5)

# 2. GPS Information
def generate_gps_info(start_location, end_location):
    coordinates = generate_coordinates(start_location, end_location)
    
    for lat, lon in coordinates:
        gps_data = {
            "vehicle_id": vehicle_id,
            "latitude": lat,
            "longitude": lon,
            "timestamp": time.time()
        }
        producer.produce(
            topic='gps_info',
            key=vehicle_id,
            value=json.dumps(gps_data),
            on_delivery=delivery_report
        )
        producer.flush()
        print(f"GPS Info Sent: {gps_data}")
        time.sleep(5)  # Simulate time between location updates

# 3. Camera Information
def generate_camera_info():
    frame_number = 0
    while True:
        camera_data = {
            "vehicle_id": vehicle_id,
            "frame_number": frame_number,
            "camera_status": "active",
            "timestamp": time.time()
        }
        producer.produce(
            topic='camera_info',
            key=vehicle_id,
            value=json.dumps(camera_data),
            on_delivery=delivery_report
        )
        producer.flush()
        print(f"Camera Info Sent: {camera_data}")
        frame_number += 1
        time.sleep(2)

# 4. Weather Information
def generate_weather_info():
    conditions = ['Sunny', 'Rainy', 'Cloudy', 'Stormy']
    
    while True:
        weather_data = {
            "vehicle_id": vehicle_id,
            "location": "Accra-Kumasi Route",
            "temperature": random.randint(20, 35),
            "humidity": random.randint(50, 100),
            "condition": random.choice(conditions),
            "timestamp": time.time()
        }
        producer.produce(
            topic='weather_info',
            key=vehicle_id,
            value=json.dumps(weather_data),
            on_delivery=delivery_report
        )
        producer.flush()
        print(f"Weather Info Sent: {weather_data}")
        time.sleep(10)

# 5. Emergency Information
def generate_emergency_info():
    emergencies = ['Accident', 'Engine Failure', 'Flat Tire', 'Low Fuel']
    
    while True:
        emergency_data = {
            "vehicle_id": vehicle_id,
            "emergency_type": random.choice(emergencies),
            "severity": random.randint(1, 5),
            "timestamp": time.time()
        }
        producer.produce(
            topic='emergency_info',
            key=vehicle_id,
            value=json.dumps(emergency_data),
            on_delivery=delivery_report
        )
        producer.flush()
        print(f"Emergency Info Sent: {emergency_data}")
        time.sleep(random.randint(20, 60))

# Main block to manage threading and simulation
if __name__ == "__main__":
    # Define and start threads for each function
    vehicle_thread = threading.Thread(target=generate_vehicle_info)
    gps_thread = threading.Thread(target=generate_gps_info, args=(start_location, end_location))
    camera_thread = threading.Thread(target=generate_camera_info)
    weather_thread = threading.Thread(target=generate_weather_info)
    emergency_thread = threading.Thread(target=generate_emergency_info)

    # Start all threads
    vehicle_thread.start()
    gps_thread.start()
    camera_thread.start()
    weather_thread.start()
    emergency_thread.start()

    # Optional: Join threads to wait for them to finish (though they will run indefinitely)
    vehicle_thread.join()
    gps_thread.join()
    camera_thread.join()
    weather_thread.join()
    emergency_thread.join()
