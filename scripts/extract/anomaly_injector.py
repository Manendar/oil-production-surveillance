import random


## Function that simulates anomalies from a real device

def inject_anomaly(reading, baselines):
    """Randomly inject a realistic anomaly into the reading."""
    anomaly_type = random.choice([
        "pressure_drop",
        "vibration_spike",
        "h2s_leak",
        "pump_failure",
        "water_cut_surge",
        "gas_breakthrough",
        "sand_erosion",
        "casing_integrity_loss",
    ])

    if anomaly_type == "pressure_drop":
        reading["wellhead_pressure"] = round(baselines["wellhead_pressure"] * random.uniform(0.5, 0.7), 2)
        reading["tubing_pressure"] = round(baselines["tubing_pressure"] * random.uniform(0.5, 0.7), 2)
    elif anomaly_type == "vibration_spike":
        reading["vibration_level"] = round(baselines["vibration_level"] * random.uniform(3, 5), 2)
    elif anomaly_type == "h2s_leak":
        reading["h2s_concentration"] = round(random.uniform(15, 80), 2)
    elif anomaly_type == "pump_failure":
        reading["motor_current"] = 0
        reading["pump_speed"] = 0
        reading["oil_flow_rate"] = 0
        reading["well_status"] = "shut-in"
    elif anomaly_type == "water_cut_surge":
        reading["water_cut"] = round(min(baselines["water_cut"] * random.uniform(2, 3), 95), 2)
        reading["water_flow_rate"] = round(baselines["water_flow_rate"] * random.uniform(2, 3), 2)
    elif anomaly_type == "gas_breakthrough":
        reading["gas_oil_ratio"] = round(baselines["gas_oil_ratio"] * random.uniform(2, 4), 2)
        reading["gas_flow_rate"] = round(baselines["gas_flow_rate"] * random.uniform(2, 3), 2)
    elif anomaly_type == "sand_erosion":
        reading["sand_rate"] = round(baselines["sand_rate"] * random.uniform(5, 10), 2)
    elif anomaly_type == "casing_integrity_loss":
        reading["casing_pressure"] = round(baselines["casing_pressure"] * random.uniform(2, 3.5), 2)

    reading["anomaly_type"] = anomaly_type
    return reading