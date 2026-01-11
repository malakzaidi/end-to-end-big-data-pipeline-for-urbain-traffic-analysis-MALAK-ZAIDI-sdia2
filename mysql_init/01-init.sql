-- Enhanced MySQL schema for Smart City Traffic Dashboard
-- Includes tables for advanced analytics and visualizations

-- Create database
CREATE DATABASE IF NOT EXISTS traffic_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE traffic_db;

-- Existing tables (from populate_mysql.py)
CREATE TABLE IF NOT EXISTS trafic_par_zone (
    zone VARCHAR(50) PRIMARY KEY,
    nombre_mesures INT,
    trafic_moyen_vehicules_heure DECIMAL(10,2),
    trafic_total INT,
    pic_trafic INT,
    variabilite_trafic DECIMAL(10,2),
    niveau_trafic VARCHAR(20),
    processed_at DATETIME
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS zones_critiques (
    zone VARCHAR(50) PRIMARY KEY,
    niveau_congestion VARCHAR(20),
    processed_at DATETIME
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS taux_congestion (
    zone VARCHAR(50) PRIMARY KEY,
    taux_congestion_pourcentage DECIMAL(5,2),
    processed_at DATETIME
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS vitesse_moyenne (
    zone VARCHAR(50) PRIMARY KEY,
    vitesse_moyenne_kmh DECIMAL(5,2),
    processed_at DATETIME
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS evolution_trafic (
    id INT AUTO_INCREMENT PRIMARY KEY,
    zone VARCHAR(50),
    trafic_vehicules_heure DECIMAL(10,2),
    heure TIME,
    date DATE,
    processed_at DATETIME
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- New tables for enhanced dashboard features

-- Geographic coordinates for zones (for heat map)
CREATE TABLE IF NOT EXISTS zone_coordinates (
    zone VARCHAR(50) PRIMARY KEY,
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    zone_name VARCHAR(100),
    zone_type VARCHAR(50)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Traffic predictions (from ML models)
CREATE TABLE IF NOT EXISTS traffic_predictions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    zone VARCHAR(50),
    prediction_timestamp DATETIME,
    predicted_traffic DECIMAL(10,2),
    predicted_speed DECIMAL(5,2),
    confidence_score DECIMAL(3,2),
    prediction_horizon_hours INT,
    model_version VARCHAR(20),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_zone_time (zone, prediction_timestamp)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Real-time alerts and anomalies
CREATE TABLE IF NOT EXISTS traffic_alerts (
    alert_id VARCHAR(100) PRIMARY KEY,
    alert_type VARCHAR(50),
    severity VARCHAR(20),
    road_id VARCHAR(50),
    zone VARCHAR(50),
    vehicle_count INT,
    average_speed DECIMAL(5,2),
    occupancy_rate DECIMAL(5,2),
    timestamp DATETIME,
    status VARCHAR(20) DEFAULT 'ACTIVE',
    escalation_count INT DEFAULT 0,
    resolved_at DATETIME NULL,
    duration_seconds INT NULL,
    INDEX idx_timestamp (timestamp),
    INDEX idx_zone (zone),
    INDEX idx_status (status)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- KPI trends and metrics
CREATE TABLE IF NOT EXISTS kpi_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    metric_name VARCHAR(100),
    metric_value DECIMAL(10,2),
    metric_unit VARCHAR(20),
    zone VARCHAR(50),
    timestamp DATETIME,
    period VARCHAR(20), -- 'hourly', 'daily', 'weekly'
    INDEX idx_metric_time (metric_name, timestamp),
    INDEX idx_zone_time (zone, timestamp)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Traffic flow direction data
CREATE TABLE IF NOT EXISTS traffic_flow (
    id INT AUTO_INCREMENT PRIMARY KEY,
    road_id VARCHAR(50),
    from_zone VARCHAR(50),
    to_zone VARCHAR(50),
    flow_volume INT,
    average_speed DECIMAL(5,2),
    direction VARCHAR(10), -- 'N', 'S', 'E', 'W', 'NE', etc.
    timestamp DATETIME,
    INDEX idx_road_time (road_id, timestamp),
    INDEX idx_zones (from_zone, to_zone)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Comparative analytics data
CREATE TABLE IF NOT EXISTS comparative_analytics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    comparison_type VARCHAR(50), -- 'zone_vs_zone', 'time_period', 'weekday_vs_weekend'
    zone1 VARCHAR(50),
    zone2 VARCHAR(50),
    metric_name VARCHAR(50),
    value1 DECIMAL(10,2),
    value2 DECIMAL(10,2),
    percentage_diff DECIMAL(5,2),
    time_period VARCHAR(50),
    timestamp DATETIME,
    INDEX idx_comparison (comparison_type, timestamp)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Insert sample zone coordinates (Moroccan cities/regions)
INSERT INTO zone_coordinates (zone, latitude, longitude, zone_name, zone_type) VALUES
('Centre_Ville', 33.5731, -7.5898, 'Casablanca Centre', 'Urban'),
('Centre_Commercial', 33.5789, -7.6328, 'Maarif District', 'Commercial'),
('Périphérie_Nord', 33.6157, -7.5285, 'Hay Hassani', 'Residential'),
('Périphérie_Sud', 33.5333, -7.5833, 'Ain Diab', 'Coastal'),
('Quartier_Résidentiel', 33.6014, -7.6317, 'Racine', 'Residential'),
('Quartier_Est', 33.5731, -7.5898, 'Lissasfa', 'Mixed'),
('Zone_Industrielle', 33.5478, -7.6578, 'Ain Sebaa', 'Industrial'),
('Zone_Commerciale', 33.5789, -7.6328, 'Gauthier', 'Commercial')
ON DUPLICATE KEY UPDATE latitude=VALUES(latitude), longitude=VALUES(longitude);

-- Insert sample KPI data
INSERT INTO kpi_metrics (metric_name, metric_value, metric_unit, zone, timestamp, period) VALUES
('traffic_volume', 1250.5, 'vehicles/hour', 'Centre_Ville', NOW(), 'hourly'),
('average_speed', 35.2, 'km/h', 'Centre_Ville', NOW(), 'hourly'),
('congestion_rate', 42.1, '%', 'Centre_Ville', NOW(), 'hourly'),
('traffic_volume', 890.3, 'vehicles/hour', 'Centre_Commercial', NOW(), 'hourly'),
('average_speed', 45.8, 'km/h', 'Centre_Commercial', NOW(), 'hourly'),
('congestion_rate', 28.5, '%', 'Centre_Commercial', NOW(), 'hourly');

-- Insert sample traffic predictions
INSERT INTO traffic_predictions (zone, prediction_timestamp, predicted_traffic, predicted_speed, confidence_score, prediction_horizon_hours, model_version) VALUES
('Centre_Ville', DATE_ADD(NOW(), INTERVAL 1 HOUR), 1450.5, 32.1, 0.85, 1, 'v1.0'),
('Centre_Ville', DATE_ADD(NOW(), INTERVAL 2 HOUR), 1650.8, 28.5, 0.78, 2, 'v1.0'),
('Centre_Commercial', DATE_ADD(NOW(), INTERVAL 1 HOUR), 1100.2, 42.3, 0.88, 1, 'v1.0'),
('Centre_Commercial', DATE_ADD(NOW(), INTERVAL 2 HOUR), 1350.7, 38.9, 0.82, 2, 'v1.0');

-- Insert sample alerts
INSERT INTO traffic_alerts (alert_id, alert_type, severity, road_id, zone, vehicle_count, average_speed, occupancy_rate, timestamp) VALUES
('alert_001', 'HIGH_CONGESTION', 'HIGH', 'ROAD_CV_001', 'Centre_Ville', 160, 25.5, 85.2, NOW()),
('alert_002', 'MODERATE_CONGESTION', 'MEDIUM', 'ROAD_CC_002', 'Centre_Commercial', 130, 35.8, 72.1, DATE_SUB(NOW(), INTERVAL 30 MINUTE));

-- Insert sample comparative data
INSERT INTO comparative_analytics (comparison_type, zone1, zone2, metric_name, value1, value2, percentage_diff, time_period, timestamp) VALUES
('zone_vs_zone', 'Centre_Ville', 'Centre_Commercial', 'traffic_volume', 1250.5, 890.3, 40.5, 'current_hour', NOW()),
('zone_vs_zone', 'Centre_Ville', 'Centre_Commercial', 'average_speed', 35.2, 45.8, -23.1, 'current_hour', NOW()),
('time_period', 'Centre_Ville', 'Centre_Ville', 'traffic_volume', 1250.5, 980.2, 27.5, 'peak_vs_offpeak', NOW());
