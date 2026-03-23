-- Création de la table de dimension pour les villes
CREATE TABLE IF NOT EXISTS dim_city (
    city_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    country VARCHAR(50)
);

-- Création de la table de faits pour la Météo (OpenWeatherMap)
CREATE TABLE IF NOT EXISTS fact_weather (
    id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES dim_city(city_id),
    timestamp TIMESTAMP NOT NULL,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    wind_speed DECIMAL(5,2),
    weather_condition VARCHAR(100)
);

-- Création de la table de faits pour la Qualité de l'Air (AQICN)
CREATE TABLE IF NOT EXISTS fact_air_quality (
    id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES dim_city(city_id),
    timestamp TIMESTAMP NOT NULL,
    aqi INTEGER,
    pm25 DECIMAL(7,2),
    pm10 DECIMAL(7,2),
    no2 DECIMAL(7,2),
    o3 DECIMAL(7,2)
);

-- Insertion des villes principales ciblées par GoodAir
INSERT INTO dim_city (name, country) VALUES
('Paris', 'France'),
('Lyon', 'France'),
('Marseille', 'France'),
('Bordeaux', 'France'),
('Lille', 'France')
ON CONFLICT (name) DO NOTHING;