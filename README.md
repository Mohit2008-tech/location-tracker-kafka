# Location Tracking System

A real-time location tracking system that simulates device movement and visualizes it on an interactive map using Apache Kafka for event streaming.

## Architecture Overview

```
┌─────────────────────┐     ┌─────────────────┐     ┌──────────────────────┐
│   LocationTracker   │     │  Apache Kafka   │     │  kafka-map-visualizer│
│   (Java Producer)   │────▶│   (Broker)      │────▶│   (Node.js Server)   │
│                     │     │                 │     │                      │
│ Simulates circular  │     │ Topic:          │     │ Consumes messages &  │
│ device movement     │     │ device-location │     │ broadcasts via WS    │
└─────────────────────┘     └─────────────────┘     └──────────┬───────────┘
                                                               │
                                                               │ WebSocket
                                                               ▼
                                                    ┌──────────────────────┐
                                                    │   Web Browser        │
                                                    │   (Leaflet Map)      │
                                                    │                      │
                                                    │ Displays real-time   │
                                                    │ device location      │
                                                    └──────────────────────┘
```

## Components

### 1. LocationTracker (Java/Spring Boot)

A Kafka producer application that simulates a GPS device moving in a circular pattern.

**Key Features:**
- Generates location coordinates in a 500m radius circle around New Delhi (28.6139°N, 77.2090°E)
- Publishes JSON messages every 5 seconds to the `device-location` Kafka topic
- Uses Spring Boot 3.5.3 with Spring Kafka

**Message Format:**
```json
{
  "deviceId": "circle-tracker-001",
  "lat": 28.618405,
  "lon": 77.209000,
  "timestamp": "2026-06-05T10:30:00+05:30"
}
```

**Technologies:**
- Java 17
- Spring Boot 3.5.3
- Spring Kafka
- Maven

### 2. kafka-map-visualizer (Node.js)

A WebSocket server that bridges Kafka messages to the browser for real-time visualization.

**Key Features:**
- Consumes messages from the `device-location` Kafka topic
- Normalizes coordinate fields (`lon` → `lng`)
- Broadcasts location updates to all connected WebSocket clients
- Runs WebSocket server on port 8081

**Technologies:**
- Node.js
- KafkaJS (v2.2.4)
- WebSocket (ws v8.18.3)

### 3. Web Frontend (Leaflet Map)

An interactive map interface for visualizing device locations in real-time.

**Key Features:**
- Uses Leaflet.js with OpenStreetMap tiles
- Connects to WebSocket server for live updates
- Displays current coordinates and moves marker in real-time

## Prerequisites

- **Java 17+** - For running the LocationTracker producer
- **Node.js 18+** - For running the visualization server
- **Apache Kafka** - Running on `localhost:9092`

## Quick Start

### 1. Start Apache Kafka

Ensure Kafka is running locally on port 9092. Create the required topic:

```bash
kafka-topics.sh --create --topic device-location --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 2. Start the Location Producer

```bash
cd LocationTracker
./mvnw spring-boot:run
```

Or run the JAR directly:
```bash
java -jar target/LocationTracker-0.0.1-SNAPSHOT.jar
```

### 3. Start the Visualization Server

```bash
cd kafka-map-visualizer
npm install
node server.js
```

### 4. Open the Map

Open `kafka-map-visualizer/index.html` in your browser, or serve it via a local HTTP server.

## Configuration

### LocationTracker Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `CENTER_LAT` | 28.6139 | Center latitude (New Delhi) |
| `CENTER_LON` | 77.2090 | Center longitude (New Delhi) |
| `RADIUS_KM` | 0.5 | Circular path radius in km |
| `UPDATE_INTERVAL_MS` | 5000 | Update interval in milliseconds |
| `BOOTSTRAP_SERVERS` | localhost:9092 | Kafka broker address |
| `TOPIC` | device-location | Kafka topic name |

### Visualization Server Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `brokers` | localhost:9092 | Kafka broker address |
| `groupId` | device-location-consumer | Kafka consumer group |
| WebSocket Port | 8081 | WebSocket server port |

## Project Structure

```
Location Tracking/
├── README.md
├── LocationTracker/                    # Java Kafka Producer
│   ├── pom.xml                         # Maven configuration
│   ├── mvnw, mvnw.cmd                  # Maven wrapper
│   ├── src/
│   │   ├── main/
│   │   │   ├── java/
│   │   │   │   ├── LocationTrackerApplication.java
│   │   │   │   └── com/producer/
│   │   │   │       └── CircularLocationProducer.java
│   │   │   └── resources/
│   │   │       └── application.properties
│   │   └── test/
│   └── target/                         # Build output
│
└── kafka-map-visualizer/               # Node.js Visualization
    ├── package.json
    ├── server.js                       # WebSocket + Kafka consumer
    └── index.html                      # Leaflet map frontend
```

## Data Flow

1. **Producer** generates GPS coordinates simulating circular movement
2. **Kafka** receives and stores messages in the `device-location` topic
3. **Consumer** (Node.js server) reads messages from Kafka
4. **WebSocket** broadcasts location updates to connected browsers
5. **Browser** updates the marker position on the Leaflet map

## Extending the Project

### Adding More Devices

Modify `CircularLocationProducer.java` to simulate multiple devices with different `deviceId` values and movement patterns.

### Adding a Consumer in Java

The project has a placeholder for a Java consumer in `com/consumer/`. Implement a `@KafkaListener` to process location data server-side.

### Persistence

Add a database consumer to store location history for analytics and playback features.

## License

ISC
