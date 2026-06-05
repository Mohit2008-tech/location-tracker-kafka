import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.*;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class LocationTrackerApplication {
    
	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "device-location";

    private static final double CENTER_LAT = 28.6139;  // New Delhi latitude
    private static final double CENTER_LON = 77.2090;  // New Delhi longitude
    private static final double RADIUS_KM = 0.5;       // 500 meters
    private static final int UPDATE_INTERVAL_MS = 5000;
	public static void main(String[] args) throws InterruptedException {
		//SpringApplication.run(LocationTrackerApplication.class, args);

		Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        double angle = 0.0;
        double angleStep = Math.toRadians(10); // Step in radians per update (10°)

        while (true) {
            double lat = CENTER_LAT + (RADIUS_KM / 111.0) * Math.cos(angle); // 1° lat ~ 111 km
            double lon = CENTER_LON + (RADIUS_KM / (111.0 * Math.cos(Math.toRadians(CENTER_LAT)))) * Math.sin(angle);

            String timestamp = new Date().toInstant().toString();
            String payload = String.format(
                "{\"deviceId\": \"circle-tracker-001\", \"lat\": %.6f, \"lon\": %.6f, \"timestamp\": \"%s\"}",
                lat, lon, timestamp
            );

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "circle-tracker-001", payload);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Send failed: " + exception.getMessage());
                } else {
                    System.out.println("Sent: " + payload);
                }
            });

            angle += angleStep;
            if (angle >= 2 * Math.PI) {
                angle = 0;
            }

            TimeUnit.MILLISECONDS.sleep(UPDATE_INTERVAL_MS);
        }

	}

}
