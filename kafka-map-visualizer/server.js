const { Kafka } = require("kafkajs");
const WebSocket = require("ws");

const kafka = new Kafka({ clientId: "map-visualizer", brokers: ["localhost:9092"] });
const consumer = kafka.consumer({ groupId: "device-location-consumer" });

const wss = new WebSocket.Server({ port: 8081 });

(async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "device-location", fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      let data;
      try {
        data = JSON.parse(message.value.toString());
        console.log(data);
      } catch (e) {
        console.error('Invalid JSON received:', message.value.toString());
        return;
      }
      // Accept both 'lng' and 'lon' as longitude, normalize to 'lng'
      if (data.lon !== undefined && data.lng === undefined) {
        console.log("Normalizing lon to lng");
        data.lng = data.lon;
        delete data.lon;
      }
      console.log('Types:', typeof data.lat, typeof data.lng, data.lat, data.lng);
      data.lat = Number(data.lat);
      data.lng = Number(data.lng);

      if (!isNaN(data.lat) && !isNaN(data.lng)) {
        console.log("Broadcasting data");
        const out = JSON.stringify({ ...data, lng: data.lng });
        wss.clients.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(out);
          }
        });
      } else {
        console.error('Message missing or invalid lat/lng:', data);
      }
    }
  });

  console.log("WebSocket running on ws://localhost:8081");
})();
