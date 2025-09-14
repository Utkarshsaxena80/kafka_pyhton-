const {Kafka}=require("kafkajs")

const kafka= new Kafka({
    clientId: "my-app",
    brokers:["192.168.1.6:9092"]
})
// ... (kafka and producer setup) ...

async function init() {
    const producer = kafka.producer();
    try {
        console.log('Connecting producer...');
        await producer.connect();
        console.log('Producer connected.');

        await producer.send({
            topic: 'rider-updates',
            messages: [{
                key: 'location-update',
                value: JSON.stringify({
                    name: "Tony stark",
                    "loc": "south",
                })
            }]
        });
        console.log("Message sent successfully!");
    } catch (error) {
        console.error("Error sending message:", error);
    } finally {
        await producer.disconnect();
        console.log("Producer disconnected.");
    }
}

init();