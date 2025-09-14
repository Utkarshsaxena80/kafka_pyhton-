const {Kafka}=require("kafkajs")

const kafka= new Kafka({
    clientId: "my-app",
    brokers:["192.168.1.6:9092"]
})

async function init() {
  const consumer = kafka.consumer({ groupId: "user-1" });
  await consumer.connect();

  await consumer.subscribe({
    topics: ["rider-updates"], 
    fromBeginning: true,       
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(
        `[${topic}] PART:${partition} => ${message.offset} | ${message.value.toString()}`
      );
    },
  });
}
    

init().catch(console.error);
