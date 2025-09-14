const {Kafka}=require("kafkajs")

const kafka= new Kafka({
    clientId: "my-app",
    brokers:["192.168.1.6:9092"]
})

async function init(){
    const admin=kafka.admin();
    console.log("admin connection ..")
 await admin.connect();
    console.log("Admin connecting")   
    
 await    admin.createTopics({
        topics:[{
             topic:"ride-updated", 
             numPartitions:2   ,
              replicationFactor: 1,
        }]
    })
    await admin.disconnect()
    console.log("Admin disconnectiong")
}
init()