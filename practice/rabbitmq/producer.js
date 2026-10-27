const amqp = require("amqplib");

const QUEUE = "tasks";

async function main() {
  const connection = await amqp.connect("amqp://localhost");
  const channel = await connection.createChannel();

  await channel.assertQueue(QUEUE, { durable: true });

  const message = {
    title: "Learn RabbitMQ",
    createdAt: new Date().toISOString(),
  };
  channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(message)), {
    persistent: true,
  });
  console.log("Sent:", message);

  await channel.close();
  await connection.close();
}

main().catch((err) => {
    console.error("Failed:", err.message);
    process.exit(1);
});
