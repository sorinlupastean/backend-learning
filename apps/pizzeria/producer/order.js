const amqp = require("amqplib");

const EXCHANGE = "orders";

async function main() {
  // 1. Connect to RabbitMQ
  const connection = await amqp.connect("amqp://localhost");

  // 2. Create a channel
  const channel = await connection.createChannel();

  // 3. Create a fanout exchange
  await channel.assertExchange(EXCHANGE, "fanout", {
    durable: true,
  });

  // 4. Create a pizza order
  const order = {
    id: Date.now(),
    pizza: "Margherita",
    quantity: 2,
  };

  // 5. Publish the order
  channel.publish(
    EXCHANGE,
    "",
    Buffer.from(JSON.stringify(order)),
    { persistent: true }
  );

  console.log("Order sent:", order);

  // 6. Close connection
  await channel.close();
  await connection.close();
}

main().catch(console.error);