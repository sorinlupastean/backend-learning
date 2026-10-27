const amqp = require("amqplib");

const QUEUE = "tasks";

async function main() {
    const connection = await amqp.connect("amqp://localhost");
    const channel = await connection.createChannel();

    await channel.assertQueue(QUEUE, { durable: true });
    await channel.prefetch(1);

    console.log("Waiting for messages...");

    channel.consume(QUEUE, async (msg) => {
        const task = JSON.parse(msg.content.toString());
        console.log("Received:", task);

        await new Promise((resolve) => setTimeout(resolve, 2000));

        console.log("Done:", task.title);
        channel.ack(msg);
    });
}

main().catch((err) => {
    console.error("Failed:", err.message);
    process.exit(1);
});
