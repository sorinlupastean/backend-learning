const {
  connectRabbitMQ,
  assertTopology,
  publishEvent,
} = require("@pizzeria/shared/rabbit");

const { DLX, QUEUES, EVENTS } = require("@pizzeria/shared/events");

const MAX_RETRIES = 2;
const RETRY_DELAY = 3000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  const { connection, channel } = await connectRabbitMQ();

  await assertTopology(channel);
  await channel.prefetch(1);

  console.log("Retry worker is waiting for failed pizzas...");

  await channel.consume(
    QUEUES.FAILED,
    async (msg) => {
      if (!msg) return;

      try {
        const event = JSON.parse(msg.content.toString());
        const retryCount = event.retryCount ?? 0;

        if (retryCount >= MAX_RETRIES) {
          console.log(`Pizza ${event.pizza.id} failed permanently`);

          channel.publish(
            DLX,
            QUEUES.PERMANENT_FAILED,
            Buffer.from(JSON.stringify(event)),
            {
              persistent: true,
              contentType: "application/json",
            },
          );

          await channel.waitForConfirms();

          await publishEvent(channel, EVENTS.PIZZA_FAILED, {
            type: EVENTS.PIZZA_FAILED,
            orderId: event.orderId,
            table: event.table,
            pizza: event.pizza,
            retryCount,
            actor: "retry-worker",
            timestamp: new Date().toISOString(),
          });

          channel.ack(msg);
          return;
        }

        console.log(`Retrying ${event.pizza.id} in 3 seconds...`);

        await publishEvent(channel, EVENTS.PIZZA_RETRYING, {
          type: EVENTS.PIZZA_RETRYING,
          orderId: event.orderId,
          table: event.table,
          pizza: event.pizza,
          retryCount: retryCount + 1,
          actor: "retry-worker",
          timestamp: new Date().toISOString(),
        });

        await sleep(RETRY_DELAY);

        const retryEvent = {
          ...event,
          retryCount: retryCount + 1,
          actor: "retry-worker",
          timestamp: new Date().toISOString(),
        };

        await publishEvent(channel, EVENTS.KITCHEN_PIZZA, retryEvent);

        channel.ack(msg);

        console.log(`Pizza sent back to kitchen, retry ${retryCount + 1}`);
      } catch (error) {
        console.error("Retry error:", error.message);

        channel.nack(msg, false, true);
      }
    },
    { noAck: false },
  );

  connection.on("error", (error) => {
    console.error("RabbitMQ error:", error.message);
  });
}

main().catch(console.error);
