const { randomUUID } = require("node:crypto");

const {
  connectRabbitMQ,
  assertTopology,
  publishEvent,
  publishTrace,
} = require("@pizzeria/shared/rabbit");

const { QUEUES, EVENTS } = require("@pizzeria/shared/events");

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  const { connection, channel } = await connectRabbitMQ();

  await assertTopology(channel);
  await channel.prefetch(1);

  console.log("Waiter is waiting for orders...");

  await channel.consume(
    QUEUES.WAITER,
    async (msg) => {
      if (!msg) return;

      try {
        const event = JSON.parse(msg.content.toString());

        await publishTrace(
          channel,
          EVENTS.TRACE_RECEIVED,
          "waiter",
          QUEUES.WAITER,
          msg,
          event,
        );

        if (event.type === EVENTS.ORDER_PLACED) {
          console.log(`Going to table ${event.table}...`);

          await sleep(2000);

          await publishEvent(channel, EVENTS.ORDER_TAKEN, {
            type: EVENTS.ORDER_TAKEN,
            orderId: event.orderId,
            table: event.table,
            actor: "waiter",
            timestamp: new Date().toISOString(),
          });

          for (const item of event.items) {
            for (let i = 0; i < item.qty; i++) {
              const pizzaEvent = {
                type: EVENTS.KITCHEN_PIZZA,
                orderId: event.orderId,
                table: event.table,
                pizza: {
                  id: item.pizzaId,
                  instanceId: randomUUID(),
                },
                actor: "waiter",
                timestamp: new Date().toISOString(),
              };

              await publishEvent(channel, EVENTS.KITCHEN_PIZZA, pizzaEvent);

              console.log("Sent to kitchen:", item.pizzaId);
            }
          }
        } else if (event.type === EVENTS.PIZZA_READY) {
          console.log(`Pizza ready: ${event.pizza.id}`);
          console.log(`Delivering to table ${event.table}...`);

          await sleep(2000);

          await publishEvent(channel, EVENTS.PIZZA_DELIVERED, {
            type: EVENTS.PIZZA_DELIVERED,
            orderId: event.orderId,
            table: event.table,
            pizza: event.pizza,
            actor: "waiter",
            timestamp: new Date().toISOString(),
          });

          console.log(`Delivered: ${event.pizza.id}`);
        }

        channel.ack(msg);

        await publishTrace(
          channel,
          EVENTS.TRACE_ACKED,
          "waiter",
          QUEUES.WAITER,
          msg,
          event,
        );
      } catch (error) {
        console.error("Waiter error:", error.message);
        channel.nack(msg, false, false);
      }
    },
    { noAck: false },
  );

  connection.on("error", (error) => {
    console.error("RabbitMQ connection error:", error.message);
  });
}

main().catch(console.error);
