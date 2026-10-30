const {
  connectRabbitMQ,
  assertTopology,
  publishEvent,
  publishTrace,
} = require("@pizzeria/shared/rabbit");

const { QUEUES, EVENTS } = require("@pizzeria/shared/events");
const { MENU } = require("@pizzeria/shared/menu");

const CHEF_NAME = process.env.CHEF_NAME || "Mario";
const BURN_CHANCE = Number(process.env.BURN_CHANCE ?? 0.3);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  const { connection, channel } = await connectRabbitMQ();

  await assertTopology(channel);
  await channel.prefetch(1);

  console.log(`${CHEF_NAME} is waiting for pizzas...`);

  await channel.consume(
    QUEUES.KITCHEN,
    async (msg) => {
      if (!msg) return;

      try {
        const event = JSON.parse(msg.content.toString());

        await publishTrace(
          channel,
          EVENTS.TRACE_RECEIVED,
          CHEF_NAME,
          QUEUES.KITCHEN,
          msg,
          event,
        );

        const pizza = MENU.find((item) => item.id === event.pizza?.id);

        if (!pizza) {
          throw new Error("Unknown pizza");
        }

        const attempt = (event.retryCount ?? 0) + 1;

        console.log(
          `${CHEF_NAME} is cooking ${pizza.name}, attempt ${attempt}`,
        );

        await publishEvent(channel, EVENTS.PIZZA_COOKING, {
          type: EVENTS.PIZZA_COOKING,
          orderId: event.orderId,
          table: event.table,
          pizza: event.pizza,
          actor: CHEF_NAME,
          timestamp: new Date().toISOString(),
        });

        await sleep(pizza.cookSeconds * 1000);

        // Simulate a burnt pizza
        if (Math.random() < BURN_CHANCE) {
          console.log(`${CHEF_NAME} burned ${pizza.name}!`);

          await publishEvent(channel, EVENTS.PIZZA_BURNED, {
            type: EVENTS.PIZZA_BURNED,
            orderId: event.orderId,
            table: event.table,
            pizza: event.pizza,
            retryCount: event.retryCount ?? 0,
            actor: CHEF_NAME,
            timestamp: new Date().toISOString(),
          });

          channel.nack(msg, false, false);

          await publishTrace(
            channel,
            EVENTS.TRACE_NACKED,
            CHEF_NAME,
            QUEUES.KITCHEN,
            msg,
            event,
          );
          return;
        }

        await publishEvent(channel, EVENTS.PIZZA_READY, {
          type: EVENTS.PIZZA_READY,
          orderId: event.orderId,
          table: event.table,
          pizza: event.pizza,
          actor: CHEF_NAME,
          timestamp: new Date().toISOString(),
        });

        channel.ack(msg);

        await publishTrace(
          channel,
          EVENTS.TRACE_ACKED,
          CHEF_NAME,
          QUEUES.KITCHEN,
          msg,
          event,
        );

        console.log(`${CHEF_NAME} finished ${pizza.name}`);
      } catch (error) {
        console.error(`${CHEF_NAME} error:`, error.message);

        channel.nack(msg, false, false);
      }
    },
    { noAck: false },
  );

  connection.on("error", (error) => {
    console.error("RabbitMQ error:", error.message);
  });
}

main().catch(console.error);
