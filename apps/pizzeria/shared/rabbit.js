const amqp = require("amqplib");

const { EXCHANGE, DLX, QUEUES, EVENTS } = require("./events");

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://localhost";

async function connectRabbitMQ() {
  const connection = await amqp.connect(RABBITMQ_URL);

  const channel = await connection.createConfirmChannel();

  return { connection, channel };
}

async function assertTopology(channel) {
  await channel.assertExchange(EXCHANGE, "topic", {
    durable: true,
  });

  await channel.assertExchange(DLX, "direct", {
    durable: true,
  });

  await channel.assertQueue(QUEUES.FAILED, {
    durable: true,
  });

  await channel.assertQueue(QUEUES.PERMANENT_FAILED, {
    durable: true,
  });

  await channel.bindQueue(QUEUES.FAILED, DLX, QUEUES.FAILED);

  await channel.bindQueue(
    QUEUES.PERMANENT_FAILED,
    DLX,
    QUEUES.PERMANENT_FAILED,
  );

  await channel.assertQueue(QUEUES.WAITER, {
    durable: true,
  });

  await channel.assertQueue(QUEUES.KITCHEN, {
    durable: true,
    deadLetterExchange: DLX,
    deadLetterRoutingKey: QUEUES.FAILED,
  });

  await channel.bindQueue(QUEUES.WAITER, EXCHANGE, EVENTS.ORDER_PLACED);

  await channel.bindQueue(QUEUES.WAITER, EXCHANGE, EVENTS.PIZZA_READY);

  await channel.bindQueue(QUEUES.KITCHEN, EXCHANGE, EVENTS.KITCHEN_PIZZA);

  await channel.assertQueue(QUEUES.API, {
    durable: true,
  });

  for (const routingKey of [
    EVENTS.ORDER_TAKEN,
    EVENTS.PIZZA_COOKING,
    EVENTS.PIZZA_READY,
    EVENTS.PIZZA_DELIVERED,
    EVENTS.PIZZA_FAILED,
  ]) {
    await channel.bindQueue(QUEUES.API, EXCHANGE, routingKey);
  }
}

async function publishEvent(channel, routingKey, event) {
  const message = Buffer.from(JSON.stringify(event));

  channel.publish(EXCHANGE, routingKey, message, {
    persistent: true,
    contentType: "application/json",
  });

  await channel.waitForConfirms();
}

async function publishTrace(channel, type, worker, queue, msg, event) {
  try {
    await publishEvent(channel, type, {
      type,
      worker,
      queue,
      routingKey: msg.fields.routingKey,
      orderId: event.orderId,
      pizza: event.pizza,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error("Failed to publish trace:", error.message);
  }
}

module.exports = {
  connectRabbitMQ,
  assertTopology,
  publishEvent,
  publishTrace,
};
