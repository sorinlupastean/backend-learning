const http = require("node:http");
const { randomUUID } = require("node:crypto");

const { MENU } = require("@pizzeria/shared/menu");
const { EVENTS, QUEUES } = require("@pizzeria/shared/events");

const {
  connectRabbitMQ,
  assertTopology,
  publishEvent,
} = require("@pizzeria/shared/rabbit");

const PORT = 4000;

const RABBITMQ_MANAGEMENT_URL =
  process.env.RABBITMQ_MANAGEMENT_URL || "http://localhost:15672";

const RABBITMQ_USER = process.env.RABBITMQ_USER || "guest";

const RABBITMQ_PASSWORD = process.env.RABBITMQ_PASSWORD || "guest";

const orders = new Map();

const sseClients = new Set();

function broadcastEvent(event) {
  const data = `data: ${JSON.stringify(event)}\n\n`;

  for (const client of sseClients) {
    client.write(data);
  }
}

function sendJSON(res, statusCode, data) {
  res.writeHead(statusCode, {
    "Content-Type": "application/json",
  });

  res.end(JSON.stringify(data));
}

async function readJSON(req) {
  let body = "";

  for await (const chunk of req) {
    body += chunk;
  }

  return JSON.parse(body);
}

async function getRabbitMQQueues() {
  const credentials = Buffer.from(
    `${RABBITMQ_USER}:${RABBITMQ_PASSWORD}`,
  ).toString("base64");

  const response = await fetch(`${RABBITMQ_MANAGEMENT_URL}/api/queues/%2F`, {
    headers: {
      Authorization: `Basic ${credentials}`,
    },
    signal: AbortSignal.timeout(5000),
  });

  if (!response.ok) {
    throw new Error(`RabbitMQ Management API returned ${response.status}`);
  }

  const queues = await response.json();

  return queues
    .filter((queue) => Object.values(QUEUES).includes(queue.name))
    .map((queue) => ({
      name: queue.name,
      ready: queue.messages_ready ?? 0,
      unacked: queue.messages_unacknowledged ?? 0,
      consumers: queue.consumers ?? 0,
    }));
}

async function main() {
  const { connection, channel } = await connectRabbitMQ();

  await assertTopology(channel);

  const monitor = await channel.assertQueue("", {
    exclusive: true,
    autoDelete: true,
  });

  await channel.bindQueue(monitor.queue, "pizzeria", "#");

  await channel.consume(
    monitor.queue,
    (msg) => {
      if (!msg) return;

      try {
        const event = JSON.parse(msg.content.toString());

        broadcastEvent(event);
      } catch (error) {
        console.error("Monitor error:", error.message);
      }
    },
    { noAck: true },
  );

  console.log("RabbitMQ monitor is running");

  await channel.consume(
    QUEUES.API,
    async (msg) => {
      if (!msg) return;

      try {
        const event = JSON.parse(msg.content.toString());
        const order = orders.get(event.orderId);

        if (!order) {
          channel.ack(msg);
          return;
        }

        if (event.type === EVENTS.ORDER_TAKEN) {
          if (order.status === "placed") {
            order.status = "taken";
          }
        }

        if (event.type === EVENTS.PIZZA_COOKING) {
          if (["placed", "taken"].includes(order.status)) {
            order.status = "cooking";
          }
        }

        if (event.type === EVENTS.PIZZA_READY) {
          if (!["completed", "failed"].includes(order.status)) {
            order.status = "delivering";
          }
        }

        if (event.type === EVENTS.PIZZA_FAILED) {
          order.failedPizzas++;

          if (order.status !== "failed") {
            order.status = "failed";
            order.failedAt = new Date().toISOString();

            await publishEvent(channel, EVENTS.ORDER_FAILED, {
              type: EVENTS.ORDER_FAILED,
              orderId: order.orderId,
              table: order.table,
              pizza: event.pizza,
              actor: "api",
              timestamp: order.failedAt,
            });

            console.log("Order failed:", order.orderId);
          }
        }

        if (event.type === EVENTS.PIZZA_DELIVERED) {
          const pizzaId = event.pizza?.instanceId;

          if (!pizzaId) {
            throw new Error("Missing pizza instanceId");
          }

          if (!order.deliveredIds.has(pizzaId)) {
            order.deliveredIds.add(pizzaId);
            order.deliveredPizzas++;
          }

          if (
            order.deliveredPizzas === order.totalPizzas &&
            order.status !== "completed"
          ) {
            order.status = "completed";
            order.completedAt = new Date().toISOString();

            await publishEvent(channel, EVENTS.ORDER_COMPLETED, {
              type: EVENTS.ORDER_COMPLETED,
              orderId: order.orderId,
              table: order.table,
              actor: "api",
              timestamp: order.completedAt,
            });

            console.log("Order completed:", order.orderId);
          }
        }

        console.log("Order status:", order.orderId, order.status);

        channel.ack(msg);
      } catch (error) {
        console.error("Status processing error:", error);
        channel.nack(msg, false, false);
      }
    },
    { noAck: false },
  );

  const server = http.createServer(async (req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");

    if (req.method === "OPTIONS") {
      res.writeHead(204);
      return res.end();
    }

    if (req.method === "GET" && req.url === "/health") {
      return sendJSON(res, 200, { status: "ok" });
    }

    if (req.method === "GET" && req.url === "/api/menu") {
      return sendJSON(res, 200, MENU);
    }

    if (req.method === "GET" && req.url === "/api/rabbit/queues") {
      try {
        const queues = await getRabbitMQQueues();

        return sendJSON(res, 200, queues);
      } catch (error) {
        console.error("RabbitMQ Management error:", error.message);

        return sendJSON(res, 502, {
          error: "Failed to get RabbitMQ queues",
        });
      }
    }

    if (req.method === "GET" && req.url === "/api/events") {
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      });

      res.write(
        `data: ${JSON.stringify({
          type: "connected",
          timestamp: new Date().toISOString(),
        })}\n\n`,
      );

      sseClients.add(res);

      req.on("close", () => {
        sseClients.delete(res);
      });

      return;
    }

    if (req.method === "GET" && req.url.startsWith("/api/orders/")) {
      const orderId = req.url.split("/")[3];
      const order = orders.get(orderId);

      if (!order) {
        return sendJSON(res, 404, {
          error: "Order not found",
        });
      }

      const { deliveredIds, ...response } = order;

      return sendJSON(res, 200, response);
    }

    // Place an order
    if (req.method === "POST" && req.url === "/api/orders") {
      try {
        const { table, items } = await readJSON(req);

        if (!Number.isInteger(table) || table < 1) {
          return sendJSON(res, 400, {
            error: "Invalid table number",
          });
        }

        if (!Array.isArray(items) || items.length === 0) {
          return sendJSON(res, 400, {
            error: "Order must contain at least one pizza",
          });
        }

        const validItems = items.every(
          (item) =>
            MENU.some((pizza) => pizza.id === item.pizzaId) &&
            Number.isInteger(item.qty) &&
            item.qty > 0,
        );

        if (!validItems) {
          return sendJSON(res, 400, {
            error: "Invalid pizza or quantity",
          });
        }

        const orderId = randomUUID();

        const order = {
          orderId,
          table,
          items,
          status: "placed",
          totalPizzas: items.reduce((sum, item) => sum + item.qty, 0),
          deliveredPizzas: 0,
          deliveredIds: new Set(),
          failedPizzas: 0,
          createdAt: new Date().toISOString(),
          completedAt: null,
          failedAt: null,
        };

        orders.set(orderId, order);

        const event = {
          type: EVENTS.ORDER_PLACED,
          orderId,
          table,
          items,
          actor: "customer",
          timestamp: order.createdAt,
        };

        try {
          await publishEvent(channel, EVENTS.ORDER_PLACED, event);
        } catch (error) {
          orders.delete(orderId);
          throw error;
        }

        console.log("Order placed:", orderId);

        return sendJSON(res, 202, { orderId });
      } catch (error) {
        if (error instanceof SyntaxError) {
          return sendJSON(res, 400, {
            error: "Invalid JSON",
          });
        }

        console.error("Order error:", error);

        return sendJSON(res, 500, {
          error: "Failed to create order",
        });
      }
    }

    return sendJSON(res, 404, {
      error: "Not found",
    });
  });

  server.listen(PORT, () => {
    console.log(`Pizzeria API running at http://localhost:${PORT}`);
  });

  connection.on("error", (error) => {
    console.error("RabbitMQ connection error:", error.message);
  });
}

main().catch((error) => {
  console.error("Failed to start API:", error);
  process.exit(1);
});
