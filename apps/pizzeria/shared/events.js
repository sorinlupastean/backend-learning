const EXCHANGE = "pizzeria";

const DLX = "pizzeria.dlx";

const QUEUES = {
  WAITER: "waiter.tasks",
  KITCHEN: "kitchen.tasks",
  API: "api.orders",
  FAILED: "kitchen.failed",
  PERMANENT_FAILED: "kitchen.permanent-failed",
};

const EVENTS = {
  ORDER_PLACED: "order.placed",
  ORDER_TAKEN: "order.taken",
  KITCHEN_PIZZA: "kitchen.pizza",
  PIZZA_COOKING: "pizza.cooking",
  PIZZA_READY: "pizza.ready",
  PIZZA_DELIVERED: "pizza.delivered",
  PIZZA_BURNED: "pizza.burned",
  PIZZA_RETRYING: "pizza.retrying",
  PIZZA_FAILED: "pizza.failed",
  ORDER_COMPLETED: "order.completed",
  ORDER_FAILED: "order.failed",
  TRACE_RECEIVED: "trace.received",
  TRACE_ACKED: "trace.acked",
  TRACE_NACKED: "trace.nacked",
};

module.exports = {
  EXCHANGE,
  DLX,
  QUEUES,
  EVENTS,
};
