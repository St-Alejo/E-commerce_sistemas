const amqp        = require('amqplib');
const { Kafka }   = require('kafkajs');

// [DOCKER: CONFIGURACIÓN]
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const EXCHANGE     = 'order_tasks';
const QUEUE        = 'inventory_queue';
const TOPIC        = 'inventory_events';

// [KAFKA: CLIENTE]
const kafka    = new Kafka({ clientId: 'inventory_worker', brokers: [KAFKA_BROKER] });
const producer = kafka.producer();

// [PROCESO: SIMULACIÓN]
function simulateInventory() {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve(Math.random() < 0.90
        ? 'STOCK_RESERVED'
        : 'STOCK_OUT_OF_STOCK');
    }, 100 + Math.random() * 400);
  });
}

// [RABBITMQ: CONEXIÓN]
async function connectRabbitMQ() {
  let retries = 20;
  while (retries--) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL);
      console.log('[inventory_worker] Conectado a RabbitMQ');
      return connection;
    } catch {
      console.log('[inventory_worker] Reintentando RabbitMQ...');
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  throw new Error('[inventory_worker] No se pudo conectar a RabbitMQ');
}

// [WORKER: INICIO]
async function start() {
  // [KAFKA: PRODUCTOR]
  await producer.connect();
  console.log('[inventory_worker] Conectado a Kafka');

  const connection = await connectRabbitMQ();
  const channel    = await connection.createChannel();

  // [RABBITMQ: CONFIGURACIÓN]
  await channel.assertExchange(EXCHANGE, 'fanout', { durable: true });
  await channel.assertQueue(QUEUE, { durable: true });
  await channel.bindQueue(QUEUE, EXCHANGE, '');

  console.log('[inventory_worker] Esperando órdenes en inventory_queue...');

  // [RABBITMQ: CONSUMIDOR]
  channel.consume(QUEUE, async (msg) => {
    if (!msg) return;
    try {
      const order  = JSON.parse(msg.content.toString());
      console.log('[inventory_worker] Orden recibida:', order);

      const status = await simulateInventory();
      const event  = { order_id: order.order_id, status };

      // [KAFKA: PUBLICACIÓN]
      await producer.send({
        topic:    TOPIC,
        messages: [{ key: String(order.order_id), value: JSON.stringify(event) }],
      });

      console.log('[inventory_worker] Evento enviado:', event);
      channel.ack(msg);
    } catch (err) {
      console.error('[inventory_worker] Error:', err.message);
      channel.nack(msg, false, false);
    }
  });
}

// [DOCKER: INICIO]
start();
