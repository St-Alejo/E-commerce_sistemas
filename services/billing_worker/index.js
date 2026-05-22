const amqp        = require('amqplib');
const { Kafka }   = require('kafkajs');

// [DOCKER: CONFIGURACIÓN]
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const EXCHANGE     = 'order_tasks';
const QUEUE        = 'billing_queue';
const TOPIC        = 'billing_events';

// [KAFKA: CLIENTE]
const kafka    = new Kafka({ clientId: 'billing_worker', brokers: [KAFKA_BROKER] });
const producer = kafka.producer();

// [PROCESO: SIMULACIÓN]
function simulatePayment() {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve(Math.random() < 0.85
        ? 'PAYMENT_SUCCESS'
        : 'PAYMENT_FAILED');
    }, 200 + Math.random() * 600);
  });
}

// [RABBITMQ: CONEXIÓN]
async function connectRabbitMQ() {
  let retries = 20;
  while (retries--) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL);
      console.log('[billing_worker] Conectado a RabbitMQ');
      return connection;
    } catch {
      console.log('[billing_worker] Reintentando RabbitMQ...');
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  throw new Error('[billing_worker] No se pudo conectar a RabbitMQ');
}

// [WORKER: INICIO]
async function startWorker() {
  // [KAFKA: PRODUCTOR]
  await producer.connect();
  console.log('[billing_worker] Conectado a Kafka');

  const connection = await connectRabbitMQ();
  const channel    = await connection.createChannel();

  // [RABBITMQ: CONFIGURACIÓN]
  await channel.assertExchange(EXCHANGE, 'fanout', { durable: true });
  await channel.assertQueue(QUEUE, { durable: true });
  await channel.bindQueue(QUEUE, EXCHANGE, '');

  console.log('[billing_worker] Esperando órdenes en billing_queue...');

  // [RABBITMQ: CONSUMIDOR]
  channel.consume(QUEUE, async (msg) => {
    if (!msg) return;
    try {
      const order  = JSON.parse(msg.content.toString());
      console.log('[billing_worker] Orden recibida:', order);

      const status = await simulatePayment();
      const event  = { order_id: order.order_id, status };

      // [KAFKA: PUBLICACIÓN]
      await producer.send({
        topic:    TOPIC,
        messages: [{ key: String(order.order_id), value: JSON.stringify(event) }],
      });

      console.log('[billing_worker] Evento enviado:', event);
      channel.ack(msg);
    } catch (err) {
      console.error('[billing_worker] Error procesando orden:', err.message);
      channel.nack(msg, false, false);
    }
  });
}

// [DOCKER: INICIO]
startWorker();
