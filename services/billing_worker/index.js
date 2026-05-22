const amqp        = require('amqplib');
const { Kafka }   = require('kafkajs');

// [DOCKER: CONFIGURACIÓN]
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672'; // url rabbitmq
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';                        // host kafka
const EXCHANGE     = 'order_tasks';    // exchange entrada
const QUEUE        = 'billing_queue';  // cola pagos
const TOPIC        = 'billing_events'; // canal resultados

// [KAFKA: CLIENTE]
const kafka    = new Kafka({ clientId: 'billing_worker', brokers: [KAFKA_BROKER] });
const producer = kafka.producer();

// [PROCESO: SIMULACIÓN]
function simulatePayment() {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve(Math.random() < 0.85
        ? 'PAYMENT_SUCCESS' // éxito 85%
        : 'PAYMENT_FAILED'); // fallo 15%
    }, 200 + Math.random() * 600); // latencia
  });
}

// [RABBITMQ: CONEXIÓN]
async function connectRabbitMQ() {
  let retries = 20;
  while (retries--) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL); // conectar rabbitmq
      console.log('[billing_worker] Conectado a RabbitMQ');
      return connection;
    } catch {
      console.log('[billing_worker] Reintentando...');
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  throw new Error('RabbitMQ error');
}

// [WORKER: INICIO]
async function startWorker() {
  // [KAFKA: PRODUCTOR]
  await producer.connect(); // conectar productor
  console.log('[billing_worker] Conectado a Kafka');

  const connection = await connectRabbitMQ();
  const channel    = await connection.createChannel();

  // [RABBITMQ: CONFIGURACIÓN]
  await channel.assertExchange(EXCHANGE, 'fanout', { durable: true }); // exchange persistente
  await channel.assertQueue(QUEUE, { durable: true });                 // cola persistente
  await channel.bindQueue(QUEUE, EXCHANGE, '');

  console.log('[billing_worker] Esperando tareas...');

  // [RABBITMQ: CONSUMIDOR]
  channel.consume(QUEUE, async (msg) => {
    if (!msg) return;
    try {
      const order  = JSON.parse(msg.content.toString()); // leer tarea
      console.log('[billing_worker] Procesando:', order.order_id);

      const status = await simulatePayment(); // ejecutar pago
      const event  = { order_id: order.order_id, status };

      // [KAFKA: PUBLICACIÓN]
      await producer.send({
        topic:    TOPIC, // feedback a kafka
        messages: [{ key: String(order.order_id), value: JSON.stringify(event) }],
      });

      console.log('[billing_worker] Evento enviado');
      channel.ack(msg); // confirmar rabbitmq
    } catch (err) {
      console.error('[billing_worker] Error');
      channel.nack(msg, false, false); // descartar
    }
  });
}

// [DOCKER: INICIO]
startWorker();
