const amqp        = require('amqplib');
const { Kafka }   = require('kafkajs');

// [DOCKER: CONFIGURACIÓN]
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672'; // url rabbitmq
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';                        // host kafka
const EXCHANGE     = 'order_tasks';       // exchange entrada
const QUEUE        = 'inventory_queue';   // cola stock
const TOPIC        = 'inventory_events';  // canal resultados

// [KAFKA: CLIENTE]
const kafka    = new Kafka({ clientId: 'inventory_worker', brokers: [KAFKA_BROKER] });
const producer = kafka.producer();

// [PROCESO: SIMULACIÓN]
function simulateInventory() {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve(Math.random() < 0.90
        ? 'STOCK_RESERVED' // éxito 90%
        : 'STOCK_OUT_OF_STOCK'); // fallo 10%
    }, 100 + Math.random() * 400); // latencia
  });
}

// [RABBITMQ: CONEXIÓN]
async function connectRabbitMQ() {
  let retries = 20;
  while (retries--) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL); // conectar rabbitmq
      console.log('[inventory_worker] Conectado a RabbitMQ');
      return connection;
    } catch {
      console.log('[inventory_worker] Reintentando...');
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  throw new Error('RabbitMQ error');
}

// [WORKER: INICIO]
async function start() {
  // [KAFKA: PRODUCTOR]
  await producer.connect(); // conectar productor
  console.log('[inventory_worker] Conectado a Kafka');

  const connection = await connectRabbitMQ();
  const channel    = await connection.createChannel();

  // [RABBITMQ: CONFIGURACIÓN]
  await channel.assertExchange(EXCHANGE, 'fanout', { durable: true }); // exchange persistente
  await channel.assertQueue(QUEUE, { durable: true });                 // cola persistente
  await channel.bindQueue(QUEUE, EXCHANGE, '');

  console.log('[inventory_worker] Esperando tareas...');

  // [RABBITMQ: CONSUMIDOR]
  channel.consume(QUEUE, async (msg) => {
    if (!msg) return;
    try {
      const order  = JSON.parse(msg.content.toString()); // leer tarea
      console.log('[inventory_worker] Procesando:', order.order_id);

      const status = await simulateInventory(); // ejecutar stock
      const event  = { order_id: order.order_id, status };

      // [KAFKA: PUBLICACIÓN]
      await producer.send({
        topic:    TOPIC, // feedback a kafka
        messages: [{ key: String(order.order_id), value: JSON.stringify(event) }],
      });

      console.log('[inventory_worker] Evento enviado');
      channel.ack(msg); // confirmar rabbitmq
    } catch (err) {
      console.error('[inventory_worker] Error');
      channel.nack(msg, false, false); // descartar
    }
  });
}

// [DOCKER: INICIO]
start();
