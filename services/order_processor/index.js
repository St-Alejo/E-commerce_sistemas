const amqp = require('amqplib');
const { Kafka, logLevel } = require('kafkajs');

// [DOCKER: CONFIGURACIÓN]
const KAFKA_BROKER  = process.env.KAFKA_BROKER  || 'kafka:9092'; // host kafka
const RABBITMQ_URL  = process.env.RABBITMQ_URL  || 'amqp://guest:guest@rabbitmq:5672'; // url rabbitmq
const KAFKA_TOPIC   = 'new_orders';             // canal entrada
const KAFKA_GROUP   = 'order_processor_group';  // grupo consumidor
const EXCHANGE      = 'order_tasks';            // distribuidor tareas
const QUEUES        = ['billing_queue', 'inventory_queue', 'notification_queue']; // colas destino

// [KAFKA: CLIENTE]
const kafka = new Kafka({
  clientId: 'order_processor',
  brokers:  [KAFKA_BROKER],
  logLevel: logLevel.WARN,
  retry: { initialRetryTime: 1000, retries: Number.MAX_SAFE_INTEGER },
});

let channel    = null;
let connection = null;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// [KAFKA: CONEXIÓN]
async function waitForKafka() {
  const admin = kafka.admin();
  while (true) {
    try {
      await admin.connect();
      await admin.listTopics();
      await admin.disconnect();
      return; // kafka listo
    } catch (err) {
      console.warn(`[order_processor] Kafka no listo. Reintentando...`);
      await sleep(2000);
    }
  }
}

// [RABBITMQ: CONEXIÓN]
async function connectRabbitMQ(maxAttempts = 30, delayMs = 2000) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      connection = await amqp.connect(RABBITMQ_URL); // conectar amqp
      channel    = await connection.createChannel(); // crear canal

      // [RABBITMQ: EXCHANGE]
      await channel.assertExchange(EXCHANGE, 'fanout', { durable: true }); // exchange fanout (megáfono)

      // [RABBITMQ: COLAS]
      for (const queue of QUEUES) {
        await channel.assertQueue(queue, { durable: true }); // asegurar colas
        await channel.bindQueue(queue, EXCHANGE, '');        // vincular colas
      }

      connection.on('close', () => {
        console.warn('[order_processor] RabbitMQ desconectado. Reconectando...');
        channel    = null;
        connection = null;
        scheduleRabbitReconnect();
      });
      connection.on('error', (err) =>
        console.error('[order_processor] Error RabbitMQ:', err.message));

      console.log(`[order_processor] RabbitMQ listo`);
      return;
    } catch (err) {
      console.warn(`[order_processor] RabbitMQ no disponible (${attempt}/${maxAttempts})`);
      if (attempt === maxAttempts) throw err;
      await sleep(delayMs);
    }
  }
}

function scheduleRabbitReconnect() {
  setTimeout(async () => {
    try { await connectRabbitMQ(); }
    catch (err) {
      console.error('[order_processor] Reconexión fallida');
      scheduleRabbitReconnect();
    }
  }, 3000); // reintento cada 3s
}

// [RABBITMQ: PUBLICACIÓN]
function publishToRabbit(order) {
  if (!channel) throw new Error('RabbitMQ no disponible');
  channel.publish(
    EXCHANGE,
    '',
    Buffer.from(JSON.stringify(order)), // serializar orden
    {
      contentType: 'application/json',
      persistent: true, // persistencia en disco
    }
  );
}

// [KAFKA: CONSUMIDOR]
async function startKafkaConsumer() {
  await waitForKafka();

  const consumer = kafka.consumer({ groupId: KAFKA_GROUP });
  await consumer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false }); // leer nuevos pedidos

  console.log(`[order_processor] Puente Kafka -> RabbitMQ activo`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      const raw = message.value?.toString();
      if (!raw) return;

      let order;
      try {
        order = JSON.parse(raw); // parsear pedido kafka
      } catch {
        console.error('[order_processor] JSON inválido');
        return;
      }

      try {
        publishToRabbit(order); // pasar a rabbitmq (fanout)
        console.log(`[order_processor] Orden #${order.order_id} enviada a RabbitMQ`);
      } catch (err) {
        console.error(`[order_processor] Error puente:`, err.message);
      }
    },
  });
}

// [DOCKER: INICIO]
async function main() {
  console.log(`[order_processor] Iniciando...`);
  await connectRabbitMQ();
  await startKafkaConsumer(); // bucle principal
}

main().catch((err) => {
  console.error('[order_processor] Fallo:', err);
  process.exit(1);
});
