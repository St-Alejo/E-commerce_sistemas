const amqp = require('amqplib');
const { Kafka, logLevel } = require('kafkajs');

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
const KAFKA_TOPIC = 'new_orders';
const KAFKA_GROUP = 'order_processor_group';
const EXCHANGE = 'order_tasks';
const QUEUES = ['billing_queue', 'inventory_queue', 'notification_queue'];

const kafka = new Kafka({
  clientId: 'order_processor',
  brokers: [KAFKA_BROKER],
  logLevel: logLevel.WARN,
  retry: {
    initialRetryTime: 1000,
    retries: Number.MAX_SAFE_INTEGER,
  },
});

let channel = null;
let connection = null;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForKafka() {
  const admin = kafka.admin();
  while (true) {
    try {
      await admin.connect();
      await admin.listTopics();
      await admin.disconnect();
      return;
    } catch (err) {
      console.warn(`[order_processor] Kafka no listo: ${err.message}. Reintentando...`);
      await sleep(2000);
    }
  }
}

async function connectRabbitMQ(maxAttempts = 30, delayMs = 2000) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      connection = await amqp.connect(RABBITMQ_URL);
      channel = await connection.createChannel();

      await channel.assertExchange(EXCHANGE, 'fanout', { durable: true });

      for (const queue of QUEUES) {
        await channel.assertQueue(queue, { durable: true });
        await channel.bindQueue(queue, EXCHANGE, '');
      }

      connection.on('close', () => {
        console.warn('[order_processor] Conexión RabbitMQ cerrada; reconectando...');
        channel = null;
        connection = null;
        scheduleRabbitReconnect();
      });

      connection.on('error', (err) => {
        console.error('[order_processor] Error RabbitMQ:', err.message);
      });

      console.log(`[order_processor] RabbitMQ listo (exchange: ${EXCHANGE}, fanout)`);
      return;
    } catch (err) {
      console.warn(
        `[order_processor] RabbitMQ no disponible (intento ${attempt}/${maxAttempts}):`,
        err.message
      );
      if (attempt === maxAttempts) throw err;
      await sleep(delayMs);
    }
  }
}

function scheduleRabbitReconnect() {
  setTimeout(async () => {
    try {
      await connectRabbitMQ();
    } catch (err) {
      console.error('[order_processor] Reconexión RabbitMQ fallida:', err.message);
      scheduleRabbitReconnect();
    }
  }, 3000);
}

function publishToRabbit(order) {
  if (!channel) {
    throw new Error('Canal RabbitMQ no disponible');
  }
  const payload = Buffer.from(JSON.stringify(order));
  channel.publish(EXCHANGE, '', payload, {
    contentType: 'application/json',
    persistent: true,
  });
}

async function startKafkaConsumer() {
  await waitForKafka();

  const consumer = kafka.consumer({ groupId: KAFKA_GROUP });
  await consumer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });

  console.log(`[order_processor] Consumiendo ${KAFKA_TOPIC} (group: ${KAFKA_GROUP})`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      const raw = message.value?.toString();
      if (!raw) return;

      let order;
      try {
        order = JSON.parse(raw);
      } catch {
        console.error('[order_processor] JSON inválido en Kafka, omitiendo');
        return;
      }

      try {
        publishToRabbit(order);
        console.log(
          `[order_processor] Orden ${order.order_id} publicada en exchange "${EXCHANGE}"`
        );
      } catch (err) {
        console.error(
          `[order_processor] Error publicando orden ${order.order_id}:`,
          err.message
        );
      }
    },
  });
}

async function main() {
  console.log(`[order_processor] Conectando a RabbitMQ (${RABBITMQ_URL})...`);
  await connectRabbitMQ();

  console.log(`[order_processor] Conectando a Kafka (${KAFKA_BROKER})...`);
  await startKafkaConsumer();
}

main().catch((err) => {
  console.error('[order_processor] Fallo al iniciar:', err);
  process.exit(1);
});
