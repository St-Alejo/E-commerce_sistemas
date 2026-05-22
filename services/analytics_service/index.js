const { Kafka, logLevel } = require('kafkajs');

// [DOCKER: CONFIGURACIÓN]
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const TOPIC        = 'new_orders';
const GROUP_ID     = 'analytics_group';
const WINDOW_MS    = 60_000;

let ordersInWindow = 0;

// [KAFKA: CLIENTE]
const kafka = new Kafka({
  clientId: 'analytics_service',
  brokers:  [KAFKA_BROKER],
  logLevel: logLevel.WARN,
  retry: {
    initialRetryTime: 1000,
    retries: Number.MAX_SAFE_INTEGER,
  },
});

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function printWindowSummary() {
  console.log(`[analytics_service] Total ventas último minuto: ${ordersInWindow}`);
  ordersInWindow = 0;
}

// [KAFKA: CONEXIÓN]
async function waitForKafka() {
  const admin = kafka.admin();
  while (true) {
    try {
      await admin.connect();
      await admin.listTopics();
      await admin.disconnect();
      return;
    } catch (err) {
      console.warn(`[analytics_service] Kafka no listo: ${err.message}. Reintentando...`);
      await sleep(2000);
    }
  }
}

// [KAFKA: CONSUMIDOR]
async function startConsumer() {
  await waitForKafka();

  const consumer = kafka.consumer({ groupId: GROUP_ID });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

  console.log(`[analytics_service] Consumiendo ${TOPIC} (group: ${GROUP_ID}), resumen cada 60s`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      const raw = message.value?.toString();
      if (!raw) return;

      let order;
      try {
        order = JSON.parse(raw);
      } catch {
        console.error('[analytics_service] JSON inválido, omitiendo');
        return;
      }

      ordersInWindow += 1;
      console.log(`[analytics_service] Orden recibida #${order.order_id} — "${order.item}"`);
    },
  });
}

// [DOCKER: INICIO]
async function main() {
  console.log(`[analytics_service] Conectando a Kafka en ${KAFKA_BROKER}...`);
  setInterval(printWindowSummary, WINDOW_MS);
  await startConsumer();
}

main().catch((err) => {
  console.error('[analytics_service] Fallo al iniciar:', err);
  process.exit(1);
});
