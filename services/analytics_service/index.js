const { Kafka, logLevel } = require('kafkajs');

// [DOCKER: CONFIGURACIÓN]
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092'; // host kafka
const TOPIC        = 'new_orders';                              // canal entrada
const GROUP_ID     = 'analytics_group';                         // id grupo independiente
const WINDOW_MS    = 60_000;                                    // ventana 1 min

let ordersInWindow = 0; // contador local

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
  console.log(`[analytics_service] Ventas/min: ${ordersInWindow}`); // métrica minuto
  ordersInWindow = 0; // reset
}

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
      console.warn(`[analytics_service] Kafka no listo. Reintentando...`);
      await sleep(2000);
    }
  }
}

// [KAFKA: CONSUMIDOR]
async function startConsumer() {
  await waitForKafka();

  const consumer = kafka.consumer({ groupId: GROUP_ID }); // consumidor independiente
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false }); // leer nuevos pedidos

  console.log(`[analytics_service] Observador activo`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      const raw = message.value?.toString();
      if (!raw) return;

      let order;
      try {
        order = JSON.parse(raw); // leer pedido
      } catch {
        console.error('[analytics_service] JSON inválido');
        return;
      }

      ordersInWindow += 1; // incrementar métrica
      console.log(`[analytics_service] Registro: #${order.order_id}`);
    },
  });
}

// [DOCKER: INICIO]
async function main() {
  console.log(`[analytics_service] Iniciando...`);
  setInterval(printWindowSummary, WINDOW_MS); // ciclo reporte
  await startConsumer(); // bucle principal
}

main().catch((err) => {
  console.error('[analytics_service] Fallo:', err);
  process.exit(1);
});
