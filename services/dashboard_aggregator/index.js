const http = require('http');
const fs   = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const { Kafka, logLevel }  = require('kafkajs');

// [DOCKER: CONFIGURACIÓN]
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const GROUP_ID     = 'dashboard_aggregator_group';
const TOPICS       = ['billing_events', 'inventory_events', 'notification_events'];
const WS_PORT      = Number(process.env.WS_PORT)   || 8080;
const HTTP_PORT    = Number(process.env.HTTP_PORT) || 8081;

// [ESTADO: MEMORIA]
const ordersState = new Map();
const wsClients   = new Set();

// [KAFKA: CLIENTE]
const kafka = new Kafka({
  clientId: 'dashboard_aggregator',
  brokers:  [KAFKA_BROKER],
  logLevel: logLevel.WARN,
  retry: { initialRetryTime: 1000, retries: Number.MAX_SAFE_INTEGER },
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function stateAsObject() {
  const obj = {};
  for (const [id, state] of ordersState.entries()) {
    obj[id] = state;
  }
  return obj;
}

// [WEBSOCKET: DIFUSIÓN]
function broadcast(message) {
  const data = JSON.stringify(message);
  for (const client of wsClients) {
    if (client.readyState === 1) {
      client.send(data);
    }
  }
}

// [ESTADO: ACTUALIZACIÓN]
function applyEvent({ order_id, status }) {
  if (!ordersState.has(order_id)) {
    ordersState.set(order_id, { payment: 'pending', stock: 'pending', email: 'pending' });
  }

  const state = ordersState.get(order_id);

  if (status === 'PAYMENT_SUCCESS' || status === 'PAYMENT_FAILED') {
    state.payment = status;
  } else if (status === 'STOCK_RESERVED' || status === 'STOCK_OUT_OF_STOCK') {
    state.stock = status;
  } else if (status === 'EMAIL_SENT') {
    state.email = status;
  } else {
    console.warn(`[dashboard_aggregator] Status desconocido: ${status}`);
    return;
  }

  console.log(`[dashboard_aggregator] Orden #${order_id} actualizada:`, state);
  broadcast({ type: 'update', order_id, state: { ...state } });
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
      console.warn(`[dashboard_aggregator] Kafka no listo: ${err.message}. Reintentando...`);
      await sleep(2000);
    }
  }
}

// [KAFKA: CONSUMIDOR]
async function startKafkaConsumer() {
  await waitForKafka();

  const consumer = kafka.consumer({ groupId: GROUP_ID });
  await consumer.connect();
  await consumer.subscribe({ topics: TOPICS, fromBeginning: false });

  console.log(`[dashboard_aggregator] Consumiendo: ${TOPICS.join(', ')} (group: ${GROUP_ID})`);

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const raw = message.value?.toString();
      if (!raw) return;
      let event;
      try {
        event = JSON.parse(raw);
      } catch {
        console.error(`[dashboard_aggregator] JSON inválido en ${topic}`);
        return;
      }

      applyEvent(event);
    },
  });
}

// [WEBSOCKET: SERVIDOR]
function startWebSocketServer() {
  const wss = new WebSocketServer({ port: WS_PORT });

  wss.on('connection', (ws) => {
    wsClients.add(ws);
    console.log(`[dashboard_aggregator] Cliente WS conectado (total: ${wsClients.size})`);

    ws.send(JSON.stringify({ type: 'snapshot', state: stateAsObject() }));

    ws.on('close', () => {
      wsClients.delete(ws);
    });

    ws.on('error', (err) => {
      console.error('[dashboard_aggregator] Error WS:', err.message);
      wsClients.delete(ws);
    });
  });

  console.log(`[dashboard_aggregator] WebSocket en ws://0.0.0.0:${WS_PORT}`);
}

// [HTTP: SERVIDOR]
function startHttpServer() {
  const htmlPath = path.join(__dirname, 'dashboard.html');

  const server = http.createServer((req, res) => {
    if (req.method === 'GET' && (req.url === '/' || req.url === '/dashboard.html')) {
      fs.readFile(htmlPath, (err, data) => {
        if (err) {
          res.writeHead(500);
          res.end('Error al cargar el dashboard');
          return;
        }
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(data);
      });
    } else {
      res.writeHead(404); res.end('Not found');
    }
  });

  server.listen(HTTP_PORT, () =>
    console.log(`[dashboard_aggregator] Dashboard en http://0.0.0.0:${HTTP_PORT}`));
}

// [DOCKER: INICIO]
async function main() {
  startWebSocketServer();
  startHttpServer();
  console.log(`[dashboard_aggregator] Conectando a Kafka en ${KAFKA_BROKER}...`);
  await startKafkaConsumer();
}

main().catch((err) => {
  console.error('[dashboard_aggregator] Fallo al iniciar:', err);
  process.exit(1);
});
