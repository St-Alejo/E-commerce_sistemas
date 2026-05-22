const express = require('express');
const path = require('path');
const { Kafka, logLevel } = require('kafkajs');

// [DOCKER: CONFIGURACIÓN]
const PORT         = Number(process.env.PORT)         || 3000;
const KAFKA_BROKER = process.env.KAFKA_BROKER         || 'kafka:9092';
const TOPIC        = 'new_orders';

let nextOrderId = 1;
let producer   = null;

// [KAFKA: CLIENTE]
const kafka = new Kafka({
  clientId: 'order_api',
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

// [KAFKA: PRODUCTOR]
async function connectProducer() {
  const p = kafka.producer();
  await p.connect();
  return p;
}

// [KAFKA: CONEXIÓN]
async function waitForProducer() {
  while (true) {
    try {
      producer = await connectProducer();
      console.log(`[order_api] Productor conectado (topic: ${TOPIC})`);
      return;
    } catch (err) {
      console.warn(`[order_api] Kafka no disponible: ${err.message}. Reintentando...`);
      await sleep(2000);
    }
  }
}

async function ensureProducer() {
  if (!producer) {
    producer = await connectProducer();
  }
}

function validateOrderBody(body) {
  if (!body || typeof body !== 'object') {
    return 'El cuerpo debe ser un objeto JSON';
  }
  if (typeof body.item !== 'string' || body.item.trim() === '') {
    return 'El campo "item" es obligatorio y debe ser un string no vacío';
  }
  return null;
}

// [KAFKA: PUBLICACIÓN]
async function publishOrder(order) {
  try {
    await producer.send({
      topic:    TOPIC,
      messages: [{ value: JSON.stringify(order) }],
    });
  } catch (err) {
    producer = null;
    await ensureProducer();
    await producer.send({
      topic:    TOPIC,
      messages: [{ value: JSON.stringify(order) }],
    });
  }
}

// [HTTP: SERVIDOR]
async function main() {
  await waitForProducer();

  const app = express();
  app.use(express.json());

  // [HTTP: RUTAS]
  app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'store.html'));
  });

  app.post('/order', async (req, res) => {
    const validationError = validateOrderBody(req.body);
    if (validationError) {
      return res.status(400).json({ error: validationError });
    }

    const order = {
      order_id: nextOrderId++,
      item:     req.body.item.trim(),
    };

    try {
      await publishOrder(order);
      console.log('[order_api] Orden publicada:', order);
      return res.status(201).json(order);
    } catch (err) {
      return res.status(503).json({ error: 'No se pudo publicar la orden en Kafka' });
    }
  });

  app.listen(PORT, () => {
    console.log(`[order_api] Escuchando en http://0.0.0.0:${PORT}`);
  });
}

// [DOCKER: INICIO]
main().catch((err) => {
  console.error('[order_api] Fallo al iniciar:', err);
  process.exit(1);
});
