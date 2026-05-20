const express = require('express');
const { Kafka, logLevel } = require('kafkajs');

const PORT = Number(process.env.PORT) || 3000;
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const TOPIC = 'new_orders';

let nextOrderId = 1;
let producer = null;

const kafka = new Kafka({
  clientId: 'order_api',
  brokers: [KAFKA_BROKER],
  logLevel: logLevel.WARN,
  retry: {
    initialRetryTime: 1000,
    retries: Number.MAX_SAFE_INTEGER,
  },
});

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function connectProducer() {
  const p = kafka.producer();
  await p.connect();
  return p;
}

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
    console.log('[order_api] Productor Kafka reconectado');
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

async function publishOrder(order) {
  try {
    await producer.send({
      topic: TOPIC,
      messages: [{ value: JSON.stringify(order) }],
    });
  } catch (err) {
    console.warn('[order_api] Fallo al publicar, reintentando conexión...', err.message);
    producer = null;
    await ensureProducer();
    await producer.send({
      topic: TOPIC,
      messages: [{ value: JSON.stringify(order) }],
    });
  }
}

async function main() {
  console.log(`[order_api] Conectando a Kafka en ${KAFKA_BROKER}...`);
  await waitForProducer();

  const app = express();
  app.use(express.json());

  app.post('/order', async (req, res) => {
    const validationError = validateOrderBody(req.body);
    if (validationError) {
      return res.status(400).json({ error: validationError });
    }

    const order = {
      order_id: nextOrderId++,
      item: req.body.item.trim(),
    };

    try {
      await publishOrder(order);
      console.log('[order_api] Orden publicada:', order);
      return res.status(201).json(order);
    } catch (err) {
      console.error('[order_api] Error al publicar en Kafka:', err.message);
      return res.status(503).json({ error: 'No se pudo publicar la orden en Kafka' });
    }
  });

  app.listen(PORT, () => {
    console.log(`[order_api] Escuchando en http://0.0.0.0:${PORT}`);
  });
}

main().catch((err) => {
  console.error('[order_api] Fallo al iniciar:', err);
  process.exit(1);
});
