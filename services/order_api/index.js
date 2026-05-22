const express = require('express');
const path = require('path');
const { Kafka, logLevel } = require('kafkajs');

// [DOCKER: CONFIGURACIÓN]
const PORT         = Number(process.env.PORT)         || 3000;         // puerto contenedor
const KAFKA_BROKER = process.env.KAFKA_BROKER         || 'kafka:9092'; // host kafka
const TOPIC        = 'new_orders';                                      // canal pedidos

let nextOrderId = 1;
let producer   = null;

// [KAFKA: CLIENTE]
const kafka = new Kafka({
  clientId: 'order_api', // id cliente
  brokers:  [KAFKA_BROKER],
  logLevel: logLevel.WARN,
  retry: {
    initialRetryTime: 1000,
    retries: Number.MAX_SAFE_INTEGER, // reintentos infinitos
  },
});

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// [KAFKA: PRODUCTOR]
async function connectProducer() {
  const p = kafka.producer(); // crear productor
  await p.connect();          // conectar
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
      console.warn(`[order_api] Kafka no disponible. Reintentando...`);
      await sleep(2000); // espera activa
    }
  }
}

async function ensureProducer() {
  if (!producer) {
    producer = await connectProducer(); // reconexión
  }
}

function validateOrderBody(body) {
  if (!body || typeof body !== 'object') {
    return 'JSON inválido';
  }
  if (typeof body.item !== 'string' || body.item.trim() === '') {
    return 'Item obligatorio';
  }
  return null;
}

// [KAFKA: PUBLICACIÓN]
async function publishOrder(order) {
  try {
    await producer.send({
      topic:    TOPIC, // enviar a kafka
      messages: [{ value: JSON.stringify(order) }], // mensaje json
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
  await waitForProducer(); // esperar kafka

  const app = express();
  app.use(express.json()); // middleware json

  // [HTTP: RUTAS]
  app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'store.html')); // servir interfaz
  });

  app.post('/order', async (req, res) => {
    const validationError = validateOrderBody(req.body); // validar entrada
    if (validationError) {
      return res.status(400).json({ error: validationError });
    }

    const order = {
      order_id: nextOrderId++,      // id auto-incremental
      item:     req.body.item.trim(),
    };

    try {
      await publishOrder(order); // publicar evento
      console.log('[order_api] Orden publicada:', order);
      return res.status(201).json(order); // respuesta exitosa
    } catch (err) {
      return res.status(503).json({ error: 'Kafka error' });
    }
  });

  app.listen(PORT, () => {
    console.log(`[order_api] Escuchando en http://0.0.0.0:${PORT}`);
  });
}

// [DOCKER: INICIO]
main().catch((err) => {
  console.error('[order_api] Fallo:', err);
  process.exit(1); // reinicio docker
});
