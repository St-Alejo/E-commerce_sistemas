// Importa el framework Express para crear la API HTTP
const express = require('express');
// Importa las dependencias de Kafka de la librería kafkajs
const { Kafka, logLevel } = require('kafkajs');

// Define el puerto en el que escuchará el servidor, desde variables de entorno o por defecto 3000
const PORT = Number(process.env.PORT) || 3000;
// Define la dirección del broker de Kafka
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
// Define el nombre del tópico de Kafka donde se publicarán las órdenes
const TOPIC = 'new_orders';

// Inicializa el contador para generar IDs de órdenes secuenciales
let nextOrderId = 1;
// Variable para almacenar la instancia del productor de Kafka
let producer = null;

// Configura la instancia de Kafka para este servicio
const kafka = new Kafka({
  clientId: 'order_api',
  brokers: [KAFKA_BROKER],
  logLevel: logLevel.WARN, // Nivel de logs: solo advertencias y errores
  retry: {
    initialRetryTime: 1000, // Tiempo inicial antes de reintentar una conexión fallida
    retries: Number.MAX_SAFE_INTEGER, // Reintenta de forma indefinida
  },
});

// Función auxiliar para pausar la ejecución un tiempo determinado (promesa)
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Función asíncrona que crea y conecta un productor de Kafka
async function connectProducer() {
  const p = kafka.producer(); // Crea el productor
  await p.connect(); // Intenta conectar al broker
  return p; // Retorna el productor conectado
}

// Función que espera hasta que el productor de Kafka se conecte exitosamente
async function waitForProducer() {
  while (true) {
    try {
      producer = await connectProducer(); // Intenta la conexión
      console.log(`[order_api] Productor conectado (topic: ${TOPIC})`);
      return; // Sale del bucle si tiene éxito
    } catch (err) {
      // Si falla, muestra advertencia y espera 2 segundos antes de reintentar
      console.warn(`[order_api] Kafka no disponible: ${err.message}. Reintentando...`);
      await sleep(2000);
    }
  }
}

// Asegura que el productor esté conectado, intentando reconectar si es null
async function ensureProducer() {
  if (!producer) {
    producer = await connectProducer();
    console.log('[order_api] Productor Kafka reconectado');
  }
}

// Función para validar que el cuerpo de la petición HTTP sea correcto
function validateOrderBody(body) {
  // Verifica que el cuerpo sea un objeto
  if (!body || typeof body !== 'object') {
    return 'El cuerpo debe ser un objeto JSON';
  }
  // Verifica que el campo "item" exista, sea string y no esté vacío
  if (typeof body.item !== 'string' || body.item.trim() === '') {
    return 'El campo "item" es obligatorio y debe ser un string no vacío';
  }
  return null; // Retorna null si no hay errores de validación
}

// Función para publicar una orden en el tópico de Kafka
async function publishOrder(order) {
  try {
    // Intenta enviar el mensaje al tópico definido
    await producer.send({
      topic: TOPIC,
      messages: [{ value: JSON.stringify(order) }],
    });
  } catch (err) {
    // Si falla el envío, intenta reconectar el productor y reintentar una vez más
    console.warn('[order_api] Fallo al publicar, reintentando conexión...', err.message);
    producer = null;
    await ensureProducer();
    await producer.send({
      topic: TOPIC,
      messages: [{ value: JSON.stringify(order) }],
    });
  }
}

// Función principal que arranca el servidor y conecta Kafka
async function main() {
  console.log(`[order_api] Conectando a Kafka en ${KAFKA_BROKER}...`);
  await waitForProducer(); // Espera la conexión inicial con Kafka

  const app = express(); // Crea la aplicación Express
  app.use(express.json()); // Middleware para parsear cuerpos JSON en las peticiones

  // Define la ruta POST /order para recibir nuevas órdenes
  app.post('/order', async (req, res) => {
    // Valida el cuerpo de la petición recibida
    const validationError = validateOrderBody(req.body);
    if (validationError) {
      // Si hay error de validación, responde con código 400 (Bad Request)
      return res.status(400).json({ error: validationError });
    }

    // Crea el objeto de la orden con un ID incremental y el ítem limpio
    const order = {
      order_id: nextOrderId++,
      item: req.body.item.trim(),
    };

    try {
      // Intenta publicar la orden en Kafka
      await publishOrder(order);
      console.log('[order_api] Orden publicada:', order);
      // Responde con código 201 (Created) y los datos de la orden
      return res.status(201).json(order);
    } catch (err) {
      // Maneja errores de publicación respondiendo con código 503 (Service Unavailable)
      console.error('[order_api] Error al publicar en Kafka:', err.message);
      return res.status(503).json({ error: 'No se pudo publicar la orden en Kafka' });
    }
  });

  // Inicia el servidor Express en el puerto y host configurados
  app.listen(PORT, () => {
    console.log(`[order_api] Escuchando en http://0.0.0.0:${PORT}`);
  });
}

// Ejecuta la función main y captura errores críticos de inicio
main().catch((err) => {
  console.error('[order_api] Fallo al iniciar:', err);
  process.exit(1); // Finaliza el proceso si ocurre un error fatal
});
