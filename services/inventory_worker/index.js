// ============================================================
// SERVICIO: inventory_worker
// ROL: Trabajador de inventario.
//      Recibe órdenes de RabbitMQ (de inventory_queue),
//      simula la reserva de stock (90% éxito, 10% sin stock),
//      y publica el resultado en Kafka (topic inventory_events)
//      para que el dashboard_aggregator actualice el estado.
// ============================================================

const amqp        = require('amqplib');    // Librería cliente de RabbitMQ para Node.js
const { Kafka }   = require('kafkajs');    // Librería cliente de Apache Kafka para Node.js

// --- CONFIGURACIÓN ---
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672'; // URL de conexión a RabbitMQ
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';                        // Dirección del broker de Kafka
const EXCHANGE     = 'order_tasks';       // Exchange fanout del que recibimos las órdenes (el mismo para los 3 workers)
const QUEUE        = 'inventory_queue';   // Cola exclusiva de este worker (inventory = inventario/stock)
const TOPIC        = 'inventory_events';  // Topic de Kafka donde publicamos el resultado de la reserva de stock

// --- CONFIGURACIÓN DE KAFKA (PRODUCTOR) ---
const kafka    = new Kafka({ clientId: 'inventory_worker', brokers: [KAFKA_BROKER] }); // Cliente Kafka identificado como 'inventory_worker'
const producer = kafka.producer(); // Productor de Kafka para ENVIAR mensajes (los resultados del procesamiento)

// --- SIMULACIÓN DE INVENTARIO ---

// simulateInventory: simula la verificación y reserva de stock con tiempo de procesamiento variable
// El 90% de las veces hay stock disponible, el 10% está agotado
function simulateInventory() {
  return new Promise((resolve) => {         // Devuelve una promesa con el resultado futuro
    setTimeout(() => {                      // Simula el tiempo que tarda consultar una base de datos de inventario
      resolve(Math.random() < 0.90         // Si el número aleatorio es menor a 0.90 (90% probabilidad)...
        ? 'STOCK_RESERVED'                 // ...hay stock y se reserva exitosamente
        : 'STOCK_OUT_OF_STOCK');           // ...no hay stock (10% de las veces)
    }, 100 + Math.random() * 400);         // Demora aleatoria entre 100ms y 500ms
  });
}

// --- CONEXIÓN A RABBITMQ ---

// connectRabbitMQ: intenta conectarse a RabbitMQ reintentando hasta 20 veces
async function connectRabbitMQ() {
  let retries = 20; // Número máximo de intentos de conexión
  while (retries--) { // Itera, decrementando retries cada vuelta
    try {
      const connection = await amqp.connect(RABBITMQ_URL); // Intenta la conexión TCP con RabbitMQ
      console.log('[inventory_worker] Conectado a RabbitMQ'); // Éxito: confirma la conexión
      return connection;                                     // Devuelve la conexión para su uso
    } catch {
      console.log('[inventory_worker] Reintentando RabbitMQ...'); // Fallo: avisa del reintento
      await new Promise((r) => setTimeout(r, 2000));              // Pausa 2 segundos entre intentos
    }
  }
  throw new Error('[inventory_worker] No se pudo conectar a RabbitMQ'); // Sin más intentos: error fatal
}

// --- FUNCIÓN PRINCIPAL DEL WORKER ---

// start: configura las conexiones y el consumidor de mensajes
async function start() {
  await producer.connect();                   // Conecta el productor Kafka para poder publicar resultados
  console.log('[inventory_worker] Conectado a Kafka'); // Confirma la conexión a Kafka

  const connection = await connectRabbitMQ();          // Conecta con RabbitMQ
  const channel    = await connection.createChannel();  // Abre un canal de comunicación sobre la conexión

  // Declara el exchange fanout (idempotente: si ya existe con los mismos parámetros, no hace nada)
  await channel.assertExchange(EXCHANGE, 'fanout', { durable: true }); // Fanout = copia a todas las colas. durable = persiste

  // Declara la cola de inventario
  await channel.assertQueue(QUEUE, { durable: true }); // durable:true = la cola no se pierde si RabbitMQ reinicia

  // Vincula la cola de inventario al exchange fanout
  await channel.bindQueue(QUEUE, EXCHANGE, ''); // '' = routing key vacía (fanout no usa routing keys)

  console.log('[inventory_worker] Esperando órdenes en inventory_queue...'); // Listo para procesar

  // Registra el handler que procesa cada mensaje de la cola
  channel.consume(QUEUE, async (msg) => {      // Se ejecuta cada vez que RabbitMQ entrega un mensaje
    if (!msg) return;                          // Mensaje nulo: ignorar (ocurre cuando se cancela el consumer)
    try {
      const order  = JSON.parse(msg.content.toString()); // Decodifica: bytes → string JSON → objeto JavaScript
      console.log('[inventory_worker] Orden recibida:', order);    // Muestra la orden recibida

      const status = await simulateInventory();            // Simula la reserva de stock y espera el resultado
      const event  = { order_id: order.order_id, status }; // Construye el evento: ID de la orden + resultado del stock

      // Publica el resultado en Kafka para que dashboard_aggregator actualice el estado de la orden
      await producer.send({
        topic:    TOPIC,                                           // Publica en el topic 'inventory_events'
        messages: [{ key: String(order.order_id), value: JSON.stringify(event) }], // key = ID orden, value = evento JSON
      });

      console.log('[inventory_worker] Evento enviado:', event); // Confirma la publicación en Kafka
      channel.ack(msg); // ACK: confirma a RabbitMQ que el mensaje fue procesado exitosamente (lo elimina de la cola)
    } catch (err) {
      console.error('[inventory_worker] Error:', err.message);      // Muestra cualquier error inesperado
      channel.nack(msg, false, false); // NACK: fallo de procesamiento. false,false = no reencolar (descarta el mensaje)
    }
  });
}

start(); // Arranca el worker cuando Docker inicia el contenedor
