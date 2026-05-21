// ============================================================
// SERVICIO: notification_worker
// ROL: Trabajador de notificaciones.
//      Recibe órdenes de RabbitMQ (de notification_queue),
//      simula el envío de un email de confirmación (100% éxito),
//      y publica el resultado en Kafka (topic notification_events)
//      para que el dashboard_aggregator actualice el estado.
// ============================================================

const amqp        = require('amqplib');    // Librería para conectarse a RabbitMQ usando el protocolo AMQP
const { Kafka }   = require('kafkajs');    // Librería para conectarse a Apache Kafka

// --- CONFIGURACIÓN ---
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672'; // URL completa de conexión a RabbitMQ
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';                        // Servidor de Kafka al que enviaremos resultados
const EXCHANGE     = 'order_tasks';           // Exchange fanout compartido entre los 3 workers (siempre el mismo)
const QUEUE        = 'notification_queue';    // Cola exclusiva de este worker para recibir las órdenes
const TOPIC        = 'notification_events';   // Topic de Kafka donde publicamos que el email fue enviado

// --- CONFIGURACIÓN DE KAFKA (PRODUCTOR) ---
const kafka    = new Kafka({ clientId: 'notification_worker', brokers: [KAFKA_BROKER] }); // Crea el cliente Kafka con nombre identificativo
const producer = kafka.producer(); // Crea el objeto productor que enviará mensajes a Kafka

// --- SIMULACIÓN DE NOTIFICACIÓN ---

// simulateNotification: simula el envío de un email de confirmación
// Siempre tiene éxito (100%) — los emails de confirmación se consideran confiables en esta arquitectura
function simulateNotification() {
  return new Promise((resolve) => {             // Devuelve una promesa (resultado asíncrono)
    setTimeout(                                 // Simula el tiempo de envío del email (latencia de servidor de correo)
      () => resolve('EMAIL_SENT'),             // Siempre resuelve con 'EMAIL_SENT': el email siempre se envía
      50 + Math.random() * 250                 // Demora aleatoria entre 50ms y 300ms (emails son rápidos)
    );
  });
}

// --- CONEXIÓN A RABBITMQ ---

// connectRabbitMQ: bucle de reconexión con hasta 20 intentos
async function connectRabbitMQ() {
  let retries = 20; // Máximo 20 intentos de conexión
  while (retries--) { // Decrementa el contador en cada vuelta del bucle
    try {
      const connection = await amqp.connect(RABBITMQ_URL); // Intenta abrir la conexión con RabbitMQ
      console.log('[notification_worker] Conectado a RabbitMQ'); // Éxito: informa la conexión
      return connection;                                         // Devuelve la conexión establecida
    } catch {
      console.log('[notification_worker] Reintentando RabbitMQ...'); // Fallo: informa el reintento
      await new Promise((r) => setTimeout(r, 2000));                  // Espera 2 segundos antes del próximo intento
    }
  }
  throw new Error('[notification_worker] No se pudo conectar a RabbitMQ'); // Agotados los intentos: lanza error
}

// --- FUNCIÓN PRINCIPAL DEL WORKER ---

// start: establece conexiones y arranca el consumidor de mensajes de la cola
async function start() {
  await producer.connect();                     // Conecta el productor de Kafka
  console.log('[notification_worker] Conectado a Kafka'); // Informa la conexión a Kafka

  const connection = await connectRabbitMQ();          // Establece conexión con RabbitMQ
  const channel    = await connection.createChannel();  // Crea el canal de comunicación dentro de la conexión

  // Declara el exchange fanout compartido (todos los workers declaran el mismo exchange, es idempotente)
  await channel.assertExchange(EXCHANGE, 'fanout', { durable: true }); // fanout + durable: distribuye a todas las colas y persiste

  // Declara la cola de notificaciones
  await channel.assertQueue(QUEUE, { durable: true }); // Cola durable: sobrevive reinicios de RabbitMQ

  // Enlaza la cola al exchange para que reciba las copias del fanout
  await channel.bindQueue(QUEUE, EXCHANGE, ''); // Routing key vacía: los exchanges fanout no usan routing keys

  console.log('[notification_worker] Esperando órdenes en notification_queue...'); // Listo y esperando mensajes

  // Registra la función que procesa cada mensaje que llega a notification_queue
  channel.consume(QUEUE, async (msg) => {      // Se invoca automáticamente por cada mensaje nuevo
    if (!msg) return;                          // Si msg es null, ignora (comportamiento estándar de amqplib)
    try {
      const order  = JSON.parse(msg.content.toString()); // Convierte bytes → texto → objeto JS
      console.log('[notification_worker] Orden recibida:', order);       // Muestra la orden que llegó

      const status = await simulateNotification();         // Simula el envío del email y espera el resultado
      const event  = { order_id: order.order_id, status }; // Construye el evento de resultado

      // Publica en Kafka que el email fue enviado para que el dashboard lo muestre
      await producer.send({
        topic:    TOPIC,                                           // Topic destino: 'notification_events'
        messages: [{ key: String(order.order_id), value: JSON.stringify(event) }], // key=ID orden, value=evento
      });

      console.log('[notification_worker] Evento enviado:', event); // Confirma la publicación en Kafka
      channel.ack(msg); // ACK: le indica a RabbitMQ que el mensaje fue procesado → puede eliminarlo de la cola
    } catch (err) {
      console.error('[notification_worker] Error:', err.message);    // Cualquier error inesperado
      channel.nack(msg, false, false); // NACK: procesamiento fallido → descarta el mensaje (no reencola)
    }
  });
}

start(); // Inicia el worker al arrancar el contenedor
