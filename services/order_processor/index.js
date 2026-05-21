// ============================================================
// SERVICIO: order_processor
// ROL: Es el "puente" entre Kafka y RabbitMQ.
//      Lee órdenes del canal Kafka 'new_orders' y las reenvía
//      al exchange fanout de RabbitMQ 'order_tasks', para que
//      los 3 workers (billing, inventory, notification) las
//      reciban simultáneamente y las procesen en paralelo.
// ============================================================

const amqp = require('amqplib');              // amqplib es la librería para hablar con RabbitMQ desde Node.js. AMQP es el protocolo que usa RabbitMQ
const { Kafka, logLevel } = require('kafkajs'); // KafkaJS para conectarse a Apache Kafka

// --- CONFIGURACIÓN ---
const KAFKA_BROKER  = process.env.KAFKA_BROKER  || 'kafka:9092';                     // Dirección del servidor Kafka (viene de Docker)
const RABBITMQ_URL  = process.env.RABBITMQ_URL  || 'amqp://guest:guest@rabbitmq:5672'; // URL de conexión a RabbitMQ: usuario:contraseña@host:puerto
const KAFKA_TOPIC   = 'new_orders';             // Nombre del topic de Kafka del que leeremos: donde order_api publica los pedidos
const KAFKA_GROUP   = 'order_processor_group';  // ID del grupo de consumidores. Kafka usa esto para recordar qué mensajes ya fueron procesados
const EXCHANGE      = 'order_tasks';            // Nombre del "exchange" de RabbitMQ. Un exchange es el distribuidor de mensajes
// Las tres colas a las que el exchange fanout distribuirá el mensaje automáticamente
const QUEUES        = ['billing_queue', 'inventory_queue', 'notification_queue']; // Array con los nombres de las 3 colas

// --- CONFIGURACIÓN DE KAFKA ---
const kafka = new Kafka({
  clientId: 'order_processor',                          // Nombre identificador de este servicio en Kafka
  brokers:  [KAFKA_BROKER],                             // Servidor(es) Kafka disponibles
  logLevel: logLevel.WARN,                              // Solo mostrar advertencias y errores en consola
  retry: { initialRetryTime: 1000, retries: Number.MAX_SAFE_INTEGER }, // Reintentos casi infinitos si Kafka no está disponible
});

// Variables globales para la conexión con RabbitMQ
let channel    = null; // 'channel' es el canal de comunicación dentro de una conexión RabbitMQ (como un sub-canal)
let connection = null; // 'connection' es la conexión TCP principal con RabbitMQ

// Función de utilidad para pausar la ejecución un tiempo determinado
const sleep = (ms) => new Promise((r) => setTimeout(r, ms)); // Pausa 'ms' milisegundos antes de continuar

// --- FUNCIONES DE CONEXIÓN ---

// waitForKafka: espera activamente hasta que Kafka esté disponible y respondiendo
// Necesario porque al iniciar con Docker, Kafka puede tardar 20-30 segundos en estar listo
async function waitForKafka() {
  const admin = kafka.admin(); // 'admin' es un cliente especial de Kafka para operaciones administrativas (como listar topics)
  while (true) {               // Bucle infinito: sigue intentando hasta que funcione
    try {
      await admin.connect();       // Intenta conectarse a Kafka
      await admin.listTopics();    // Intenta listar los topics (si esto funciona, Kafka está listo)
      await admin.disconnect();    // Cierra la conexión administrativa (ya no la necesitamos)
      return;                      // Sale del bucle: Kafka está listo
    } catch (err) {
      console.warn(`[order_processor] Kafka no listo: ${err.message}. Reintentando...`); // Muestra el error y espera
      await sleep(2000); // Espera 2 segundos antes del próximo intento
    }
  }
}

// connectRabbitMQ: conecta a RabbitMQ, declara el exchange fanout y crea las 3 colas
// Un exchange FANOUT es como un megáfono: todo lo que llega, lo manda a TODAS las colas vinculadas
async function connectRabbitMQ(maxAttempts = 30, delayMs = 2000) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) { // Intenta hasta 30 veces con 2s de espera entre cada intento
    try {
      connection = await amqp.connect(RABBITMQ_URL);     // Abre la conexión TCP con RabbitMQ
      channel    = await connection.createChannel();     // Crea un canal de comunicación dentro de esa conexión

      // assertExchange: crea el exchange si no existe, o verifica que ya existe con la misma configuración
      // 'fanout' = tipo de exchange que copia el mensaje a TODAS las colas vinculadas (ignora routing keys)
      // { durable: true } = el exchange sobrevive si RabbitMQ se reinicia
      await channel.assertExchange(EXCHANGE, 'fanout', { durable: true });

      // Para cada cola en el array QUEUES, la crea y la vincula al exchange
      for (const queue of QUEUES) {
        await channel.assertQueue(queue, { durable: true }); // Crea la cola si no existe. durable:true = sobrevive reinicios
        await channel.bindQueue(queue, EXCHANGE, '');        // Vincula la cola al exchange. '' = routing key vacía (fanout la ignora)
      }

      // Si la conexión con RabbitMQ se cierra inesperadamente, programa una reconexión
      connection.on('close', () => {
        console.warn('[order_processor] RabbitMQ desconectado; reconectando...'); // Avisa de la desconexión
        channel    = null;    // Marca el canal como no disponible
        connection = null;    // Marca la conexión como no disponible
        scheduleRabbitReconnect(); // Programa un intento de reconexión
      });
      connection.on('error', (err) =>
        console.error('[order_processor] Error RabbitMQ:', err.message)); // Muestra cualquier error de la conexión

      console.log(`[order_processor] RabbitMQ listo — exchange "${EXCHANGE}" (fanout)`); // Confirma que todo está listo
      return; // Sale del bucle de reintentos: la conexión fue exitosa
    } catch (err) {
      console.warn(`[order_processor] RabbitMQ no disponible (${attempt}/${maxAttempts}):`, err.message); // Muestra el intento fallido
      if (attempt === maxAttempts) throw err; // Si se acabaron los intentos, lanza el error para detener el proceso
      await sleep(delayMs); // Espera antes del próximo intento
    }
  }
}

// scheduleRabbitReconnect: programa un intento de reconexión a RabbitMQ después de 3 segundos
// Se llama cuando la conexión se cae inesperadamente durante la operación
function scheduleRabbitReconnect() {
  setTimeout(async () => {           // setTimeout ejecuta la función después de 3000ms (3 segundos)
    try { await connectRabbitMQ(); } // Intenta reconectar
    catch (err) {
      console.error('[order_processor] Reconexión fallida:', err.message); // Si falla la reconexión, muestra el error
      scheduleRabbitReconnect();                                            // Y programa otro intento (recursivo)
    }
  }, 3000); // 3000ms = 3 segundos de espera antes de intentar reconectar
}

// --- PUBLICACIÓN EN RABBITMQ ---

// publishToRabbit: envía la orden al exchange fanout de RabbitMQ
// RabbitMQ se encarga de copiarla automáticamente a las 3 colas
function publishToRabbit(order) {
  if (!channel) throw new Error('Canal RabbitMQ no disponible'); // Si no hay canal activo, lanza un error
  channel.publish(
    EXCHANGE,                           // Destino: el exchange 'order_tasks'
    '',                                 // Routing key: vacía (los exchanges fanout la ignoran)
    Buffer.from(JSON.stringify(order)), // Contenido: la orden convertida a JSON y luego a bytes (Buffer)
    {
      contentType: 'application/json', // Metadato: indica que el contenido es JSON
      persistent: true,                // persistent:true = el mensaje se guarda en disco y sobrevive si RabbitMQ se reinicia
    }
  );
}

// --- CONSUMIDOR DE KAFKA ---

// startKafkaConsumer: se suscribe al topic 'new_orders' de Kafka y procesa cada mensaje
async function startKafkaConsumer() {
  await waitForKafka(); // Primero espera a que Kafka esté disponible

  const consumer = kafka.consumer({ groupId: KAFKA_GROUP }); // Crea el consumidor con su grupo ID
  await consumer.connect();                                    // Conecta el consumidor a Kafka
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false }); // Se suscribe al topic. fromBeginning:false = solo procesa mensajes nuevos (no los históricos)

  console.log(`[order_processor] Consumiendo "${KAFKA_TOPIC}" (group: ${KAFKA_GROUP})`); // Confirma que está escuchando

  // consumer.run: bucle que procesa cada mensaje que llega al topic
  await consumer.run({
    eachMessage: async ({ message }) => {         // Esta función se ejecuta una vez por cada mensaje nuevo
      const raw = message.value?.toString();      // Convierte el contenido del mensaje de bytes a string. '?.' evita error si value es null
      if (!raw) return;                           // Si el mensaje está vacío, lo ignora y pasa al siguiente

      let order; // Variable donde guardaremos la orden parseada
      try {
        order = JSON.parse(raw);                                      // Convierte el string JSON a un objeto JavaScript
      } catch {
        console.error('[order_processor] JSON inválido, omitiendo'); // Si el JSON es inválido (mensaje corrupto), avisa y omite
        return; // Sale del handler de este mensaje sin procesarlo
      }

      try {
        publishToRabbit(order);                                                            // Envía la orden al exchange fanout de RabbitMQ
        console.log(`[order_processor] Orden #${order.order_id} enviada al exchange "${EXCHANGE}"`); // Confirma el envío
      } catch (err) {
        console.error(`[order_processor] Error publicando orden #${order.order_id}:`, err.message); // Si falla, muestra el error
      }
    },
  });
}

// --- FUNCIÓN PRINCIPAL ---
async function main() {
  console.log(`[order_processor] Conectando a RabbitMQ (${RABBITMQ_URL})...`); // Avisa que está iniciando
  await connectRabbitMQ();  // Primero conecta a RabbitMQ y prepara el exchange y las colas

  console.log(`[order_processor] Conectando a Kafka (${KAFKA_BROKER})...`); // Avisa que va a conectar con Kafka
  await startKafkaConsumer(); // Luego arranca el consumidor de Kafka (este no termina, queda escuchando indefinidamente)
}

// Inicia todo y maneja errores fatales de arranque
main().catch((err) => {
  console.error('[order_processor] Fallo al iniciar:', err); // Muestra el error que impidió el arranque
  process.exit(1); // Termina el proceso con error para que Docker lo reinicie
});
