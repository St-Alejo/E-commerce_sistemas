// Importa amqplib para la comunicación con RabbitMQ
const amqp = require('amqplib');
// Importa kafkajs para la comunicación con Kafka
const { Kafka, logLevel } = require('kafkajs');

// Define la dirección del broker de Kafka
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
// Define la URL de conexión para RabbitMQ (con credenciales por defecto)
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
// Define el tópico de Kafka desde donde se leerán las órdenes
const KAFKA_TOPIC = 'new_orders';
// Define el ID del grupo de consumidores para este servicio en Kafka
const KAFKA_GROUP = 'order_processor_group';
// Define el nombre del exchange en RabbitMQ para distribuir tareas
const EXCHANGE = 'order_tasks';
// Define las colas de RabbitMQ que recibirán los mensajes del exchange
const QUEUES = ['billing_queue', 'inventory_queue', 'notification_queue'];

// Configura la instancia de Kafka
const kafka = new Kafka({
  clientId: 'order_processor',
  brokers: [KAFKA_BROKER],
  logLevel: logLevel.WARN, // Solo muestra advertencias y errores
  retry: {
    initialRetryTime: 1000, // Tiempo inicial de reintento
    retries: Number.MAX_SAFE_INTEGER, // Reintenta indefinidamente
  },
});

// Variables para mantener la conexión y el canal de RabbitMQ
let channel = null;
let connection = null;

// Función auxiliar para pausar la ejecución (espera asíncrona)
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Función que espera hasta que el servicio de Kafka esté listo
async function waitForKafka() {
  const admin = kafka.admin(); // Crea una instancia administrativa de Kafka
  while (true) {
    try {
      await admin.connect(); // Intenta conectar
      await admin.listTopics(); // Intenta listar tópicos para validar conexión
      await admin.disconnect(); // Desconecta tras la prueba exitosa
      return; // Sale si todo está bien
    } catch (err) {
      // Si falla, avisa y espera 2 segundos antes de reintentar
      console.warn(`[order_processor] Kafka no listo: ${err.message}. Reintentando...`);
      await sleep(2000);
    }
  }
}

// Función para conectar a RabbitMQ con un número limitado de reintentos
async function connectRabbitMQ(maxAttempts = 30, delayMs = 2000) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      // Intenta establecer la conexión con el servidor RabbitMQ
      connection = await amqp.connect(RABBITMQ_URL);
      // Crea un canal de comunicación dentro de la conexión
      channel = await connection.createChannel();

      // Asegura que el exchange de tipo 'fanout' exista y sea persistente (durable)
      await channel.assertExchange(EXCHANGE, 'fanout', { durable: true });

      // Crea y vincula cada una de las colas definidas al exchange
      for (const queue of QUEUES) {
        await channel.assertQueue(queue, { durable: true }); // Crea la cola si no existe
        await channel.bindQueue(queue, EXCHANGE, ''); // Vincula la cola al exchange fanout
      }

      // Manejador para el cierre inesperado de la conexión
      connection.on('close', () => {
        console.warn('[order_processor] Conexión RabbitMQ cerrada; reconectando...');
        channel = null;
        connection = null;
        scheduleRabbitReconnect(); // Programa un intento de reconexión
      });

      // Manejador para errores en la conexión de RabbitMQ
      connection.on('error', (err) => {
        console.error('[order_processor] Error RabbitMQ:', err.message);
      });

      console.log(`[order_processor] RabbitMQ listo (exchange: ${EXCHANGE}, fanout)`);
      return; // Conexión exitosa, sale de la función
    } catch (err) {
      // Informa del fallo en el intento de conexión actual
      console.warn(
        `[order_processor] RabbitMQ no disponible (intento ${attempt}/${maxAttempts}):`,
        err.message
      );
      if (attempt === maxAttempts) throw err; // Si agota intentos, lanza error
      await sleep(delayMs); // Espera antes del siguiente intento
    }
  }
}

// Función para programar una reconexión a RabbitMQ tras un fallo
function scheduleRabbitReconnect() {
  setTimeout(async () => {
    try {
      await connectRabbitMQ();
    } catch (err) {
      console.error('[order_processor] Reconexión RabbitMQ fallida:', err.message);
      scheduleRabbitReconnect(); // Reintenta programar la reconexión
    }
  }, 3000); // Espera 3 segundos antes de intentar reconectar
}

// Función para publicar un mensaje (orden) en el exchange de RabbitMQ
function publishToRabbit(order) {
  if (!channel) {
    throw new Error('Canal RabbitMQ no disponible');
  }
  // Convierte el objeto de la orden a un buffer de bytes para el envío
  const payload = Buffer.from(JSON.stringify(order));
  // Publica el mensaje en el exchange con opciones de persistencia
  channel.publish(EXCHANGE, '', payload, {
    contentType: 'application/json', // Indica que el contenido es JSON
    persistent: true, // El mensaje se guarda en disco para no perderse si RabbitMQ reinicia
  });
}

// Función para iniciar el consumo de mensajes desde Kafka
async function startKafkaConsumer() {
  await waitForKafka(); // Asegura que Kafka esté listo antes de empezar

  const consumer = kafka.consumer({ groupId: KAFKA_GROUP }); // Crea el consumidor
  await consumer.connect(); // Conecta el consumidor
  // Se suscribe al tópico de órdenes desde el último mensaje recibido
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });

  console.log(`[order_processor] Consumiendo ${KAFKA_TOPIC} (group: ${KAFKA_GROUP})`);

  // Inicia el bucle de procesamiento de mensajes de Kafka
  await consumer.run({
    eachMessage: async ({ message }) => {
      // Obtiene el valor del mensaje como string
      const raw = message.value?.toString();
      if (!raw) return;

      let order;
      try {
        order = JSON.parse(raw); // Parsea el JSON del mensaje
      } catch {
        console.error('[order_processor] JSON inválido en Kafka, omitiendo');
        return;
      }

      try {
        // Al recibir una orden de Kafka, la publica en RabbitMQ para ser procesada
        publishToRabbit(order);
        console.log(
          `[order_processor] Orden ${order.order_id} publicada en exchange "${EXCHANGE}"`
        );
      } catch (err) {
        // Maneja errores de publicación en RabbitMQ
        console.error(
          `[order_processor] Error publicando orden ${order.order_id}:`,
          err.message
        );
      }
    },
  });
}

// Función principal de entrada del servicio
async function main() {
  console.log(`[order_processor] Conectando a RabbitMQ (${RABBITMQ_URL})...`);
  await connectRabbitMQ(); // Inicializa conexión con RabbitMQ

  console.log(`[order_processor] Conectando a Kafka (${KAFKA_BROKER})...`);
  await startKafkaConsumer(); // Inicializa consumidor de Kafka
}

// Ejecuta el servicio y captura errores fatales
main().catch((err) => {
  console.error('[order_processor] Fallo al iniciar:', err);
  process.exit(1); // Sale con error si el inicio falla
});
