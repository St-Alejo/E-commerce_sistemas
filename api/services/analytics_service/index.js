// Importa las dependencias necesarias de la librería kafkajs
const { Kafka, logLevel } = require('kafkajs');

// Define la dirección del broker de Kafka, usando una variable de entorno o un valor por defecto
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
// Define el nombre del tópico de Kafka al que se suscribirá el servicio
const TOPIC = 'new_orders';
// Define el ID del grupo de consumidores para Kafka
const GROUP_ID = 'analytics_group';
// Define el intervalo de tiempo (en milisegundos) para mostrar el resumen de ventas (60 segundos)
const WINDOW_MS = 60_000;

// Inicializa el contador de órdenes recibidas en el intervalo de tiempo actual
let ordersInWindow = 0;

// Configura la instancia de Kafka con el ID del cliente, brokers y nivel de log
const kafka = new Kafka({
  clientId: 'analytics_service',
  brokers: [KAFKA_BROKER],
  logLevel: logLevel.WARN, // Solo muestra advertencias y errores
  retry: {
    initialRetryTime: 1000, // Tiempo inicial de reintento en milisegundos
    retries: Number.MAX_SAFE_INTEGER, // Reintenta indefinidamente
  },
});

// Función auxiliar para pausar la ejecución durante un tiempo determinado
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Función que imprime el resumen de ventas del último minuto y reinicia el contador
function printWindowSummary() {
  console.log(
    `[analytics_service] Total ventas último minuto: ${ordersInWindow}`
  );
  ordersInWindow = 0; // Reinicia el contador para el siguiente minuto
}

// Función asíncrona que espera a que Kafka esté disponible antes de continuar
async function waitForKafka() {
  const admin = kafka.admin(); // Crea una instancia administrativa de Kafka
  while (true) {
    try {
      await admin.connect(); // Intenta conectar con el broker
      await admin.listTopics(); // Intenta listar los tópicos para verificar la conexión
      await admin.disconnect(); // Se desconecta si la prueba fue exitosa
      return; // Sale de la función si la conexión fue exitosa
    } catch (err) {
      // Informa si Kafka no está listo y espera antes de reintentar
      console.warn(`[analytics_service] Kafka no listo: ${err.message}. Reintentando...`);
      await sleep(2000); // Espera 2 segundos antes del próximo intento
    }
  }
}

// Función principal para iniciar el consumidor de mensajes de Kafka
async function startConsumer() {
  await waitForKafka(); // Espera a que Kafka esté listo

  const consumer = kafka.consumer({ groupId: GROUP_ID }); // Crea el consumidor con el ID de grupo definido
  await consumer.connect(); // Conecta el consumidor al broker de Kafka
  // Se suscribe al tópico especificado, sin leer mensajes antiguos (fromBeginning: false)
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

  console.log(
    `[analytics_service] Consumiendo ${TOPIC} (group: ${GROUP_ID}), resumen cada 60s`
  );

  // Inicia la ejecución del consumidor para procesar cada mensaje recibido
  await consumer.run({
    eachMessage: async ({ message }) => {
      // Convierte el valor del mensaje a una cadena de texto
      const raw = message.value?.toString();
      if (!raw) return; // Si el mensaje está vacío, lo ignora

      let order;
      try {
        order = JSON.parse(raw); // Intenta parsear el contenido del mensaje como JSON
      } catch {
        // Informa si el JSON no es válido y omite el procesamiento
        console.error('[analytics_service] JSON inválido, omitiendo');
        return;
      }

      ordersInWindow += 1; // Incrementa el contador de órdenes en la ventana actual
      console.log(
        `[analytics_service] Orden recibida #${order.order_id} — "${order.item}"`
      );
    },
  });
}

// Función de entrada principal del servicio
async function main() {
  console.log(`[analytics_service] Conectando a Kafka en ${KAFKA_BROKER}...`);
  // Establece un intervalo para imprimir el resumen de ventas periódicamente
  setInterval(printWindowSummary, WINDOW_MS);
  await startConsumer(); // Inicia el proceso de consumo de mensajes
}

// Ejecuta la función principal y maneja posibles errores fatales
main().catch((err) => {
  console.error('[analytics_service] Fallo al iniciar:', err);
  process.exit(1); // Sale del proceso con un código de error
});
