// ============================================================
// SERVICIO: analytics_service
// ROL: Observador independiente del sistema.
//      Lee del mismo topic que order_processor ('new_orders')
//      pero con un Group ID diferente, por eso Kafka le entrega
//      su propia copia de cada mensaje. Cuenta cuántas órdenes
//      llegan por minuto y lo imprime en consola.
//      Demuestra el principio clave de Kafka: múltiples
//      consumidores independientes sobre los mismos datos.
// ============================================================

const { Kafka, logLevel } = require('kafkajs'); // Importa la librería KafkaJS para comunicarse con Apache Kafka

// --- CONFIGURACIÓN ---
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092'; // Dirección del servidor Kafka (inyectada por Docker)
const TOPIC        = 'new_orders';                              // Topic del que este servicio lee: el mismo que usa order_processor
const GROUP_ID     = 'analytics_group';                         // Grupo de consumidores DISTINTO al de order_processor. Por esto Kafka le da su propia copia de los mensajes
const WINDOW_MS    = 60_000;                                    // 60_000 milisegundos = 60 segundos = 1 minuto. Este es el tamaño de la ventana de tiempo para contar órdenes

// --- ESTADO DEL SERVICIO ---
let ordersInWindow = 0; // Contador de órdenes recibidas en la ventana de tiempo actual. Se resetea cada minuto

// --- CONFIGURACIÓN DE KAFKA ---
const kafka = new Kafka({
  clientId: 'analytics_service',                                     // Identificador de este servicio en Kafka
  brokers:  [KAFKA_BROKER],                                          // Lista de servidores Kafka
  logLevel: logLevel.WARN,                                           // Solo mostrar advertencias y errores
  retry: {
    initialRetryTime: 1000,            // Espera 1 segundo antes del primer reintento de conexión
    retries: Number.MAX_SAFE_INTEGER,  // Reintentos prácticamente infinitos
  },
});

// --- FUNCIONES DE UTILIDAD ---

// sleep: pausa la ejecución 'ms' milisegundos. Usada para reintentos de conexión
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms)); // Crea una promesa que se resuelve después de 'ms' ms
}

// printWindowSummary: imprime el resumen de la ventana de tiempo y reinicia el contador
// Esta función se llama automáticamente cada 60 segundos
function printWindowSummary() {
  console.log(
    `[analytics_service] Total ventas último minuto: ${ordersInWindow}` // Imprime cuántas órdenes llegaron en el último minuto
  );
  ordersInWindow = 0; // Reinicia el contador a 0 para empezar a contar el siguiente minuto
}

// waitForKafka: bucle de espera hasta que Kafka esté listo y respondiendo
async function waitForKafka() {
  const admin = kafka.admin(); // Crea un cliente administrador de Kafka para verificar la conectividad
  while (true) {               // Repite indefinidamente hasta que tenga éxito
    try {
      await admin.connect();       // Intenta conectarse a Kafka
      await admin.listTopics();    // Verifica que Kafka responde (lista los topics disponibles)
      await admin.disconnect();    // Cierra la conexión de administración
      return;                      // Si llegó aquí sin error, Kafka está listo: sale del bucle
    } catch (err) {
      console.warn(`[analytics_service] Kafka no listo: ${err.message}. Reintentando...`); // Kafka aún no está listo
      await sleep(2000); // Espera 2 segundos antes de intentar de nuevo
    }
  }
}

// --- CONSUMIDOR PRINCIPAL ---

// startConsumer: conecta a Kafka, se suscribe al topic y procesa cada orden recibida
async function startConsumer() {
  await waitForKafka(); // Espera a que Kafka esté disponible antes de continuar

  const consumer = kafka.consumer({ groupId: GROUP_ID }); // Crea el consumidor con el group ID 'analytics_group'
  await consumer.connect();                                 // Establece la conexión con Kafka
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false }); // Se suscribe a 'new_orders'. fromBeginning:false = ignora mensajes anteriores al arranque

  console.log(
    `[analytics_service] Consumiendo ${TOPIC} (group: ${GROUP_ID}), resumen cada 60s` // Avisa que está activo y escuchando
  );

  // consumer.run: bucle interno que procesa cada mensaje nuevo que llegue al topic
  await consumer.run({
    eachMessage: async ({ message }) => {         // Se ejecuta una vez por cada mensaje nuevo en el topic
      const raw = message.value?.toString();      // Lee el contenido del mensaje como texto (los mensajes Kafka viajan como bytes)
      if (!raw) return;                           // Si el mensaje está vacío, lo salta

      let order; // Variable para guardar la orden una vez parseada
      try {
        order = JSON.parse(raw);                                         // Convierte el JSON (texto) a objeto JavaScript
      } catch {
        console.error('[analytics_service] JSON inválido, omitiendo'); // Si el JSON es inválido, avisa y salta este mensaje
        return; // Sale del handler sin procesar el mensaje corrupto
      }

      ordersInWindow += 1; // Incrementa el contador de órdenes de la ventana actual en 1
      console.log(
        `[analytics_service] Orden recibida #${order.order_id} — "${order.item}"` // Imprime qué orden llegó (para depuración)
      );
    },
  });
}

// --- FUNCIÓN PRINCIPAL ---
async function main() {
  console.log(`[analytics_service] Conectando a Kafka en ${KAFKA_BROKER}...`); // Mensaje de inicio
  setInterval(printWindowSummary, WINDOW_MS); // Programa que printWindowSummary se ejecute cada 60 segundos de forma automática
  await startConsumer();                       // Arranca el consumidor de Kafka (queda corriendo indefinidamente)
}

// Inicia el servicio y captura cualquier error fatal de arranque
main().catch((err) => {
  console.error('[analytics_service] Fallo al iniciar:', err); // Muestra el error
  process.exit(1); // Termina el proceso con código de error para que Docker lo reinicie
});
