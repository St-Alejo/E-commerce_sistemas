// ============================================================
// SERVICIO: billing_worker
// ROL: Trabajador de pagos.
//      Recibe órdenes de RabbitMQ (de billing_queue),
//      simula el cobro (85% éxito, 15% fallo),
//      y publica el resultado en Kafka (topic billing_events)
//      para que el dashboard_aggregator actualice el estado.
// ============================================================

const amqp        = require('amqplib');    // librería para conectarse a RabbitMQ (protocolo AMQP)
const { Kafka }   = require('kafkajs');    // librería para conectarse a Apache Kafka

// --- CONFIGURACIÓN ---
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672'; // URL de RabbitMQ con usuario, contraseña, host y puerto
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';                        // Dirección del servidor Kafka
const EXCHANGE     = 'order_tasks';    // Nombre del exchange fanout de RabbitMQ del que recibimos las órdenes
const QUEUE        = 'billing_queue';  // Cola específica de este worker (billing = facturación/pagos)
const TOPIC        = 'billing_events'; // Topic de Kafka donde publicamos los resultados de los pagos

// --- CONFIGURACIÓN DE KAFKA (PRODUCTOR) ---
const kafka    = new Kafka({ clientId: 'billing_worker', brokers: [KAFKA_BROKER] }); // Crea el cliente Kafka con el nombre de este servicio
const producer = kafka.producer(); // Crea el productor de Kafka (el componente que ENVÍA mensajes)

// --- SIMULACIÓN DE PAGO ---

// simulatePayment: simula el procesamiento de un pago con demora aleatoria
// Devuelve 'PAYMENT_SUCCESS' el 85% de las veces y 'PAYMENT_FAILED' el 15%
function simulatePayment() {
  return new Promise((resolve) => {         // Devuelve una promesa (resultado asíncrono futuro)
    setTimeout(() => {                      // setTimeout simula el tiempo que tarda un pago real (ej: llamada a banco)
      resolve(Math.random() < 0.85         // Math.random() genera número entre 0 y 1. Si es menor a 0.85 (85% probabilidad)...
        ? 'PAYMENT_SUCCESS'                // ...pago exitoso
        : 'PAYMENT_FAILED');               // ...pago fallido (el 15% restante)
    }, 200 + Math.random() * 600);         // Espera entre 200ms y 800ms (simula latencia de procesamiento bancario)
  });
}

// --- CONEXIÓN A RABBITMQ ---

// connectRabbitMQ: intenta conectarse a RabbitMQ hasta 20 veces con 2s de espera entre intentos
async function connectRabbitMQ() {
  let retries = 20; // Contador de intentos disponibles
  while (retries--) { // Decrementa el contador en cada vuelta. Cuando llega a 0, sale del bucle
    try {
      const connection = await amqp.connect(RABBITMQ_URL); // Intenta abrir la conexión con RabbitMQ
      console.log('[billing_worker] Conectado a RabbitMQ'); // Si tuvo éxito, lo avisa
      return connection;                                    // Devuelve la conexión activa
    } catch {
      console.log('[billing_worker] Reintentando RabbitMQ...'); // Fallo de conexión, avisa
      await new Promise((r) => setTimeout(r, 2000));            // Espera 2 segundos antes del próximo intento
    }
  }
  throw new Error('[billing_worker] No se pudo conectar a RabbitMQ'); // Se acabaron los intentos: lanza error fatal
}

// --- FUNCIÓN PRINCIPAL DEL WORKER ---

// startWorker: conecta a Kafka y RabbitMQ, configura la cola y empieza a procesar mensajes
async function startWorker() {
  await producer.connect();                 // Conecta el productor de Kafka (para poder enviar resultados)
  console.log('[billing_worker] Conectado a Kafka'); // Confirma conexión a Kafka

  const connection = await connectRabbitMQ();          // Establece la conexión con RabbitMQ
  const channel    = await connection.createChannel();  // Crea un canal de comunicación dentro de la conexión

  // Declara el exchange fanout (si ya existe, verifica que sea igual; si no existe, lo crea)
  await channel.assertExchange(EXCHANGE, 'fanout', { durable: true }); // 'fanout' distribuye a todas las colas. durable:true = persiste en disco

  // Declara la cola específica de este worker
  await channel.assertQueue(QUEUE, { durable: true }); // durable:true = la cola sobrevive si RabbitMQ se reinicia

  // Vincula la cola al exchange. Routing key '' es ignorada en fanout (el fanout manda a TODAS las colas vinculadas)
  await channel.bindQueue(QUEUE, EXCHANGE, ''); // Enlaza billing_queue al exchange order_tasks

  console.log('[billing_worker] Esperando órdenes en billing_queue...'); // Avisa que está listo y escuchando

  // channel.consume: registra una función que se ejecutará cada vez que llegue un mensaje a billing_queue
  channel.consume(QUEUE, async (msg) => {      // 'msg' es el mensaje recibido de RabbitMQ
    if (!msg) return;                          // Si msg es null (puede ocurrir cuando el consumer se cancela), lo ignora
    try {
      const order  = JSON.parse(msg.content.toString()); // Convierte el contenido del mensaje (bytes → string → objeto JS)
      console.log('[billing_worker] Orden recibida:', order);   // Imprime la orden en consola

      const status = await simulatePayment();              // Llama a la simulación y espera el resultado (SUCCESS o FAILED)
      const event  = { order_id: order.order_id, status }; // Construye el objeto de evento con el ID de la orden y el resultado

      // Publica el resultado en el topic 'billing_events' de Kafka para que dashboard_aggregator lo procese
      await producer.send({
        topic:    TOPIC,                                           // Destino: topic 'billing_events'
        messages: [{ key: String(order.order_id), value: JSON.stringify(event) }], // key=ID de orden (permite particionado), value=evento JSON
      });

      console.log('[billing_worker] Evento enviado:', event); // Confirma que el resultado fue publicado en Kafka
      channel.ack(msg); // ACK (Acknowledge): le dice a RabbitMQ "procesé este mensaje correctamente, puedes borrarlo de la cola"
    } catch (err) {
      console.error('[billing_worker] Error procesando orden:', err.message);    // Si hubo un error, lo muestra
      channel.nack(msg, false, false); // NACK (Negative Acknowledge): el mensaje NO se procesó. false,false = no reencolar (lo descarta)
    }
  });
}

startWorker(); // Inicia el worker inmediatamente cuando Docker arranca el contenedor
