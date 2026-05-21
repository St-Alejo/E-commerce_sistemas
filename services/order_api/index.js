// ============================================================
// SERVICIO: order_api
// ROL: Es la "puerta de entrada" del sistema. Recibe pedidos
//      de los clientes por internet (HTTP) y los mete al
//      sistema de mensajes Kafka para que otros servicios
//      los procesen. No procesa nada él mismo.
// ============================================================

const express = require('express'); // Express es un mini-servidor web para Node.js. Lo usamos para recibir peticiones HTTP (como cuando alguien llena un formulario)
const path = require('path');       // 'path' es una herramienta de Node.js para construir rutas de archivos de forma segura (funciona igual en Windows y Linux)
const { Kafka, logLevel } = require('kafkajs'); // KafkaJS es la librería que nos permite hablar con Apache Kafka desde Node.js. Kafka es el sistema de mensajería central

// --- CONFIGURACIÓN INICIAL ---
// process.env.VARIABLE permite leer valores que Docker pasa al contenedor sin hardcodearlos
const PORT         = Number(process.env.PORT)         || 3000;         // Puerto donde escucha el servidor HTTP. Si Docker no pasa PORT, usa 3000 por defecto
const KAFKA_BROKER = process.env.KAFKA_BROKER         || 'kafka:9092'; // Dirección de Kafka. 'kafka' es el nombre del contenedor en la red Docker, 9092 es su puerto
const TOPIC        = 'new_orders';                                      // Nombre del "canal" de Kafka donde publicamos las órdenes nuevas (un topic es como una bandeja de entrada)

let nextOrderId = 1;  // Contador auto-incremental para dar un ID único a cada orden (1, 2, 3, 4...)
let producer   = null; // Variable que guardará la "conexión activa" con Kafka para poder enviar mensajes. Empieza vacía (null)

// --- CONFIGURACIÓN DE KAFKA ---
const kafka = new Kafka({
  clientId: 'order_api',                          // Nombre con el que este servicio se identifica ante Kafka (para logs y monitoreo)
  brokers:  [KAFKA_BROKER],                       // Lista de servidores Kafka a los que conectarse. Aquí solo hay uno
  logLevel: logLevel.WARN,                        // Nivel de detalle de los logs: WARN solo muestra advertencias y errores, no mensajes normales (para no llenar la consola)
  retry: {
    initialRetryTime: 1000,          // Si falla la conexión, espera 1000ms (1 segundo) antes del primer reintento
    retries: Number.MAX_SAFE_INTEGER, // Número máximo de reintentos: prácticamente infinito. El servicio nunca se rinde
  },
});

// --- FUNCIONES DE UTILIDAD ---

// sleep: pausa la ejecución durante 'ms' milisegundos. Es como hacer Thread.sleep() en Java
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms)); // Promise + setTimeout = forma moderna de "esperar" en JavaScript asíncrono
}

// connectProducer: crea y conecta un "productor" de Kafka. Un productor es el componente que ENVÍA mensajes
async function connectProducer() {
  const p = kafka.producer(); // Crea el objeto productor usando la configuración de Kafka definida arriba
  await p.connect();          // Abre la conexión real con el servidor Kafka. 'await' espera a que termine antes de continuar
  return p;                   // Devuelve el productor listo para usar
}

// waitForProducer: bucle infinito que intenta conectarse a Kafka hasta que lo logra
// Es necesario porque al arrancar con Docker, Kafka tarda unos segundos en estar listo
async function waitForProducer() {
  while (true) { // Bucle infinito: sigue intentando hasta que conecte
    try {
      producer = await connectProducer();                                          // Intenta conectar
      console.log(`[order_api] Productor conectado (topic: ${TOPIC})`);          // Si tuvo éxito, imprime mensaje en consola
      return;                                                                      // Sale del bucle cuando la conexión es exitosa
    } catch (err) {
      console.warn(`[order_api] Kafka no disponible: ${err.message}. Reintentando...`); // Si falló, muestra el error
      await sleep(2000);                                                                  // Espera 2 segundos antes de intentar de nuevo
    }
  }
}

// ensureProducer: si la conexión con Kafka se cayó, la restablece antes de enviar
async function ensureProducer() {
  if (!producer) {                                                // Si 'producer' es null, significa que no hay conexión activa
    producer = await connectProducer();                          // Crea una nueva conexión
    console.log('[order_api] Productor Kafka reconectado');      // Avisa en consola que se reconectó
  }
}

// validateOrderBody: revisa que el cuerpo del pedido tenga los datos correctos
// Devuelve null si todo está bien, o un mensaje de error si hay problema
function validateOrderBody(body) {
  if (!body || typeof body !== 'object') {                               // Si no hay body, o no es un objeto JSON
    return 'El cuerpo debe ser un objeto JSON';                          // Devuelve este mensaje de error
  }
  if (typeof body.item !== 'string' || body.item.trim() === '') {       // Si 'item' no existe, no es texto, o está vacío
    return 'El campo "item" es obligatorio y debe ser un string no vacío'; // Devuelve este mensaje de error
  }
  return null; // null significa "sin errores", el pedido es válido
}

// publishOrder: envía la orden al topic 'new_orders' de Kafka
// Si falla el envío, intenta reconectar y reenviar una vez más
async function publishOrder(order) {
  try {
    await producer.send({
      topic:    TOPIC,                                          // El "canal" de Kafka donde se publica: 'new_orders'
      messages: [{ value: JSON.stringify(order) }],            // El contenido del mensaje: la orden convertida a texto JSON
    });
  } catch (err) {
    console.warn('[order_api] Fallo al publicar, reintentando conexión...', err.message); // Si el envío falló, avisa
    producer = null;                   // Marca la conexión como rota para forzar reconexión
    await ensureProducer();            // Reconecta con Kafka
    await producer.send({             // Intenta enviar el mensaje por segunda vez
      topic:    TOPIC,
      messages: [{ value: JSON.stringify(order) }],
    });
  }
}

// --- FUNCIÓN PRINCIPAL ---
async function main() {
  console.log(`[order_api] Conectando a Kafka en ${KAFKA_BROKER}...`); // Avisa que está iniciando
  await waitForProducer();                                               // Espera hasta conectar con Kafka antes de abrir el servidor HTTP

  const app = express(); // Crea la aplicación web con Express (el servidor HTTP)
  app.use(express.json()); // Middleware: le dice a Express que lea automáticamente el cuerpo de las peticiones como JSON

  // Ruta GET /: devuelve la página HTML de la tienda cuando alguien abre el navegador
  app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'store.html')); // __dirname = carpeta donde está este archivo. Envía el archivo store.html al navegador
  });

  // Ruta POST /order: recibe una nueva orden, la valida y la publica en Kafka
  app.post('/order', async (req, res) => {
    const validationError = validateOrderBody(req.body); // Valida los datos que llegaron en el cuerpo de la petición
    if (validationError) {                               // Si hay error de validación...
      return res.status(400).json({ error: validationError }); // Responde con código 400 (Bad Request) y el mensaje de error
    }

    const order = {
      order_id: nextOrderId++,      // Asigna el siguiente ID disponible (y luego incrementa el contador para el próximo pedido)
      item:     req.body.item.trim(), // Toma el campo 'item' del cuerpo, .trim() elimina espacios al inicio/fin
    };

    try {
      await publishOrder(order);                            // Envía la orden a Kafka
      console.log('[order_api] Orden publicada:', order);   // Registra en consola que se publicó correctamente
      return res.status(201).json(order);                   // Responde con código 201 (Created) y los datos de la orden creada
    } catch (err) {
      console.error('[order_api] Error al publicar en Kafka:', err.message);       // Si falló al publicar, muestra el error
      return res.status(503).json({ error: 'No se pudo publicar la orden en Kafka' }); // Responde con código 503 (Servicio no disponible)
    }
  });

  app.listen(PORT, () => {
    console.log(`[order_api] Escuchando en http://0.0.0.0:${PORT}`); // Arranca el servidor y avisa en qué puerto está escuchando
  });
}

// Llama a main() y si falla por algún motivo inesperado, muestra el error y detiene el proceso
main().catch((err) => {
  console.error('[order_api] Fallo al iniciar:', err); // Muestra el error en consola
  process.exit(1);                                      // Cierra el proceso con código 1 (indica error). Docker lo reiniciará automáticamente
});
