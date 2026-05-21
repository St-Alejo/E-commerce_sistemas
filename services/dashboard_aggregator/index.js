// ============================================================
// SERVICIO: dashboard_aggregator
// ROL: El "cerebro" del sistema y la interfaz visual.
//      Es el servicio más complejo: escucha los 3 topics de
//      retroalimentación de Kafka, mantiene en memoria el estado
//      de cada orden, y transmite actualizaciones en tiempo real
//      al navegador del usuario usando WebSocket.
//      También sirve la página HTML del dashboard via HTTP.
// ============================================================

const http = require('http');   // Módulo nativo de Node.js para crear un servidor HTTP (sin librerías externas)
const fs   = require('fs');     // Módulo nativo de Node.js para leer archivos del disco
const path = require('path');   // Módulo nativo de Node.js para construir rutas de archivos de forma segura
const { WebSocketServer } = require('ws');      // 'ws' es una librería para WebSocket. Un WebSocket permite comunicación bidireccional en tiempo real entre servidor y navegador
const { Kafka, logLevel }  = require('kafkajs'); // KafkaJS para conectarse a Apache Kafka como consumidor

// --- CONFIGURACIÓN ---
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092'; // Servidor Kafka (enviado por Docker Compose)
const GROUP_ID     = 'dashboard_aggregator_group';              // ID de grupo único para este consumidor
const TOPICS       = ['billing_events', 'inventory_events', 'notification_events']; // Los 3 topics de Kafka que escucha este servicio
const WS_PORT      = Number(process.env.WS_PORT)   || 8080;    // Puerto del servidor WebSocket (tiempo real al navegador)
const HTTP_PORT    = Number(process.env.HTTP_PORT) || 8081;    // Puerto del servidor HTTP (para servir dashboard.html)

// --- ESTADO CENTRAL (STATEFUL) ---
// ordersState es el "cerebro" del agregador: un Map donde la clave es el order_id
// y el valor es el estado de esa orden en los 3 sistemas (pago, inventario, email)
// Ejemplo: Map { 1 => { payment: 'PAYMENT_SUCCESS', stock: 'pending', email: 'EMAIL_SENT' } }
const ordersState = new Map(); // Map vacío al inicio. Se llena conforme llegan eventos de Kafka
const wsClients   = new Set(); // Conjunto de clientes WebSocket conectados en este momento (Set evita duplicados)

// --- CONFIGURACIÓN DE KAFKA ---
const kafka = new Kafka({
  clientId: 'dashboard_aggregator',                               // Nombre de identificación en Kafka
  brokers:  [KAFKA_BROKER],                                       // Servidor Kafka al que conectarse
  logLevel: logLevel.WARN,                                        // Solo mostrar advertencias y errores en logs
  retry: { initialRetryTime: 1000, retries: Number.MAX_SAFE_INTEGER }, // Reintentos infinitos si Kafka no está listo
});

// Función auxiliar para pausar la ejecución un tiempo determinado
const sleep = (ms) => new Promise((r) => setTimeout(r, ms)); // Pausa 'ms' milisegundos

// --- FUNCIONES DE ESTADO ---

// stateAsObject: convierte el Map interno a un objeto plano para poder enviarlo como JSON
// Los Map de JavaScript no se serializan directamente con JSON.stringify, por eso necesitamos esta conversión
function stateAsObject() {
  const obj = {};                                          // Objeto plano vacío
  for (const [id, state] of ordersState.entries()) {     // Itera cada par [key, value] del Map
    obj[id] = state;                                     // Copia al objeto plano: obj[order_id] = estado
  }
  return obj; // Devuelve el objeto listo para serializar a JSON
}

// broadcast: envía un mensaje JSON a TODOS los clientes WebSocket conectados actualmente
// Así todos los navegadores abiertos reciben la actualización al mismo tiempo
function broadcast(message) {
  const data = JSON.stringify(message); // Convierte el objeto JavaScript a string JSON para enviarlo por la red
  for (const client of wsClients) {    // Itera sobre cada cliente WebSocket conectado
    if (client.readyState === 1) {     // readyState === 1 significa OPEN: solo envía si la conexión está activa
      client.send(data);               // Envía el mensaje JSON al cliente
    }
  }
}

// applyEvent: procesa un evento de Kafka y actualiza el estado interno de la orden correspondiente
// Luego notifica a todos los clientes WebSocket con el nuevo estado
function applyEvent({ order_id, status }) { // Destructuring: extrae order_id y status del objeto evento
  if (!ordersState.has(order_id)) {         // Si es la primera vez que vemos esta orden...
    ordersState.set(order_id, { payment: 'pending', stock: 'pending', email: 'pending' }); // ...crea su entrada con todos los estados en 'pending' (pendiente)
  }

  const state = ordersState.get(order_id); // Obtiene el estado actual de esta orden del Map

  // Actualiza el campo correcto según el tipo de evento recibido
  if (status === 'PAYMENT_SUCCESS' || status === 'PAYMENT_FAILED') {
    state.payment = status;              // Evento de pago: actualiza el campo 'payment'
  } else if (status === 'STOCK_RESERVED' || status === 'STOCK_OUT_OF_STOCK') {
    state.stock = status;               // Evento de inventario: actualiza el campo 'stock'
  } else if (status === 'EMAIL_SENT') {
    state.email = status;               // Evento de notificación: actualiza el campo 'email'
  } else {
    console.warn(`[dashboard_aggregator] Status desconocido: ${status}`); // Estado no reconocido: avisa en consola
    return; // Sale sin hacer broadcast (no hay cambio válido que notificar)
  }

  console.log(`[dashboard_aggregator] Orden #${order_id} actualizada:`, state); // Muestra el estado actualizado en consola

  // Envía SOLO el estado de esta orden a todos los clientes (no todo el Map completo)
  // { ...state } crea una copia del objeto para evitar que el cliente reciba una referencia mutable
  broadcast({ type: 'update', order_id, state: { ...state } }); // 'update' = actualización parcial de una orden
}

// --- KAFKA ---

// waitForKafka: espera hasta que Kafka esté disponible y respondiendo
async function waitForKafka() {
  const admin = kafka.admin(); // Cliente administrativo de Kafka para verificar conectividad
  while (true) {               // Bucle hasta que tenga éxito
    try {
      await admin.connect();       // Intenta conectar
      await admin.listTopics();    // Verifica que responde listando los topics
      await admin.disconnect();    // Cierra la conexión administrativa
      return;                      // Kafka está listo, sale del bucle
    } catch (err) {
      console.warn(`[dashboard_aggregator] Kafka no listo: ${err.message}. Reintentando...`); // Kafka aún no disponible
      await sleep(2000); // Espera 2 segundos antes del siguiente intento
    }
  }
}

// startKafkaConsumer: suscribe este servicio a los 3 topics de eventos y procesa cada mensaje
async function startKafkaConsumer() {
  await waitForKafka(); // Primero verifica que Kafka está listo

  const consumer = kafka.consumer({ groupId: GROUP_ID }); // Crea el consumidor con grupo único
  await consumer.connect();                                 // Conecta el consumidor a Kafka

  // Se suscribe a los 3 topics a la vez con una sola llamada
  await consumer.subscribe({ topics: TOPICS, fromBeginning: false }); // fromBeginning:false = ignora historial, solo mensajes nuevos

  console.log(`[dashboard_aggregator] Consumiendo: ${TOPICS.join(', ')} (group: ${GROUP_ID})`); // Confirma suscripción

  // Procesa cada mensaje que llegue a cualquiera de los 3 topics
  await consumer.run({
    eachMessage: async ({ topic, message }) => { // Se ejecuta por cada mensaje. 'topic' indica cuál de los 3 topics lo envió
      const raw = message.value?.toString();     // Lee el contenido del mensaje como texto
      if (!raw) return;                          // Ignora mensajes vacíos
      let event; // Variable para el evento parseado
      try {
        event = JSON.parse(raw);                                               // Convierte JSON texto a objeto JS
      } catch {
        console.error(`[dashboard_aggregator] JSON inválido en ${topic}`);    // JSON malformado: avisa y descarta
        return; // Sale sin procesar este mensaje
      }

      applyEvent(event); // Aplica el evento al estado interno y hace broadcast a los clientes WebSocket
    },
  });
}

// --- SERVIDOR WEBSOCKET ---

// startWebSocketServer: arranca el servidor WebSocket en el puerto 8080
// WebSocket permite que el servidor EMPUJE datos al navegador sin que el navegador lo solicite
function startWebSocketServer() {
  const wss = new WebSocketServer({ port: WS_PORT }); // Crea el servidor WebSocket en el puerto configurado

  // Se ejecuta cada vez que un nuevo navegador abre una conexión WebSocket
  wss.on('connection', (ws) => {
    wsClients.add(ws);                                                            // Registra el nuevo cliente en el Set
    console.log(`[dashboard_aggregator] Cliente WS conectado (total: ${wsClients.size})`); // Muestra cuántos clientes hay

    // Al conectarse, el cliente recibe inmediatamente un "snapshot" con el estado completo actual
    // Así si abres el dashboard después de que ya llegaron algunas órdenes, las ves todas
    ws.send(JSON.stringify({ type: 'snapshot', state: stateAsObject() })); // 'snapshot' = foto completa del estado actual

    // Cuando el cliente cierra la pestaña del navegador o se desconecta
    ws.on('close', () => {
      wsClients.delete(ws); // Elimina el cliente del Set de clientes activos
    });

    // Si ocurre un error en la conexión WebSocket
    ws.on('error', (err) => {
      console.error('[dashboard_aggregator] Error WS:', err.message); // Muestra el error
      wsClients.delete(ws); // Elimina el cliente problemático del Set
    });
  });

  console.log(`[dashboard_aggregator] WebSocket en ws://0.0.0.0:${WS_PORT}`); // Confirma que el servidor WebSocket está activo
}

// --- SERVIDOR HTTP ---

// startHttpServer: sirve la página HTML del dashboard cuando el usuario abre el navegador
function startHttpServer() {
  const htmlPath = path.join(__dirname, 'dashboard.html'); // Construye la ruta absoluta al archivo dashboard.html

  // Crea el servidor HTTP con un handler que responde a cada petición
  const server = http.createServer((req, res) => {
    if (req.method === 'GET' && (req.url === '/' || req.url === '/dashboard.html')) { // Si piden la raíz o el archivo HTML
      fs.readFile(htmlPath, (err, data) => {  // Lee el archivo dashboard.html del disco
        if (err) {                            // Si hubo error leyendo el archivo (ej: no existe)
          res.writeHead(500);                 // Responde con código 500 (Internal Server Error)
          res.end('Error al cargar el dashboard'); // Mensaje de error al navegador
          return; // Termina el handler
        }
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); // Responde 200 OK, indicando que es HTML en UTF-8
        res.end(data); // Envía el contenido del archivo HTML al navegador
      });
    } else {
      res.writeHead(404); res.end('Not found'); // Cualquier otra URL: responde 404 (No encontrado)
    }
  });

  server.listen(HTTP_PORT, () =>
    console.log(`[dashboard_aggregator] Dashboard en http://0.0.0.0:${HTTP_PORT}`)); // Inicia el servidor HTTP y avisa en qué puerto
}

// --- FUNCIÓN PRINCIPAL ---
async function main() {
  startWebSocketServer(); // Arranca el servidor WebSocket primero (para que esté listo antes de que lleguen eventos)
  startHttpServer();      // Arranca el servidor HTTP (para servir la página del dashboard)
  console.log(`[dashboard_aggregator] Conectando a Kafka en ${KAFKA_BROKER}...`); // Avisa que va a conectar a Kafka
  await startKafkaConsumer(); // Arranca el consumidor de Kafka (queda corriendo indefinidamente)
}

// Lanza main() y captura cualquier error fatal durante el arranque
main().catch((err) => {
  console.error('[dashboard_aggregator] Fallo al iniciar:', err); // Muestra el error
  process.exit(1); // Termina el proceso para que Docker lo reinicie automáticamente
});
