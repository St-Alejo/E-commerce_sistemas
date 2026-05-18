const amqp = require('amqplib');
const { Kafka } = require('kafkajs');

const RABBITMQ_URL =
  process.env.RABBITMQ_URL ||
  'amqp://guest:guest@rabbitmq:5672';

const KAFKA_BROKER =
  process.env.KAFKA_BROKER ||
  'kafka:9092';

const EXCHANGE = 'order_tasks';
const QUEUE = 'billing_queue';
const TOPIC = 'billing_events';

const kafka = new Kafka({
  clientId: 'billing_worker',
  brokers: [KAFKA_BROKER]
});

const producer = kafka.producer();

function simulatePayment() {
  return new Promise(resolve => {

    const delay = 200 + Math.random() * 600;

    setTimeout(() => {

      const success = Math.random() < 0.85;

      resolve(
        success
          ? 'PAYMENT_SUCCESS'
          : 'PAYMENT_FAILED'
      );

    }, delay);
  });
}

async function connectRabbitMQ() {

  let retries = 20;

  while (retries) {

    try {

      const connection = await amqp.connect(RABBITMQ_URL);

      console.log('✅ Conectado a RabbitMQ');

      return connection;

    } catch (error) {

      console.log('⏳ Reintentando conexión RabbitMQ...');

      retries--;

      await new Promise(resolve =>
        setTimeout(resolve, 2000)
      );
    }
  }

  throw new Error('❌ No se pudo conectar a RabbitMQ');
}

async function startWorker() {

  try {

    await producer.connect();

    console.log('✅ Conectado a Kafka');

    const connection = await connectRabbitMQ();

    const channel = await connection.createChannel();

    await channel.assertExchange(
      EXCHANGE,
      'fanout',
      {
        durable: true
      }
    );

    await channel.assertQueue(
      QUEUE,
      {
        durable: true
      }
    );

    await channel.bindQueue(
      QUEUE,
      EXCHANGE,
      ''
    );

    console.log('🚀 Billing Worker esperando órdenes...');

    channel.consume(
      QUEUE,
      async (msg) => {

        if (!msg) return;

        try {

          const order = JSON.parse(
            msg.content.toString()
          );

          console.log('📦 Orden recibida:', order);

          const status =
            await simulatePayment();

          const event = {
            order_id: order.order_id,
            status
          };

          await producer.send({
            topic: TOPIC,
            messages: [
              {
                key: String(order.order_id),
                value: JSON.stringify(event)
              }
            ]
          });

          console.log('💳 Evento enviado:', event);

          channel.ack(msg);

        } catch (error) {

          console.error(
            '❌ Error procesando orden:',
            error.message
          );

          channel.nack(msg, false, false);
        }
      }
    );

  } catch (error) {

    console.error(
      '❌ Error iniciando worker:',
      error
    );
  }
}

startWorker();