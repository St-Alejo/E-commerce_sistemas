const amqp = require('amqplib');
const { Kafka } = require('kafkajs');

const EXCHANGE = 'order_tasks';
const QUEUE = 'notification_queue';
const TOPIC = 'notification_events';

const RABBITMQ_URL =
  process.env.RABBITMQ_URL;

const KAFKA_BROKER =
  process.env.KAFKA_BROKER;

const kafka = new Kafka({
  clientId: 'notification_worker',
  brokers: [KAFKA_BROKER]
});

const producer = kafka.producer();

function simulateNotification() {

  return new Promise(resolve => {

    const delay =
      50 + Math.random() * 250;

    setTimeout(() => {

      resolve('EMAIL_SENT');

    }, delay);
  });
}

async function connectRabbitMQ() {

  let retries = 20;

  while (retries) {

    try {

      const connection =
        await amqp.connect(RABBITMQ_URL);

      console.log('✅ RabbitMQ conectado');

      return connection;

    } catch (error) {

      console.log('⏳ Reintentando RabbitMQ...');

      retries--;

      await new Promise(resolve =>
        setTimeout(resolve, 2000)
      );
    }
  }

  throw new Error(
    '❌ No se pudo conectar RabbitMQ'
  );
}

async function start() {

  await producer.connect();

  console.log('✅ Kafka conectado');

  const connection =
    await connectRabbitMQ();

  const channel =
    await connection.createChannel();

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

  console.log(
    '📧 Notification Worker esperando órdenes'
  );

  channel.consume(
    QUEUE,
    async msg => {

      if (!msg) return;

      try {

        const order = JSON.parse(
          msg.content.toString()
        );

        console.log(
          '📥 Orden recibida:',
          order
        );

        const status =
          await simulateNotification();

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

        console.log(
          '📤 Evento enviado:',
          event
        );

        channel.ack(msg);

      } catch (error) {

        console.error(
          '❌ Error:',
          error.message
        );

        channel.nack(msg, false, false);
      }
    }
  );
}

start();