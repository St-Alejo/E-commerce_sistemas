const amqp = require('amqplib');
const { Kafka } = require('kafkajs');

const EXCHANGE = 'order_tasks';
const QUEUE = 'inventory_queue';
const TOPIC = 'inventory_events';

const RABBITMQ_URL =
  process.env.RABBITMQ_URL;

const KAFKA_BROKER =
  process.env.KAFKA_BROKER;

const kafka = new Kafka({
  clientId: 'inventory_worker',
  brokers: [KAFKA_BROKER]
});

const producer = kafka.producer();

function simulateInventory() {

  return new Promise(resolve => {

    const delay =
      100 + Math.random() * 400;

    setTimeout(() => {

      const status =
        Math.random() < 0.90
          ? 'STOCK_RESERVED'
          : 'STOCK_OUT_OF_STOCK';

      resolve(status);

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
    '📦 Inventory Worker esperando órdenes'
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
          await simulateInventory();

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