# CONTRACT.md — Acuerdo del equipo

> **Este archivo es la unica fuente de verdad sobre los nombres y formatos que cruzan fronteras entre servicios.**
> Nadie cambia nada aqui solo. Cambios = discusion en chat → consenso de los 3 → PR firmado por todos.

---

## 1. Topics de Kafka

| Topic | Publica | Consume | Proposito |
|---|---|---|---|
| `new_orders` | `order_api` | `order_processor`, `analytics_service` | Toda orden nueva |
| `billing_events` | `billing_worker` | `dashboard_aggregator` | Resultado del cobro |
| `inventory_events` | `inventory_worker` | `dashboard_aggregator` | Resultado del stock |
| `notification_events` | `notification_worker` | `dashboard_aggregator` | Resultado del email |

## 2. Consumer groups

Cada servicio consumidor de Kafka usa **exactamente** este `groupId`. Compartir un `groupId` entre servicios distintos rompe el ejercicio.

| Servicio | groupId |
|---|---|
| `order_processor` | `order_processor_group` |
| `analytics_service` | `analytics_group` |
| `dashboard_aggregator` | `dashboard_aggregator_group` |

## 3. RabbitMQ

| Elemento | Nombre | Tipo |
|---|---|---|
| Exchange | `order_tasks` | `fanout` |
| Cola | `billing_queue` | bindeada a `order_tasks` |
| Cola | `inventory_queue` | bindeada a `order_tasks` |
| Cola | `notification_queue` | bindeada a `order_tasks` |

Bindings con routing key vacia (`""`). Es lo que un fanout exige.

## 4. Schema de mensajes (JSON)

### Mensaje en `new_orders` (publicado por `order_api`)

```json
{
  "order_id": 123,
  "item": "Libro"
}
```

- `order_id` es entero positivo, incremental.
- `item` es un string libre.

### Mensaje en `order_tasks` (publicado por `order_processor`)

Es **exactamente** el mismo objeto que entro por `new_orders`. El `order_processor` no transforma, solo reenvia.

### Mensajes en los 3 topics de eventos

```json
{
  "order_id": 123,
  "status": "PAYMENT_SUCCESS"
}
```

Valores validos de `status` por topic:

| Topic | Status posibles |
|---|---|
| `billing_events` | `PAYMENT_SUCCESS`, `PAYMENT_FAILED` |
| `inventory_events` | `STOCK_RESERVED`, `STOCK_OUT_OF_STOCK` |
| `notification_events` | `EMAIL_SENT` |

## 5. Variables de entorno

| Variable | Valor en docker-compose | Notas |
|---|---|---|
| `KAFKA_BROKER` | `kafka:9092` | El hostname es el nombre del servicio en compose |
| `RABBITMQ_URL` | `amqp://guest:guest@rabbitmq:5672` | Solo para servicios que tocan RabbitMQ |

## 6. Puertos expuestos al host

| Servicio | Puerto | Para que |
|---|---|---|
| `order_api` | `3000` | POST /order |
| `dashboard_aggregator` | `8080` | WebSocket |
| `dashboard_aggregator` | `8081` | HTML estatico |
| `rabbitmq` | `15672` | UI Management (guest/guest) |
| `rabbitmq` | `5672` | AMQP (opcional desde host) |
| `kafka` | `9092` | Kafka (opcional, para kcat) |

## 7. Estado interno del `dashboard_aggregator`

```json
{
  "123": { "payment": "pending", "stock": "pending", "email": "pending" },
  "124": { "payment": "PAYMENT_SUCCESS", "stock": "STOCK_RESERVED", "email": "pending" }
}
```

WebSocket envia dos tipos de mensaje a los clientes:

```json
{ "type": "snapshot", "state": { /* todo el Map */ } }
{ "type": "update",  "order_id": 123, "state": { "payment": "...", "stock": "...", "email": "..." } }
```
