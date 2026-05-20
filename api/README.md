# E-commerce — Servicios Luna (Fase 1)

Arquitectura híbrida: **Kafka** (eventos) + **RabbitMQ** (tareas fanout) + **Node.js 20**.

## Propietario

| Persona | Servicios |
|---|---|
| **Luna** | `order_api`, `order_processor`, `analytics_service` |

## Estructura

```
api/
├── docker-compose.yml
├── CONTRACT.md
├── README.md
├── .gitignore
└── services/
    ├── order_api/
    ├── order_processor/
    └── analytics_service/
```

## Flujo (captura y delegación)

1. Cliente → `POST http://localhost:3000/order` con `{"item":"..."}`.
2. `order_api` publica en Kafka topic `new_orders`.
3. En paralelo:
   - `order_processor` (group `order_processor_group`) reenvía el JSON al exchange RabbitMQ `order_tasks` (fanout, routing key `""`).
   - `analytics_service` (group `analytics_group`) cuenta órdenes y muestra resumen cada 60 s.

## Arranque

```bash
cd api
docker compose up --build
```

> **Nota:** La imagen `bitnami/kafka:3.7` ya no existe en Docker Hub. Este proyecto usa `bitnamilegacy/kafka:3.7.0` (misma configuración KRaft).

## Pruebas

```bash
curl -X POST http://localhost:3000/order -H "Content-Type: application/json" -d "{\"item\":\"Libro\"}"
```

Verificar:

```bash
docker compose logs -f order_api
docker compose logs -f order_processor
docker compose logs -f analytics_service
```

Kafka (mensaje en `new_orders`):

```bash
docker exec kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic new_orders --from-beginning
```

RabbitMQ UI: http://localhost:15672 (`guest` / `guest`) → exchange `order_tasks` → colas `billing_queue`, `inventory_queue`, `notification_queue`.

## Contrato

Nombres de topics, groups, exchange, colas y schemas: [CONTRACT.md](./CONTRACT.md).
