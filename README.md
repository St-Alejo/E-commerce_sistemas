# E-commerce Sistemas

Arquitectura de microservicios para e-commerce con **Apache Kafka** + **RabbitMQ** + **Docker Compose** + **Node.js**.

Proyecto academico: 7 microservicios desacoplados que se comunican exclusivamente por mensajes. Kafka actua como bus de eventos (log inmutable), RabbitMQ como distribuidor de comandos (exchange fanout).

---

## Inicio Rapido

### Requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y corriendo
- Puertos disponibles: `3000`, `5672`, `8080`, `8081`, `9092`, `15672`

### Levantar todo el sistema

```bash
# Desde la carpeta E-commerce_sistemas/
docker compose up --build
```

La primera vez tarda 2-3 minutos mientras se descargan las imagenes y se compilan los servicios.

### Verificar que todo esta corriendo

```bash
docker compose ps
```

Todos los servicios deben aparecer en estado `running`.

---

## Como Usar el Sistema

### 1. Crear una orden

```bash
curl -X POST http://localhost:3000/order \
  -H "Content-Type: application/json" \
  -d '{"item": "Laptop"}'
```

Respuesta esperada:
```json
{ "order_id": 1, "item": "Laptop" }
```

### 2. Ver el dashboard en tiempo real

Abrir en el navegador:
```
http://localhost:8081
```

El dashboard se actualiza automaticamente via WebSocket a medida que cada servicio procesa la orden:
- **Pago**: `PAYMENT_SUCCESS` (85%) o `PAYMENT_FAILED` (15%)
- **Inventario**: `STOCK_RESERVED` (90%) o `STOCK_OUT_OF_STOCK` (10%)
- **Notificacion**: `EMAIL_SENT` (100%)

### 3. Crear multiples ordenes

```bash
# Crear 5 ordenes de prueba
for i in {1..5}; do
  curl -s -X POST http://localhost:3000/order \
    -H "Content-Type: application/json" \
    -d "{\"item\": \"Producto $i\"}"
  echo ""
done
```

En Windows PowerShell:
```powershell
1..5 | ForEach-Object {
  Invoke-RestMethod -Method Post -Uri http://localhost:3000/order `
    -ContentType "application/json" `
    -Body "{`"item`": `"Producto $_`"}"
}
```

---

## Arquitectura

```
Cliente
  |
  | POST /order
  v
order_api (puerto 3000)
  |
  | Publica a Kafka topic: new_orders
  v
Kafka
  |
  +---> order_processor (group: order_processor_group)
  |       |
  |       | Reenvio al exchange RabbitMQ "order_tasks" (fanout)
  |       v
  |     RabbitMQ
  |       |
  |       +---> billing_queue  --> billing_worker  --> Kafka topic: billing_events
  |       +---> inventory_queue --> inventory_worker --> Kafka topic: inventory_events
  |       +---> notification_queue --> notification_worker --> Kafka topic: notification_events
  |
  +---> analytics_service (group: analytics_group)
          Cuenta ordenes en ventana de 60s, imprime resumen en logs

Kafka (topics de eventos)
  billing_events + inventory_events + notification_events
          |
          v
  dashboard_aggregator (group: dashboard_aggregator_group)
          |
          +---> WebSocket (puerto 8080) --> navegador actualiza tabla
          +---> HTTP (puerto 8081) --> sirve dashboard.html
```

---

## Servicios

| Servicio | Puerto | Responsable | Descripcion |
|---|---|---|---|
| `order_api` | `3000` | Luna | API REST que recibe ordenes y las publica en Kafka |
| `order_processor` | — | Luna | Puente Kafka -> RabbitMQ (fanout) |
| `analytics_service` | — | Luna | Contador de ordenes por ventana de 60s (logs) |
| `billing_worker` | — | Nicolas | Simula cobro (85% exito) y publica resultado en Kafka |
| `inventory_worker` | — | Nicolas | Simula reserva de stock (90% exito) y publica en Kafka |
| `notification_worker` | — | Nicolas | Simula envio de email (100% exito) y publica en Kafka |
| `dashboard_aggregator` | `8080` (WS), `8081` (HTTP) | Steven | Agrega eventos y sirve dashboard en tiempo real |

---

## Comandos Utiles

```bash
# Levantar todo (reconstruyendo imagenes)
docker compose up --build

# Levantar solo los brokers (para desarrollo)
docker compose up kafka rabbitmq

# Ver logs de un servicio especifico
docker compose logs -f dashboard_aggregator
docker compose logs -f order_api

# Ver logs de todos los servicios
docker compose logs -f

# Detener todo sin borrar datos
docker compose stop

# Detener y eliminar contenedores
docker compose down

# Detener, eliminar contenedores Y volumenes (reset completo)
docker compose down -v

# Reconstruir solo un servicio
docker compose up --build dashboard_aggregator
```

---

## Interfaces de Administracion

| URL | Credenciales | Para que |
|---|---|---|
| http://localhost:15672 | guest / guest | RabbitMQ Management UI |
| http://localhost:8081 | — | Dashboard de ordenes en tiempo real |

---

## Contrato Tecnico

Los nombres de topics, colas, schemas JSON, status y puertos estan en [CONTRACT.md](./CONTRACT.md).

No se cambia nada sin acuerdo de los 3 integrantes del equipo.

---

## Estructura del Proyecto

```
E-commerce_sistemas/
├── docker-compose.yml           # Orquestacion completa del sistema
├── CONTRACT.md                  # Fuente de verdad de nombres y formatos
├── README.md                    # Este archivo
├── .gitignore
└── services/
    ├── order_api/               # Luna: API REST + productor Kafka
    │   ├── index.js
    │   ├── package.json
    │   └── Dockerfile
    ├── order_processor/         # Luna: consumer Kafka + publisher RabbitMQ
    │   ├── index.js
    │   ├── package.json
    │   └── Dockerfile
    ├── analytics_service/       # Luna: contador de ventas por minuto
    │   ├── index.js
    │   ├── package.json
    │   └── Dockerfile
    ├── billing_worker/          # Nicolas: simulador de pagos
    │   ├── index.js
    │   ├── package.json
    │   └── Dockerfile
    ├── inventory_worker/        # Nicolas: simulador de stock
    │   ├── index.js
    │   ├── package.json
    │   └── Dockerfile
    ├── notification_worker/     # Nicolas: simulador de email
    │   ├── index.js
    │   ├── package.json
    │   └── Dockerfile
    └── dashboard_aggregator/    # Steven: dashboard en tiempo real
        ├── index.js
        ├── dashboard.html
        ├── package.json
        └── Dockerfile
```

---

## Propietarios

| Persona | Servicios |
|---|---|
| **Luna** | `order_api`, `order_processor`, `analytics_service` |
| **Nicolas** | `billing_worker`, `inventory_worker`, `notification_worker` |
| **Steven** | `dashboard_aggregator`, `docker-compose.yml`, `README.md` |
