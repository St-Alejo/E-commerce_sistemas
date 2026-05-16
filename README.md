# E-commerce_sistemas

Arquitectura hibrida de E-commerce con **Apache Kafka** + **RabbitMQ** + **Docker Compose** + **Node.js**.

Proyecto academico: microservicios desacoplados que se comunican exclusivamente por mensajes. Kafka como bus de eventos (log inmutable), RabbitMQ como distribuidor de comandos (fanout).

## Sprint 0 — Validacion inicial

Antes de que nadie escriba codigo de un servicio, los 3 deben validar que los brokers levantan en su maquina:

```bash
docker compose up
```

Validacion:
- Abrir http://localhost:15672 (usuario `guest`, contrasena `guest`) → debe verse la UI de RabbitMQ.
- El log de Kafka no debe mostrar errores rojos.

Si esto funciona en las maquinas de los 3, ya pueden empezar a trabajar en sus respectivas ramas.

## Propietarios

| Persona | Servicios | Rama de feature |
|---|---|---|
| **Luna** | `order_api`, `order_processor`, `analytics_service` | `feat/order_api`, `feat/order_processor`, `feat/analytics_service` |
| **Nicolas** | `billing_worker`, `inventory_worker`, `notification_worker` | `feat/billing_worker`, `feat/inventory_worker`, `feat/notification_worker` |
| **Steven** | `dashboard_aggregator`, `docker-compose.yml`, `README.md` | `feat/dashboard_aggregator` |

Reglas anti-pisado completas y guia de desarrollo: ver **Guia de Desarrollo del Equipo.docx** (entregado por Steven).

## Contrato tecnico

Los nombres de topics, colas, schemas, status y puertos estan en [CONTRACT.md](./CONTRACT.md). **No se cambian sin acuerdo de los 3.**

## Estructura

```
E-commerce_sistemas/
├── docker-compose.yml         # Kafka + RabbitMQ. Steven agrega bloques de servicios via PR.
├── CONTRACT.md                # Nombres y schemas (no cambiar solo).
├── README.md                  # Este archivo.
├── .gitignore
└── services/
    ├── order_api/             # Luna
    ├── order_processor/       # Luna
    ├── analytics_service/     # Luna
    ├── billing_worker/        # Nicolas
    ├── inventory_worker/      # Nicolas
    ├── notification_worker/   # Nicolas
    └── dashboard_aggregator/  # Steven
```

## Comandos utiles (cuando ya haya servicios implementados)

```bash
# Levantar todo
docker compose up --build

# Solo los brokers (Sprint 0 y debugging)
docker compose up kafka rabbitmq

# Ver logs de un servicio
docker compose logs -f order_api

# Detener todo
docker compose down

# Probar (cuando order_api este implementado)
curl -X POST http://localhost:3000/order \
  -H "Content-Type: application/json" \
  -d '{"item":"Libro"}'
```
