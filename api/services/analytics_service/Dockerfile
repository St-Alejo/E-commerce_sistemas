# Usa la imagen ligera de Node.js versión 20 basada en Alpine Linux
FROM node:20-alpine

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia el archivo de definición de dependencias al contenedor
COPY package.json ./

# Instala solo las dependencias de producción para ahorrar espacio
RUN npm install --omit=dev

# Copia el código fuente del servicio al contenedor
COPY index.js ./

# Comando de inicio: ejecuta el script principal con Node.js
CMD ["node", "index.js"]
