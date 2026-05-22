# Usa la imagen ligera de Node.js versión 20 basada en Alpine Linux
FROM node:20-alpine

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia el archivo de definición de dependencias al contenedor
COPY package.json ./

# Instala solo las dependencias de producción (omitiendo las de desarrollo)
RUN npm install --omit=dev

# Copia el archivo de código fuente principal al contenedor
COPY index.js ./

# Informa a Docker que el contenedor escuchará en el puerto 3000
EXPOSE 3000

# Comando para iniciar la aplicación cuando el contenedor arranque
CMD ["node", "index.js"]
