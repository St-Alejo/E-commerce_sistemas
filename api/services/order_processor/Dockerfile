# Utiliza la imagen base de Node.js 20 con Alpine para un tamaño reducido
FROM node:20-alpine

# Define /app como el directorio de trabajo principal
WORKDIR /app

# Copia el archivo package.json para la instalación de dependencias
COPY package.json ./

# Instala únicamente las dependencias necesarias para ejecución en producción
RUN npm install --omit=dev

# Copia el archivo de lógica del procesador al contenedor
COPY index.js ./

# Inicia el proceso del microservicio
CMD ["node", "index.js"]
