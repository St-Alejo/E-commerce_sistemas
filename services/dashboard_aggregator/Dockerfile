FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY index.js ./
COPY dashboard.html ./
EXPOSE 8080
EXPOSE 8081
CMD ["node", "index.js"]
