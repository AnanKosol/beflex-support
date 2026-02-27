FROM node:20-bookworm-slim

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY server.js ./
COPY services ./services

RUN mkdir -p /app/data /app/uploads

ENV NODE_ENV=production
EXPOSE 3000

CMD ["npm", "start"]
