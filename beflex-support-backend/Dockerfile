FROM node:20-bookworm-slim

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY server.js ./
COPY services ./services
COPY scripts ./scripts
COPY routes ./routes

RUN mkdir -p /app/data /app/uploads \
	&& chown -R node:node /app

ENV NODE_ENV=production
EXPOSE 3000
USER node

CMD ["npm", "start"]
