FROM node:20-bookworm-slim
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY shared ./shared
COPY notifier ./notifier
CMD ["node", "notifier/server.mjs"]
