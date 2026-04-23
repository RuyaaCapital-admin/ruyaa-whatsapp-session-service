FROM node:20-bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY package*.json ./
RUN npm install --omit=dev

COPY . .
RUN mkdir -p /data/sessions

ENV NODE_ENV=production
ENV PORT=3000
ENV SESSIONS_DIR=/data/sessions

EXPOSE 3000

CMD ["npm", "start"]
