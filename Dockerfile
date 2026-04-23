FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY . .
RUN mkdir -p /data/sessions

ENV NODE_ENV=production
ENV PORT=3000
ENV SESSIONS_DIR=/data/sessions

EXPOSE 3000

CMD ["npm", "start"]
