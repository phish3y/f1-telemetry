FROM node:24-bookworm-slim AS vue-builder

WORKDIR /app
COPY package*.json ./
RUN npm ci

COPY . .
RUN npm run build

FROM nginx:1.29-bookworm

RUN rm /etc/nginx/conf.d/default.conf
COPY nginx/nginx.conf /etc/nginx/conf.d/

COPY --from=vue-builder /app/dist /usr/share/nginx/html

# TODO
CMD ["nginx", "-g", "daemon off;"]
