FROM node:24-bookworm-slim AS vue-builder

WORKDIR /app
COPY package*.json ./
RUN npm ci

COPY . .
RUN npm run build

FROM nginx:1.29-bookworm

COPY nginx/nginx.conf /etc/nginx/nginx.conf

RUN rm /etc/nginx/conf.d/default.conf

COPY nginx/nginx-local.conf /etc/nginx/nginx-local.conf
COPY nginx/nginx-k8s.conf /etc/nginx/nginx-k8s.conf

RUN ln -s /etc/nginx/nginx-k8s.conf /etc/nginx/conf.d/default.conf

COPY --from=vue-builder /app/dist /usr/share/nginx/html

CMD ["nginx", "-g", "daemon off;"]
