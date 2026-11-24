FROM node:26-alpine@sha256:a90bda2938f2b57112d94ee2e3722c5346efca76e4a034042b5223197567a16c AS build
WORKDIR /app

COPY frontend/package*.json ./
RUN npm ci --no-audit --no-fund

COPY frontend/ ./
RUN npm run build

FROM nginx:1.31.9-alpine@sha256:e68702efeaad0be9ced2b4d1cedb62cd15f5d51865d913955a883f653cd9e117

ENV NGINX_ENVSUBST_OUTPUT_DIR=/tmp
# Keep stdout JSON only: the image's entrypoint otherwise prints plain-text startup messages.
ENV NGINX_ENTRYPOINT_QUIET_LOGS=1

COPY frontend/nginx-main.conf /etc/nginx/nginx.conf
COPY frontend/nginx.conf /etc/nginx/templates/default.conf.template
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 8080
