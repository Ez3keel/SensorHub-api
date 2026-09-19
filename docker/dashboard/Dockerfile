FROM node:22-alpine AS build
WORKDIR /web
COPY web/dashboard/package.json web/dashboard/package-lock.json ./
RUN npm ci
COPY web/dashboard/ ./
RUN npm run build

FROM nginx:1.27-alpine
COPY docker/dashboard/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /web/dist /usr/share/nginx/html
EXPOSE 80
