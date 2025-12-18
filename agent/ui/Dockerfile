FROM node:24-alpine AS build

WORKDIR /ui
COPY package.json package-lock.json /ui/
RUN npm ci --no-audit --no-fund
COPY . /ui
RUN npm run build


FROM nginx:1.27-alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf

RUN rm -rf /usr/share/nginx/html/*
RUN mkdir -p /usr/share/nginx/html/ui
COPY --from=build /ui/dist/ /usr/share/nginx/html/ui/
