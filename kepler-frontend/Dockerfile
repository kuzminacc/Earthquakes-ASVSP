# Build stage
FROM node:18-alpine AS build
WORKDIR /app

# Update npm
RUN npm install -g npm@9

# Kopiraj package.json i instaliraj zavisnosti
COPY package.json ./
RUN npm install --silent

# Kopiraj ostatak koda
COPY . .

# Rešenje za Node 17+ OpenSSL problem
ENV NODE_OPTIONS=--openssl-legacy-provider

# Build
RUN npm run build

# Production stage
FROM nginx:alpine
COPY --from=build /app/build /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
