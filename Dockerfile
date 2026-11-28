FROM node:alpine3.22

WORKDIR /app

# 先安装系统依赖
RUN apk update && apk upgrade && \
    apk add --no-cache openssl curl gcompat iproute2 coreutils bash unzip wget

# 复制依赖配置并安装（利用 Docker 缓存加快后续构建）
COPY package*.json ./
RUN npm install

# 复制项目代码到 /app
COPY . .

EXPOSE 3000/tcp

CMD ["node", "index.js"]
