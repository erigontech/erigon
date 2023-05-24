FROM docker.io/library/golang:1.20-alpine3.17 AS builder
WORKDIR /app

ADD go.mod go.mod
ADD go.sum go.sum
RUN go mod download

ADD . .

ARG GITHUB_TOKEN

RUN go build -o releaser main.go
RUN ./releaser --github-token=$GITHUB_TOKEN

FROM nginx:1.21.3-alpine

COPY page /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /app/releases.json /usr/share/nginx/html/data.json
