FROM golang:1.16-alpine3.13 as builder

ARG git_commit
ENV GIT_COMMIT=$git_commit

ARG git_branch
ENV GIT_BRANCH=$git_branch

# for linters to avoid warnings. we won't use linters in Docker anyway
ENV LATEST_COMMIT="undefined"

RUN apk --no-cache add make gcc g++ linux-headers git bash ca-certificates libgcc libstdc++

WORKDIR /app

# next 2 lines helping utilize docker cache
COPY go.mod go.sum ./
RUN go mod download

ADD . .
RUN make all

FROM alpine:3.13

RUN apk add --no-cache ca-certificates libgcc libstdc++
COPY --from=builder /app/build/bin/* /usr/local/bin/

EXPOSE 8545 8546 30303 30303/udp 8080 9090 6060
