FROM node:14-alpine as build

WORKDIR /app

# next 2 lines helping utilize docker cache
COPY package.json yarn.lock /app/
RUN yarn install

ADD . .

EXPOSE 3000
ENTRYPOINT ["yarn", "start"]