FROM node:12-alpine as build

WORKDIR /app
#ENV PATH /app/node_modules/.bin:$PATH

# next 2 lines helping utilize docker cache
COPY package.json yarn.lock /app/
RUN yarn install

ADD . .

EXPOSE 3000
CMD ["yarn", "start"]