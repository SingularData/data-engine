FROM node:8-slim

ENV NODE_ENV production
ENV APP_DIR /sdn-data-engine

WORKDIR $APP_DIR

RUN apt-get update && apt-get install -y \
  python2.7 \
  make \
  build-essential

RUN ln -s /usr/bin/python2.7 /usr/bin/python

COPY package.json $APP_DIR
COPY package-lock.json $APP_DIR
RUN npm install
COPY . $APP_DIR

CMD [ "npm", "run", "start-engine" ]
