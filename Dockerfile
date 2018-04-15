FROM node:8-slim

ENV NODE_ENV production
ENV APP_DIR /opt/hub-indexer

WORKDIR $APP_DIR

RUN apt-get update && apt-get install -y bash  git

COPY package.json $APP_DIR
COPY package-lock.json $APP_DIR
RUN npm install
COPY . $APP_DIR

CMD [ "npm", "run", "start-engine" ]
