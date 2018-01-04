#!/bin/bash

branch=$(git rev-parse --abbrev-ref HEAD $refname)

if [ "master" == "$branch" ]; then
  npm run deploy-ci
fi
