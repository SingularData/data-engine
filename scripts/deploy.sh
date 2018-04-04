#!/bin/bash

branch=$(git rev-parse --abbrev-ref HEAD $refname)

if [ "master" == "$branch" ]; then
  npm run build && sls deploy
  echo "Data pipeline deployed!"
fi
