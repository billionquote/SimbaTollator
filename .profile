#!/bin/sh
eval $(ssh-agent -s)
ssh-add /app/.ssh/heroku_deploy_key
ssh-add /Users/Bardia/.ssh/heroku_deploy_key

