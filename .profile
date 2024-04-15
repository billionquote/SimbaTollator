#!/bin/sh
eval $(ssh-agent -s)
ssh-add /Users/Bardia/.ssh/heroku_deploy_key

