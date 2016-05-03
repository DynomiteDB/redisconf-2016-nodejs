#!/bin/bash

redis-cli del tags 

redis-cli del 'tags:category=startups' 

redis-cli del 'tags:category=marketing'

redis-cli del 'tags:category=common'

redis-cli del 'tags:category=union'
