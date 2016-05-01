# RedisConf 2016 Node.js code samples

This repo contains code samples for the talk titled `The Redis API: Simple, Composable, Powerful` given by Akbar S. Ahmed from DynomiteDB.

- Language: JavaScript/Node.js

The slides for the `The Redis API: Simple, Composable, Powerful` talk are available at:

**INCLUDE LINK TO SLIDES HERE**


## Directory structure

The repo is broken into 2 main categories/subdirectories:
- `data_types`: Contains a subdirectory for each data type with code samples that take advantage of the type's innate properties.
- `use_cases`: Contains a subdirectory named after a use case.`

## Table of Contents

- Data types
    1. String
        - [String](https://github.com/DynomiteDB/redisconf-2016-nodejs/blob/master/data_types/string/string.js)
        - [Integer](https://github.com/DynomiteDB/redisconf-2016-nodejs/blob/master/data_types/string/integer.js)
        - Float
        - Bitmap
        - HyperLogLog
    2. List
    3. Set
    4. Sorted set
    5. Hash
- Use cases
    1. Cache

## Coding style

The coding style in this repo uses unnested callbacks which yields both readable code that flows from top to bottom and allows the Node.js runtime to optimize performance. Please read the links below for more information on this coding style.

- https://github.com/akbarahmed/unnest-callbacks-in-javascript
- http://exponential.io/blog/unnest-callbacks/

**Note**: There is deliberate repetition of functions to simplify the example code for learning purposes.
