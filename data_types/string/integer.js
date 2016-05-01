/**
 * Copyright 2016 DynomiteDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Integers
 *
 * Run
 * ---
 *
 * node integer.js
 *
 * Expected output
 * ---------------
 * set: country:iso=us:population 318900000
 * incr: country:iso=us:population 318900001
 * decr: country:iso=us:population 318900000
 * incrby: country:iso=us:population 318900500
 * decrby: country:iso=us:population 318900400
 * set: country:iso=cn:population 9357000000
 * updated: country:iso=cn:population
 * get: country:iso=cn:population 1357000000
 * getrange: country:iso=us:population 318 million
 * strlen: country:iso=us:population 9
 * mset: OK
 * append: country:iso=au:population 8
 * mget: 318900400,23130000,200400000
 * Unable to setnx: key country:iso=us:population already exists
 * Unable to msetnx: a key already exists
 * set: country:iso=uk:population 64100000
 * get: country:iso=uk:population 64100000
 * Waiting 2 seconds for key to expire...
 * Unable to get: country:iso=uk:population
 * set: country:iso=se:population 9593000
 * get: country:iso=se:population 9593000
 * Waiting 1 second for key to expire...
 * Unable to get: country:iso=se:population
 * getset: country:iso=us:population was 318900400
 * getset: country:iso=us:population is now 318900000
 *
 * Commands used in this file
 * --------------------------
 *
 * Get a value
 * [x] GET: get a value
 * [x] GETRANGE: get a substring
 * [x] STRLEN: get the string length of a value
 *
 * Multi-commands
 * [x] MGET: get multiple values
 *
 * Save or update a value
 * [x] INCR: increment a value by 1
 * [x] INCRBY: increment a value by a specified number
 * [x] DECR: decrement the value by 1
 * [x] DECRBY: decrement the value by a specified number
 * [x] SET: save a key/value pair
 * [x] SETNX: save a key/value pair only if the key does not already exist
 * [x] SETEX: save a key/value pair with an expiration in seconds
 * [x] PSETEX: save a key/value pair with an expiration in milliseconds
 * [x] SETRANGE: update a substring within a value
 * [x] APPEND: append a string value to the end of an existing string value
 *
 * Hybrid get and set commands
 * [x] GETSET: get a value and update the value with a new string
 *
 * The following list commands are not supported by DynomiteDB as they do not
 * work well in a clustered environment. Only use the commands below when
 * running a standalone RESP server:
 * [x] MSET: save multiple key/value pairs
 * [x] MSETNX: save multiple key/value pairs only if all keys do not already exist
 */


var redis = require('redis');

// Connect to a DynomiteDB cluster
// var db = redis.createClient(8102, 'localhost');

// Connect to a local dynomitedb-redis instance
// var db = redis.createClient(22122, 'localhost');

// Connect to a single server Redis instance
var db = redis.createClient(6379, 'localhost');

db.on('error', function(err) {
    console.log(err);
    process.exit(1);
});

/*
 * Wait until we're connected to the Dynomite server before we issue commands
 */
db.on('connect', function() {
    // Let's add some keys
    setUS('country:iso=us:population', 318900000);
});

/*
 * Add a key for the US.
 */
function setUS(key, value) {
    db.set(key, value, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('set: ' + key + ' ' + value);
            incrUS('country:iso=us:population');
        } else {
            console.log('Unable to set: ' + key + ' ' + value);
        }
    });
}

/*
 * A baby is born in the US.
 */
function incrUS(key) {
    db.incr(key, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('incr: ' + key + ' ' + reply);
            decrUS('country:iso=us:population');
        } else {
            console.log('Unable to set: ' + key + ' ' + value);
        }
    });
}

/*
 * No one gets out alive.
 */
function decrUS(key) {
    db.decr(key, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('decr: ' + key + ' ' + reply);
            incrByUS('country:iso=us:population', 500);
        } else {
            console.log('Unable to set: ' + key + ' ' + value);
        }
    });
}

/*
 * Many new babies.
 */
function incrByUS(key, increment) {
    db.incrby(key, increment, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('incrby: ' + key + ' ' + reply);
            decrByUS('country:iso=us:population', 100);
        } else {
            console.log('Unable to set: ' + key + ' ' + value);
        }
    });
}

/*
 * Too morbid.
 */
function decrByUS(key, decrement) {
    db.decrby(key, decrement, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('decrby: ' + key + ' ' + reply);
            setCN('country:iso=cn:population', 9357000000);
        } else {
            console.log('Unable to set: ' + key + ' ' + value);
        }
    });
}

/*
 * Add a key for China with an incorrect population.
 */
function setCN(key, value) {
    db.set(key, value, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('set: ' + key + ' ' + value);
            fixCN('country:iso=cn:population', 1);
        } else {
            console.log('Unable to set: ' + key + ' ' + value);
        }
    });
}

/*
 * Fix the population of China by replacing the 9 with a 1.
 */
function fixCN(key, value) {
    db.setrange(key, 0, value, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('updated: ' + key);
            getCN('country:iso=cn:population');
        } else {
            console.log('Unable to update: ' + key + ' ' + value);
            db.quit();
        }
    });
}

function getCN(key) {
    db.get(key, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('get: ' + key + ' ' + reply);
            getUSinMillions('country:iso=us:population');
        } else {
            console.log('Unable to get: ' + key);
            db.quit();
        }
    });
}

function getUSinMillions(key) {
    db.getrange(key, 0, 2, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('getrange: ' + key + ' ' + reply + ' million');
            lenUS('country:iso=us:population');
        } else {
            console.log('Unable to getrange: ' + key);
            db.quit();
        }
    });
}

function lenUS(key) {
    db.strlen(key, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('strlen: ' + key + ' ' + reply);
            setMulti();
        } else {
            console.log('Unable to strlen: ' + key);
            db.quit();
        }
    });
}

function setMulti() {
    // Population of Australia (AU) is 23 million, which we'll fix in fixAU()
    // WARNING: Do not use mset in a clustered environment.
    db.mset(
        'country:iso=au:population', 2313000,
        'country:iso=br:population', 200400000, function(err, reply) {

        if (err) throw err;

        if (reply) {
            console.log('mset: ' + reply);
            fixAU('country:iso=au:population');
        } else {
            console.log('Unable to mset.');
            db.quit();
        }
    });
}

function fixAU(key) {
    db.append(key, 0, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('append: ' + key + ' ' + reply);
            getMulti();
        } else {
            console.log('Unable to append: ' + key);
            db.quit();
        }
    });
}

function getMulti() {
    var keyUS = 'country:iso=us:population';
    var keyAU = 'country:iso=au:population';
    var keyBR = 'country:iso=br:population';
    db.mget(keyUS, keyAU, keyBR, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('mget: ' + reply);
            preventSetUS('country:iso=us:population', 1);
        } else {
            console.log('Unable to mset.');
            db.quit();
        }
    });
}

/**
 * Prevent an update to the US population since the key already exists
 */
function preventSetUS(key, value) {
    db.setnx(key, value, function(err, reply) {
        if (err) throw err;

        // reply=0 since the key already exists
        if (reply) {
            console.log('setnx: ' + key + ' ' + reply);
            db.quit();
        } else {
            // setnx should fail to update the value in this example, therefore
            // the code below should execute.
            console.log('Unable to setnx: key ' + key + ' already exists');
            preventMultiSet();
        }
    });
}

/**
 * Prevent a mset since all of the keys already exists. To prevent an update,
 * one or more keys must exist.
 */
function preventMultiSet() {
    // WARNING: Do not use msetnx in a clustered environment.
    db.msetnx(
        'country:iso=au:population', 111,
        'country:iso=br:population', 222, function(err, reply) {
        if (err) throw err;

        // reply=0 since a key already exists. msetnx will fail if one or more
        // keys already exist.
        if (reply) {
            console.log('msetnx : ' + reply);
            db.quit();
        } else {
            // msetnx should fail to create the key/value pairs in this example,
            // because >= 1 key already exists.
            console.log('Unable to msetnx: a key already exists');
            setUKWithExpiration('country:iso=uk:population', 1, 64100000);
        }
    });
}

function setUKWithExpiration(key, expiration, value) {
    // Set key that will expire after 1 second
    db.setex(key, expiration, value, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('set: ' + key + ' ' + value);
            getUK('country:iso=uk:population');
        } else {
            console.log('Unable to set: ' + key + ' ' + value);
            db.quit();
        }
    });
}

function getUK(key) {
    db.get(key, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('get: ' + key + ' ' + reply);
            // Recursively call getUK() after 2 seconds
            console.log('Waiting 2 seconds for key to expire...');
            setTimeout(getUK, 2000, 'country:iso=uk:population');
        } else {
            console.log('Unable to get: ' + key);
            setSEWithMillisecondExpiration('country:iso=se:population', 100, 9593000);
        }
    });
}

function setSEWithMillisecondExpiration(key, expiration, value) {
    // Set key that will expire after 100 milliseconds
    db.psetex(key, expiration, value, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('set: ' + key + ' ' + value);
            getSE('country:iso=se:population');
        } else {
            console.log('Unable to set: ' + key + ' ' + value);
            db.quit();
        }
    });
}

function getSE(key) {
    db.get(key, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('get: ' + key + ' ' + reply);
            // Recursively call getSE() after 1 second
            console.log('Waiting 1 second for key to expire...');
            setTimeout(getSE, 1000, 'country:iso=se:population');
        } else {
            console.log('Unable to get: ' + key);
            getsetUS('country:iso=us:population', 318900000);
        }
    });
}

/**
 * Get the current population of the US and reset it to the default value
 */
function getsetUS(key, value) {
    db.getset(key, value, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('getset: ' + key + ' was ' + reply);
            console.log('getset: ' + key + ' is now ' + value);
            db.quit();
        } else {
            console.log('Unable to set: ' + key + ' ' + value);
            db.quit();
        }
    });
}
