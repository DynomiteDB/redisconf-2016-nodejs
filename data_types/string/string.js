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
 * Strings
 *
 * Run
 * ---
 *
 * node string.js
 *
 * Expected output
 * ---------------
 *
 * set: state:usps=ca California
 * set: state:usps=ny New York
 * set: state:usps=co Xolorado
 * updated: state:usps=co
 * get: state:usps=co Colorado
 * getrange: state:usps=ny York
 * strlen: state:usps=ny 8
 * mset: OK
 * append: state:usps=ia 4
 * mget: California,Iowa,New York
 * Unable to setnx: key state:usps=co already exists
 * Unable to msetnx: a key already exists
 * set: state:usps=al Alabama
 * get: state:usps=al Alabama
 * Waiting 2 seconds for key to expire...
 * Unable to get: state:usps=al
 * set: state:usps=ks Kansas
 * get: state:usps=ks Kansas
 * Waiting 1 second for key to expire...
 * Unable to get: state:usps=ks
 * getset: state:usps=ca was California
 * getset: state:usps=ca is now California, USA
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
    setCA('state:usps=ca', 'California');
});

/*
 * Add a key for California.
 */
function setCA(key, value) {
    db.set(key, value, function(err, reply) {
        if (err) throw err;
        
        if (reply) {
            console.log('set: ' + key + ' ' + value);
            setNY('state:usps=ny', 'New York');
        } else {
            console.log('Unable to set: ' + key + ' ' + value);
        }
    });
}

/*
 * Add a key for New York.
 */
function setNY(key, value) {
    db.set(key, value, function(err, reply) {
        if (err) throw err;
        
        if (reply) {
            console.log('set: ' + key + ' ' + value);
            setCO('state:usps=co', 'Xolorado');
        } else {
            console.log('Unable to set: ' + key + ' ' + value);
        }
    });
}

/*
 * Add a key for Colorado.
 */
function setCO(key, value) {
    db.set(key, value, function(err, reply) {
        if (err) throw err;
        
        if (reply) {
            console.log('set: ' + key + ' ' + value);
            fixCO('state:usps=co','C')
        } else {
            console.log('Unable to set: ' + key + ' ' + value);
            db.quit();
        }
    });
}

/*
 * Fix the spelling of Colorado by replacing the X with a C.
 */
function fixCO(key, value) {
    db.setrange(key, 0, value, function(err, reply) {
        if (err) throw err;
        
        if (reply) {
            console.log('updated: ' + key);
            getCO('state:usps=co');
        } else {
            console.log('Unable to update: ' + key + ' ' + value);
            db.quit();
        }
    });
}

function getCO(key) {
    db.get(key, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('get: ' + key + ' ' + reply);
            getYork('state:usps=ny');
        } else {
            console.log('Unable to get: ' + key);
            db.quit();
        }
    });
}

function getYork(key) {
    db.getrange(key, 4, -1, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('getrange: ' + key + ' ' + reply);
            lenNY('state:usps=ny');
        } else {
            console.log('Unable to getrange: ' + key);
            db.quit();
        }
    });
}

function lenNY(key) {
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
    // WARNING: Do not use mset in a clustered environment.
    db.mset('state:usps=oh', 'Ohio', 'state:usps=ia', 'Iow', function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('mset: ' + reply);
            fixIA('state:usps=ia');
        } else {
            console.log('Unable to mset.');
            db.quit();
        }
    });
}

function fixIA(key) {
    db.append(key, 'a', function(err, reply) {
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
    var keyCA = 'state:usps=ca';
    var keyIA = 'state:usps=ia';
    var keyNY = 'state:usps=ny';
    db.mget(keyCA, keyIA, keyNY, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('mget: ' + reply);
            updateCO('state:usps=co', 'Wrong name');
        } else {
            console.log('Unable to mset.');
            db.quit();
        }
    });
}

function updateCO(key, value) {
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
            multiSetNew();
        }
    });
}


function multiSetNew() {
    // WARNING: Do not use msetnx in a clustered environment.
    db.msetnx('state:usps=ia', 'Iowa', 'state:usps=ut', 'Utah', function(err, reply) {
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
            setALWithExpiration('state:usps=al', 1, 'Alabama');
        }
    });
}

function setALWithExpiration(key, expiration, value) {
    // Set key that will expire after 1 second
    db.setex(key, expiration, value, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('set: ' + key + ' ' + value);
            getAL('state:usps=al');
        } else {
            console.log('Unable to set: ' + key + ' ' + value);
            db.quit();
        }
    });
}

function getAL(key) {
    db.get(key, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('get: ' + key + ' ' + reply);
            // Recursively call getUK() after 2 seconds
            console.log('Waiting 2 seconds for key to expire...');
            setTimeout(getAL, 2000, 'state:usps=al');
        } else {
            console.log('Unable to get: ' + key);
            setKSWithMillisecondExpiration('state:usps=ks', 100, 'Kansas');
        }
    });
}

function setKSWithMillisecondExpiration(key, expiration, value) {
    // Set key that will expire after 100 milliseconds
    db.psetex(key, expiration, value, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('set: ' + key + ' ' + value);
            getKS('state:usps=ks');
        } else {
            console.log('Unable to set: ' + key + ' ' + value);
            db.quit();
        }
    });
}

function getKS(key) {
    db.get(key, function(err, reply) {
        if (err) throw err;

        if (reply) {
            console.log('get: ' + key + ' ' + reply);
            // Recursively call getSE() after 1 second
            console.log('Waiting 1 second for key to expire...');
            setTimeout(getKS, 1000, 'state:usps=ks');
        } else {
            console.log('Unable to get: ' + key);
            getsetCA('state:usps=ca', 'California, USA');
        }
    });
}

function getsetCA(key, value) {
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