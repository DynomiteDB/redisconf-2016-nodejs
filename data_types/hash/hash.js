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
 * Hashes
 *
 * Run
 * ---
 *
 * node hash.js
 *
 * Expected output
 * ---------------
 *
 * --- BEGIN ---
 * hset: user:id=bob (1)
 * hmset: user:id=bob (OK)
 * hmset: user:id=sue (OK)
 * hlen: user:id=bob (6)
 * hlen: user:id=sue (5)
 * hexists: user:id=bob has citizenship (1)
 * hkeys: user:id=sue (fname,lname,age,gender,occupation)
 * hget: user:id=sue occupation (CTO)
 * hmget: user:id=bob (Bob,Smith,40.5)
 * hincrbyfloat: user:id=bob age (41)
 * hincrby: user:id=sue age (38)
 * hgetall: user:id=bob ({"fname":"Bob","lname":"Smith","age":"41","citizenship":"USA","gender":"male","occupation":"Software engineer"})
 * hgetall: user:id=sue ({"fname":"Sue","lname":"Jones","age":"38","gender":"female","occupation":"CTO"})
 * hvals: user:id=sue (Sue,Jones,38,female,CTO)
 * Unable to hsetnx: user:id=bob fname (0)
 * hdel: occupation from user:id=bob (1)
 * hgetall: user:id=sue ({"fname":"Sue","lname":"Jones","age":"38","gender":"female","occupation":"CTO"})
 * hscan: user:id=sue (0,fname,Sue,lname,Jones)
 * --- END ---
 *
 * Commands used in this file
 * --------------------------
 *
 * [x] HSET: Set the value associated with a field
 * [x] HMSET: Set the value(s) associated with one or more fields
 * [x] HGET: Get the value associated with a field
 * [x] HMGET: Get the value(s) associated with one or more fields
 * [x] HDEL: Delete one or more fields
 * [x] HEXISTS: Return 1 if a field exists, else return 0
 * [x] HGETALL: Get all fields and values
 * [x] HINCRBY: Increment an integer value stored at the specified field
 * [x] HINCRBYFLOAT: Increment a float value stored at the specified field
 * [x] HKEYS: Get all field names
 * [x] HLEN: Count the number of fields
 * [x] HSETNX: Set the value associated with a field, only if the field does not exist
 * [x] HSTRLEN: Get the string length of a value associated with the specified field
 * [x] HVALS: Get all values
 * [x] HSCAN: Iterate over the field/value pairs
 */


var redis = require('redis');

// Connect to a DynomiteDB cluster
// var db = redis.createClient(8102, 'localhost');

// Connect to a local dynomitedb-redis instance
// var db = redis.createClient(22122, 'localhost');

// Connect to a single server Redis instance
var db = redis.createClient(6379, 'localhost');

var bobKey = 'user:id=bob';
var sueKey = 'user:id=sue';

db.on('error', function(err) {
    console.log(err);
    process.exit(1);
});

/*
 * Wait until we're connected to the Dynomite server before we issue commands
 */
db.on('connect', function() {
    // Let's add some keys
    console.log('--- BEGIN ---');
    createBob(bobKey, 'fname', 'Bob');
});

function createBob(key, field, value) {
    db.hset(key, field, value, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hset: ' + key + ' (' + reply + ')');
            nextStep();
        } else {
            console.log('Unable to hset: ' + key + ' (' + reply + ')');
            nextStep();
        }

        function nextStep() {
            updateBob(bobKey,
                'lname', 'Smith',
                'age', 40.5,
                'citizenship', 'USA',
                'gender', 'male',
                'occupation', 'Software engineer');
        }
    });
}

function updateBob(key, f1, v1, f2, v2, f3, v3, f4, v4, f5, v5) {
    db.hmset(key, f1, v1, f2, v2, f3, v3, f4, v4, f5, v5, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hmset: ' + key + ' (' + reply + ')');
            nextStep();
        } else {
            console.log('Unable to hmset: ' + key + ' (' + reply + ')');
            nextStep();
        }
    });

    function nextStep() {
        createSue(sueKey,
            'fname', 'Sue',
            'lname', 'Jones',
            'age', 37,
            'gender', 'female',
            'occupation', 'CTO');
    }
}

function createSue(key, f1, v1, f2, v2, f3, v3, f4, v4, f5, v5) {
    db.hmset(key, f1, v1, f2, v2, f3, v3, f4, v4, f5, v5, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hmset: ' + key + ' (' + reply + ')');
            nextStep(bobKey);
        } else {
            console.log('Unable to hmset: ' + key + ' (' + reply + ')');
            nextStep(bobKey);
        }

        function nextStep(key) {
            countBobFields(key);
        }
    });
}

function countBobFields(key) {
    db.hlen(key, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hlen: ' + key + ' (' + reply + ')');
            countSueFields(sueKey);
        } else {
            console.log('Unable to hlen: ' + key);
        }
    });
}

function countSueFields(key) {
    db.hlen(key, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hlen: ' + key + ' (' + reply + ')');
            bobHasCitizenship(bobKey, 'citizenship');
        } else {
            console.log('Unable to hlen: ' + key);
        }
    });
}

function bobHasCitizenship(key, field) {
    db.hexists(key, field, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hexists: ' + key + ' has ' + field + ' (' + reply + ')');
            getSueFields(sueKey, 'citizenship');
        } else {
            console.log('hexists:  ' + key + ' does not have ' + field + ' (' + reply + ')');
            db.quit();
        }
    });
}

function getSueFields(key) {
    db.hkeys(key, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hkeys: ' + key + ' (' + reply + ')');
            getSueOccupation(sueKey, 'occupation');
        } else {
            console.log('Unable to hkeys: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function getSueOccupation(key, field) {
    db.hget(key, field, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hget: ' + key + ' ' + field + ' (' + reply + ')');
            getBobMultiFields(bobKey, 'fname', 'lname', 'age');
        } else {
            console.log('Unable to hget: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function getBobMultiFields(key, f1, f2, f3) {
    db.hmget(key, f1, f2, f3, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hmget: ' + key + ' (' + reply + ')');
            bobBDay(bobKey, 'age', 0.5);
        } else {
            console.log('Unable to hmget: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function bobBDay(key, field, increment) {
    db.hincrbyfloat(key, field, increment, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hincrbyfloat: ' + key + ' ' + field + ' (' + reply + ')');
            sueBDay(sueKey, 'age', 1);
        } else {
            console.log('Unable to hincrbyfloat: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function sueBDay(key, field, increment) {
    db.hincrby(key, field, increment, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hincrby: ' + key + ' ' + field + ' (' + reply + ')');
            viewBobAfterBDay(bobKey);
        } else {
            console.log('Unable to hincrby: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function viewBobAfterBDay(key) {
    db.hgetall(key, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hgetall: ' + key + ' (' + JSON.stringify(reply) + ')');
            viewSueAfterBDay(sueKey);
        } else {
            console.log('Unable to hgetall: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function viewSueAfterBDay(key) {
    db.hgetall(key, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hgetall: ' + key + ' (' + JSON.stringify(reply) + ')');
            viewSueValues(sueKey);
            //strLenBobOccupation(bobKey, 'occupation');
        } else {
            console.log('Unable to hgetall: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

// ONLY AVAILABLE IN REDIS >3.2 (WHICH IS NOT RELEASED YET)
// function strLenBobOccupation(key, field) {
//     db.hstrlen(key, field, function(err, reply) {
//         if (err) console.log(err);
//
//         if (reply) {
//             console.log('hstrlen: ' + key + ' ' + field + ' (' + reply + ')');
//             viewSueValues(sueKey);
//         } else {
//             console.log('Unable to hstrlen: ' + key + ' (' + reply + ')');
//             db.quit();
//         }
//     });
// }

function viewSueValues(key) {
    db.hvals(key, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hvals: ' + key + ' (' + reply + ')');
            preventSetBobFname(bobKey, 'fname', 'Frank');
        } else {
            console.log('Unable to hvals: ' + key);
            db.quit();
        }
    });
}

function preventSetBobFname(key, field, value) {
    db.hsetnx(key, field, value, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hsetnx: ' + key + ' ' + field + ' (' + reply + ')');
            db.quit();
        } else {
            // Logic flows here as hsetnx prevent changing Bob's fname
            console.log('Unable to hsetnx: ' + key + ' ' + field + ' (' + reply + ')');
            deleteBobOccupation(bobKey, 'occupation');
        }
    });
}

function deleteBobOccupation(key, field) {
    db.hdel(key, field, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hdel: ' + field + ' from ' + key + ' (' + reply + ')');
            viewSueBeforeScan(sueKey);
        } else {
            console.log('Unable to hdel: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function viewSueBeforeScan(key) {
    db.hgetall(key, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hgetall: ' + key + ' (' + JSON.stringify(reply) + ')');
            scanSue(sueKey, 0, '*name');
        } else {
            console.log('Unable to hgetall: ' + key + ' (' + reply + ')');
            db.quit();
        }
    });
}

function scanSue(key, cursor, pattern) {
    db.hscan(key, cursor, 'MATCH', pattern, function(err, reply) {
        if (err) console.log(err);

        if (reply) {
            console.log('hscan: ' + key + ' (' + reply + ')');
            console.log('--- END ---');
            db.quit();
        } else {
            console.log('Unable to hscan: ' + key);
            db.quit();
        }
    });
}
