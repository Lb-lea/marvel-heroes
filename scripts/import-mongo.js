var mongodb = require("mongodb");
var csv = require("csv-parser");
var fs = require("fs");

var MongoClient = mongodb.MongoClient;
var mongoUrl = "mongodb://localhost:27017";
const dbName = "marvel";
const collectionName = "heroes";

/**
 * Make an array out ou a string.
 * @param {*} stringList
 */
function splitArrays(stringList) {
    return stringList === '' ? [] : stringList.split(",")
}

const insertHeroes = (db, callback) => {
    const collection = db.collection(collectionName);

    const heroes = [];
    fs.createReadStream('all-heroes.csv')
        .pipe(csv())

        .on('data', data => {
            heroes.push({
                "id": data["id"],
                "name": data["name"],
                "description": data["description"],
                "imageUrl": data["imageUrl"],
                "backgroundImageUrl": data["backgroundImageUrl"],
                "externalLink": data["externalLink"],
                "identity": {
                    "secretIdentities": splitArrays(data["secretIdentities"]),
                    "birthPlace": data["birthPlace"],
                    "occupation": data["occupation"],
                    "aliases": splitArrays(data["aliases"]),
                    "alignment": data["alignment"],
                    "firstAppearance": data["firstAppearance"],
                    "yearAppearance": new Date(data["yearAppearance"]),
                    "universe": data["universe"]
                },
                "apperence": {
                    "gender": data["gender"],
                    "race": data["race"],
                    "type": data["type"],
                    "height": data["height"],
                    "weight": data["weight"],
                    "eyeColor": data["eyeColor"],
                    "hairColor": data["hairColor"]
                },
                "teams": splitArrays(data["teams"]),
                "powers": splitArrays(data["powers"]),
                "partners": splitArrays(data["partners"]),
                "skills": {
                    "intelligence": data["intelligence"],
                    "strength": data["strength"],
                    "speed": data["speed"],
                    "durability": data["durability"],
                    "power": data["power"],
                    "combat": data["combat"]
                },
                "creators": splitArrays(data["creators"])
            });
        })

        .on('end', () => {
            collection.insertMany(heroes, (err, result) => {
                callback(result);
            });
        });
}

MongoClient.connect(mongoUrl, (err, client) => {
    if (err) {
        console.error(err);
        throw err;
    }
    const db = client.db(dbName);
    insertHeroes(db, result => {
        console.log(`${result.insertedCount} heroes inserted`);
        client.close();
    });
});