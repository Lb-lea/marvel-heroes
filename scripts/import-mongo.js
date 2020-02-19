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
                    "yearAppearance": data["yearAppearance"],
                    "universe": data["universe"]
                },
                "appearance": {
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
                    "intelligence": data["intelligence"] === "" ? 0 : parseFloat(data["intelligence"]),
                    "strength": data["strength"] === "" ? 0 : parseFloat(data["strength"]),
                    "speed": data["speed"] === "" ? 0 : parseFloat(data["speed"]),
                    "durability": data["durability"] === 0 ? "" : parseFloat(data["durability"]),
                    "power": data["power"] === "" ? 0 : parseFloat(data["power"]),
                    "combat": data["combat"] === "" ? 0 : parseFloat(data["combat"]),
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