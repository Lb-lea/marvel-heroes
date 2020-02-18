const csv = require('csv-parser');
const fs = require('fs');
const _ = require('lodash');

const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' })
const heroesIndexName = 'heroes'


function createBulkInsertQuery(anomalies) {
    const body = heroes.reduce((acc, hero) => {
        const { id, ...params } = hero;
        acc.push({ index: { _index: heroesIndexName, _id: hero.id } })
        acc.push(params)
        return acc
    }, []);

    return { body };
}

//Clean index beforehand
var cleanUp = client.indices.delete({ index: heroesIndexName })


var heroes = [];

// Read CSV file
fs.createReadStream('all-heroes.csv')
    .pipe(csv({
        separator: ','
    }))
    .on('data', (data) => {

        var hero = {
            "id": data["id"],
            "name": data["name"],
            "description": data["description"],
            "imageUrl": data["imageUrl"],
            "backgroundImageUrl": data["backgroundImageUrl"],
            "externalLink": data["externalLink"],
            "secretIdentities": data["secretIdentities"],
            "birthPlace": data["birthPlace"],
            "occupation": data["occupation"],
            "aliases": data["aliases"],
            "alignment": data["alignment"],
            "firstAppearance": data["firstAppearance"],
            "yearAppearance": new Date(data["yearAppearance"]),
            "universe": data["universe"],
            "gender": data["gender"],
            "race": data["race"],
            "type": data["type"],
            "height": data["height"],
            "weight": data["weight"],
            "eyeColor": data["eyeColor"],
            "hairColor": data["hairColor"],
            "teams": data["teams"],
            "powers": data["powers"],
            "partners": data["partners"],
            "intelligence": data["intelligence"],
            "strength": data["strength"],
            "speed": data["speed"],
            "durability": data["durability"],
            "power": data["power"],
            "combat": data["combat"],
            "creators": data["creators"]
        }
        heroes.push(hero)
        //console.log(problem);
    })
    .on('end', () => {
        // TODO il y a peut être des choses à faire à la fin aussi ?
        _.chunk(heroes, 20000).forEach(chunk => client.bulk(createBulkInsertQuery(chunk)), (err, resp) => {
            if (err) console.trace(err.message);
            else console.log(`Inserted ${resp.body.items.length} heroes`);
            client.close();
            console.log(`${resp.items.length} heroes inserted`);
        });
        console.log('Terminated!');
    });

