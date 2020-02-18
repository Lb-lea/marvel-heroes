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

/**
 * Make an array out ou a string.
 * @param {*} stringList 
 */
function splitArrays(stringList) {
    return stringList === '' ? [] : stringList.split(",")
}

//Clean index beforehand
var cleanUp = client.indices.delete({ index: heroesIndexName })
suggest = {
    "mappings": {
        "properties": {
            "suggest": {
                "type": "completion"
            }
        }
    }
}
var sugest = client.indices.create({
    index: heroesIndexName,
    body: suggest
});

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
            "gender": data["gender"],
            "imageUrl": data["imageUrl"],
            "description": data["description"],
            "secretIdentities": splitArrays(data["secretIdentities"]),
            "aliases": splitArrays(data["aliases"]),
            "universe": splitArrays(data["universe"]),
            "teams": splitArrays(data["teams"]),
            "partners": splitArrays(data["partners"]),
            "suggest":
                [
                    {
                        "input": data["name"],
                        "weight": 10
                    },
                    {
                        "input": splitArrays(data["aliases"]),
                        "weight": 9
                    },
                    {
                        "input": splitArrays(data["secretIdentities"]),
                        "weight": 9
                    }
                ]

        }
        heroes.push(hero)
        //console.log(hero);
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

