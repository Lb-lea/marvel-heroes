const csv = require('csv-parser');
const fs = require('fs');
const _ = require('lodash');

const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' })
const heroesIndexName = 'heroes'


function createBulkInsertQuery(heroes) {
    const body = heroes.reduce((acc, hero) => {
        const { object_id, ...params } = hero;
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

async function run() {
    //Clean index beforehand
    if ((await client.indices.exists({ index: heroesIndexName })).body) {
        var cleanUp = await client.indices.delete({ index: heroesIndexName })
    }

    suggest = {
        "mappings": {
            "properties": {
                "suggest": {
                    "type": "completion"
                }
            }
        }
    }
    var sugest = await client.indices.create({
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
                "secretIdentities": data["secretIdentities"],
                "aliases": data["aliases"],
                "universe": data["universe"],
                "teams": data["teams"],
                "partners": data["partners"],
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

            _.chunk(heroes, 20000).forEach(chunk => client.bulk(createBulkInsertQuery(chunk)), (err, resp) => {
                if (err) console.trace(err.message);
                else console.log(`Inserted ${resp.body.items.length} heroes`);
                client.close();
                console.log(`${resp.items.length} heroes inserted`);
            });
            console.log('Terminated!');
        });

}


run().catch(console.error);