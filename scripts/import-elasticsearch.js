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
            "gender": data["gender"],
            "imageUrl": data["imageUrl"],
            "description": data["description"],
            "secretIdentities": data["secretIdentities"],
            "aliases": data["aliases"],
            "universe": data["universe"],
            "teams": data["teams"],
            "partners": data["partners"]
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

