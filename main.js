const Apify = require('apify');
const MongoClient = require('mongodb').MongoClient;
const Promise = require('bluebird');
const { createTunnel, closeTunnel } = require('proxy-chain');


const tearDown = async (mongoClient, tunnel) => {
    await mongoClient.close();
    if (tunnel) await closeTunnel(tunnel);
};

Apify.main(async () => {
    let { dbName, query, fields = {}, collectionName, mongoUrl, concurrency = 10, useTunnel } = await Apify.getValue('INPUT');

    mongoUrl = process.env.MONGO_URL || mongoUrl;

    if (!query || !collectionName || !mongoUrl) throw new Error('Fields query, collectionName and mongoUrl are required!');

    // TODO: Better datetime parsing
    // Object.keys(query).forEach((key) => {
    //     if (key.match(/At$/)) {
    //         if (_.isObject(query[key])) {
    //             Object.keys(query[key]).forEach((childKey) => {
    //                 query[key][childKey] = new Date(query[key][childKey]);
    //             });
    //         } else {
    //             query[key] = new Date(query[key]);
    //         }
    //     }
    // });

    // Connect to MongoDB
    let tunnel;
    if (useTunnel) {
        const proxyUrl = Apify.getApifyProxyUrl({ groups: ['StaticUS3'] });

        const [wholeString, protocol, credentials, host, additionalDetails] = mongoUrl.match(/(mongodb):\/\/(.*)@([^/]*)\/?(.*)/);

        // Create Tunel for mongoDb connection
        console.log('Creating tunnel fo connection to mongo DB.')
        tunnel = await createTunnel(proxyUrl, host);
        mongoUrl = `${protocol}://${credentials}@${tunnel}/${additionalDetails}`;
    }


    const mongoClient = await MongoClient.connect(mongoUrl);

    const db = dbName ? await mongoClient.db(dbName) : await mongoClient.db();
    const selectedCollection = await db.collection(collectionName);

    // If it fails creates index on query!!!
    let itemsCount;
    try {
        itemsCount = await selectedCollection.count(query, { maxTimeMS: 60000 });
    } catch (err) {
        await tearDown(mongoClient, tunnel);
        throw new Error('Can not get count for exporting items, you need to create index for your query!');
    }

    console.log(`Exporting ${itemsCount} to dataset!`);

    const batchesToExport = [];

    const batchLimit = 1000;
    for (let skip = 0; skip <= itemsCount; skip += batchLimit) {
        batchesToExport.push({ skip, limit: batchLimit });
    }

    await Promise.map(batchesToExport, async (batch) => {
        const { skip, limit } = batch;
        const items = await selectedCollection.find(query, { skip, limit, fields }).addCursorFlag('noCursorTimeout', true).toArray();
        console.log(`Saving ${skip} - ${skip + limit} items to dataset!`);
        await Apify.pushData(items);
    }, { concurrency });

    await tearDown(mongoClient, tunnel);

    console.log('Done.');
});
