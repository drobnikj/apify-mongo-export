const Apify = require('apify');
const MongoClient = require('mongodb').MongoClient;
const Promise = require('bluebird');
const { createTunnel, closeTunnel } = require('proxy-chain');

const ISODateRegExp = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}(?:\.\d*))(?:Z|(\+|-)([\d|:]*))?$/;

const tearDown = async (mongoClient, tunnel) => {
    await mongoClient.close();
    if (tunnel) await closeTunnel(tunnel);
};

const parseDates = (key, value) => {
    if (typeof value === 'string' && ISODateRegExp.test(value)) {
        return new Date(value);
    }
    return value;
};

/**
 * Parses input of actor with all nested dates in it.
 * @return {object}
 */
const getInputWithParsedDates = async () => {
    const input = await Apify.getInput();
    const inputJSON = JSON.stringify(input);
    return JSON.parse(inputJSON, parseDates);
};

Apify.main(async () => {
    let { dbName, query, fields = {}, collectionName, mongoUrl, concurrency = 10, useTunnel, estimatedItemsCount } = await getInputWithParsedDates();

    mongoUrl = process.env.MONGO_URL || mongoUrl;

    if (!query || !collectionName || !mongoUrl) throw new Error('Fields query, collectionName and mongoUrl are required!');

    // Connect to MongoDB
    let tunnel;
    if (useTunnel) {
        const proxyUrl = Apify.getApifyProxyUrl({ groups: ['StaticUS3'] });

        const [wholeString, protocol, credentials, host, additionalDetails] = mongoUrl.match(/(mongodb):\/\/(.*)@([^/]*)\/?(.*)/);

        // Create Tunnel for mongoDb connection
        console.log('Creating tunnel fo connection to mongo DB.');
        tunnel = await createTunnel(proxyUrl, host);
        mongoUrl = `${protocol}://${credentials}@${tunnel}/${additionalDetails}`;
    }


    const mongoClient = await MongoClient.connect(mongoUrl);

    const db = dbName ? await mongoClient.db(dbName) : await mongoClient.db();
    const selectedCollection = await db.collection(collectionName);

    // If it fails creates index on query!!!
    let itemsCount;
    if (estimatedItemsCount) {
        itemsCount = estimatedItemsCount;
    } else {
        try {
            itemsCount = await selectedCollection.count(query, { maxTimeMS: 1200000 });
        } catch (err) {
            await tearDown(mongoClient, tunnel);
            throw new Error('Can not get count for exporting items, you need to create index for your query!');
        }
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
