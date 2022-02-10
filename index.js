const { Storage } = require('@google-cloud/storage');
const { v1 } = require('@google-cloud/pubsub');

const storage = new Storage();
const pubSubClient = new v1.PublisherClient();
const topic = pubSubClient.projectTopicPath(process.env.PROJECT, process.env.TOPIC_TEST);

let fileName;
let bankslipBlock = process.env.BANKSLIP_BLOCK;
let publishSuccess = true;

exports.extractCnab = async (event) => {

    console.log(`Event received. File: ${event.name}`);

    const bucketName = event.bucket;
    const processingBucket = process.env.PROCESSING_BUCKET;
    const finalBucket = process.env.FINAL_BUCKET;
    fileName = event.name;

    let file = storage.bucket(bucketName).file(fileName);

    return (async () => {
        let fileData = await getFileData(file).catch(console.error);
        if (!fileData) {
            console.log('Data not available.');
            return;
        }
        await moveFile(bucketName, fileName, processingBucket).catch(console.error);
        await processFile(fileData);
        if (publishSuccess) {
            await moveFile(processingBucket, fileName, finalBucket).catch(console.error);
        }

        fileName = null;
    })()
};

async function moveFile(origin, fileName, destination) {
    await storage.bucket(origin).file(fileName).move(storage.bucket(destination));
}

async function getFileData(remoteFile) {
    console.log('Getting file data...');
    return new Promise((resolve, reject) => {
        let buf = '';
        remoteFile
            .createReadStream()
            .on('data', d => (buf += d))
            .on('end', () => resolve(buf))
            .on('error', e => reject(e));
    });
}

async function processFile(fileData) {
    console.log('Processing file...')
    const lineLength = parseInt(process.env.LINE_LENGTH);
    let data = stripCRLF(fileData);
    const numberOfLines = data.length / lineLength;
    if (numberOfLines < 3)
        return;

    console.log(`Lines retrieved from file: ${numberOfLines}`)
    const branch = data.substring(26, 30);
    const number = data.substring(31, 40);
    const account = { number, branch };
    let numberOfBankSlip = 0;
    let payloads = [];
    let message = 0;
    const retrySettings = getRetrySettings();
    
    for (let i = 0; i < numberOfLines; i++) {
        let line = data.substring(lineLength * i, lineLength * (i + 1));

        if (line.substring(0, 1) == '7') {
            let bankslip = getPayloadFromLine(line);
            bankslip.data.account = account;
            payloads.push(bankslip);
            numberOfBankSlip++;
        }

        if (payloads.length > 0 && payloads.length == bankslipBlock || numberOfLines - 2 - message * bankslipBlock < bankslipBlock && payloads.length == numberOfLines - 2 - message * bankslipBlock) {
            let dataBuffer = Buffer.from(JSON.stringify(payloads));
            let messagesElement = {
                data: dataBuffer,
            };
            let messages = [messagesElement];

            let request = {
                topic,
                messages,
            };

            try {
                const messageId = await pubSubClient.publish(request, { retry: retrySettings });
                console.log(`Message ${messageId} published.`);
            } catch(error) {
                publishSuccess = false;
                console.log('--- Error publishing message in pub/sub ---');
                console.error(error);
                break;
            }
            message++;
            console.log(`Bankslips sent = ${payloads.length}/${numberOfLines - 2} - message #${message}`);
            payloads = [];
        }
    }
    console.log(`Total bankslips sent #${numberOfBankSlip}`)
}

function stripCRLF(data) {
    return data.replace(/\r?\n|\r/g, '');
}

function getPayloadFromLine(line) {
    let alias = line.substring(63, 80);
    let documentNumber = line.substring(3, 17);
    let amount = parseFloat(line.substring(126, 137) + '.' + line.substring(137, 139));
    const dueDate = line.substring(120, 126);
    let emissionFee = false;
    let type = process.env.BANKSLIP_TYPE;
    let number = line.substring(22, 31);
    let branch = line.substring(17, 22);
    let document = '';
    if (line.substring(218, 220) == "01") {
        document = line.substring(223, 234);
    } else {
        document = line.substring(220, 234);
    }
    let name = line.substring(234, 271);
    let tradeName = line.substring(234, 271);
    let addressLine = line.substring(274, 314);
    let zipCode = line.substring(326, 334);
    let city = line.substring(334, 349);
    let state = line.substring(349, 351);
    let controlCode = line.substring(38, 63);
    let yourNumber = line.substring(110, 120);
    let payload = {
        cnabFileName: fileName,
        controlCode,
        yourNumber,
        data: {
            alias, documentNumber, amount, dueDate, emissionFee, type,
            account: { number, branch },
            payer: {
                document, name, tradeName,
                address: { addressLine, city, state, zipCode }
            }
        }
    };
    return payload;
}

function getRetrySettings() {
    return {
        retryCodes: [
            10, // 'ABORTED'
            1, // 'CANCELLED',
            4, // 'DEADLINE_EXCEEDED'
            13, // 'INTERNAL'
            8, // 'RESOURCE_EXHAUSTED'
            14, // 'UNAVAILABLE'
            2, // 'UNKNOWN'
        ],
        backoffSettings: {
            // The initial delay time, in milliseconds, between the completion
            // of the first failed request and the initiation of the first retrying request.
            initialRetryDelayMillis: 100,
            // The multiplier by which to increase the delay time between the completion
            // of failed requests, and the initiation of the subsequent retrying request.
            retryDelayMultiplier: 1.3,
            // The maximum delay time, in milliseconds, between requests.
            // When this value is reached, retryDelayMultiplier will no longer be used to increase delay time.
            maxRetryDelayMillis: 60000,
            // The initial timeout parameter to the request.
            initialRpcTimeoutMillis: 5000,
            // The multiplier by which to increase the timeout parameter between failed requests.
            rpcTimeoutMultiplier: 1.0,
            // The maximum timeout parameter, in milliseconds, for a request. When this value is reached,
            // rpcTimeoutMultiplier will no longer be used to increase the timeout.
            maxRpcTimeoutMillis: 600000,
            // The total time, in milliseconds, starting from when the initial request is sent,
            // after which an error will be returned, regardless of the retrying attempts made meanwhile.
            totalTimeoutMillis: 600000,
        },
    };
}