/* Merge content from blobs into one blob */
/* The configuration is here              */
/* Modify the configuration and npm start */

const inputContainerName = 'firstdata';
const inputFolder = 'exception/';
const outputContainerName = 'firstdata';
const blobName = 'output/exception.txt';

/* End configuration-----------------------*/


if (process.env.NODE_ENV !== 'production') {
    require('dotenv').load();
}

const path = require('path');
const storage = require('azure-storage');
const blobService = storage.createBlobService();

const listContainers = async () => {
    return new Promise((resolve, reject) => {
        blobService.listContainersSegmented(null, (err, data) => {
            if (err) {
                reject(err);
            } else {
                resolve({message: `${data.entries.length} containers`, containers: data.entries});
            }
        });
    });
};

const createContainer = async (containerName) => {
    return new Promise((resolve, reject) => {
        blobService.createContainerIfNotExists(containerName, {publicAccessLevel: 'blob'}, err => {
            if (err) {
                reject(err);
            } else {
                resolve({message: `Container '${containerName}' created`});
            }
        });
    });
};

const uploadString = async (containerName, blobName, text) => {
    return new Promise((resolve, reject) => {
        blobService.createAppendBlobFromText(containerName, blobName, text, err => {
            if (err) {
                reject(err);
            } else {
                resolve({message: `Text "${text}" is written to blob storage`});
            }
        });
    });
};

const appendString = async (containerName, blobName, text) => {
    return new Promise((resolve, reject) => {
        blobService.appendFromText(containerName, blobName, text, (err, result, response) => {
            if (err) {
                console.log("error");
                reject(err);
            } else {
                console.log("write success block of three thousand or less");
                resolve({message: `Text "${text}" is written to blob storage`});
            }
        });
    });
};


const listBlobs = async (containerName) => {
    return new Promise((resolve, reject) => {
        blobService.listBlobsSegmentedWithPrefix(containerName, inputFolder, null, (err, data) => {
            if (err) {
                reject(err);
            } else {
                resolve({message: `${data.entries.length} blobs in '${containerName}'`, blobs: data.entries});
            }
        });
    });
};

const downloadBlob = async (containerName, blobName) => {
    return new Promise((resolve, reject) => {
        blobService.getBlobToText(containerName, blobName, (err, data) => {
            if (err) {
                reject(err);
            } else {
                resolve({message: `Blob downloaded "${data}"`, text: data});
            }
        });
    });
};

const createContainerIfDoesNotExist = async (containerName) => {
    response = await listContainers();
    const containerDoesNotExist = response.containers.findIndex((container) => container.name === containerName) === -1;
    if (containerDoesNotExist) {
        await createContainer(containerName);
        console.log(`Container "${containerName}" is created`);
    }
};

function customComparator(a, b) {
    if (a.length != b.length) {
        return a.length - b.length;
    } else {
        return a < b ? -1 : 1;
    }
}

const execute = async () => {
    let response;
    var blobArray = [];
    var content = '';

    // get all the blob name in a container
    response = await listBlobs(inputContainerName);
    response.blobs.forEach((blob) => {
        blobArray.push(blob.name);
    });

    // sort all the blob names by custom comparator
    blobArray.sort(customComparator);

    // upload content into one blob to destination container
    await createContainerIfDoesNotExist(outputContainerName);
    await uploadString(outputContainerName, blobName, '');
    console.log("START MERGING ...");

    // get all content from blobs
    for (var index in blobArray) {
        response = await downloadBlob(inputContainerName, blobArray[index]);
        console.log("read from: " + blobArray[index]);
        console.log("output container name: " + outputContainerName);
        console.log("blob name: " + blobName);
        var arr = response.text.split('\n');
        var count = 0;
        var content = '';
        for (var i in arr) {
            if (count <= 3000) {
                content += arr[i] + '\n';
                count++;
            } else {
                await appendString(outputContainerName, blobName, content);
                count = 0;
                content = '\n';
            }
        }
        if (content != '') {
            await appendString(outputContainerName, blobName, content);
        }
    }

}

execute().then(() => console.log("Done")).catch((e) => console.log(e));
