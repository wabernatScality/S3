const async = require('async');
const uuid = require('uuid/v4');
const { errors } = require('arsenal');
const { eachSlice, getRandomInt, createMpuKey, createMpuList } =
    require('../GcpUtils');
const { minimumAllowedPartSize } = require('../../../../../constants');

module.exports = {
    _maxConcurrent: 5,
    _maxRetries: 5,

    _retryCompose(params, retry, callback) {
        // retries up each request to a maximum of 5 times before
        // declaring as a failed completeMPU
        const timeout = Math.pow(2, retry) * 1000 + getRandomInt(100, 500);
        return setTimeout((params, callback) =>
        this.composeObject(params, callback), timeout, params, (err, res) => {
            if (err) {
                if (retry <= this._maxRetries && err.statusCode === 429) {
                    return this._retryCompose(params, retry + 1, callback);
                }
                return callback(err);
            }
            return callback(null, res);
        });
    },

    _splitMerge(params, partList, level, callback) {
        // create composition of slices from the partList array
        return async.mapLimit(eachSlice.call(partList, 32), this._maxConcurrent,
        (infoParts, cb) => {
            const mpuPartList = infoParts.Parts.map(item =>
                ({ PartName: item.PartName }));
            const partNumber = infoParts.PartNumber;
            const tmpKey =
                createMpuKey(params.Key, params.UploadId, partNumber, level);
            const mergedObject = { PartName: tmpKey };
            if (mpuPartList.length < 2) {
                // else just perform a copy
                const copyParams = {
                    Bucket: params.MPU,
                    Key: tmpKey,
                    CopySource: `${params.MPU}/${mpuPartList[0].PartName}`,
                };
                return this.copyObject(copyParams, (err, res) => {
                    if (err) {
                        return cb(err);
                    }
                    mergedObject.VersionId = res.VersionId;
                    mergedObject.ETag = res.ETag;
                    return cb(null, mergedObject);
                });
            }
            const composeParams = {
                Bucket: params.MPU,
                Key: tmpKey,
                MultipartUpload: { Parts: mpuPartList },
            };
            return this._retryCompose(composeParams, 0, (err, res) => {
                if (err) {
                    return cb(err);
                }
                mergedObject.VersionId = res.VersionId;
                mergedObject.ETag = res.ETag;
                return cb(null, mergedObject);
            });
        }, (err, res) => {
            if (err) {
                return callback(err);
            }
            return callback(null, res.length);
        });
    },

    _removeParts(params, callback) {
        // marks live objects as archived for lifecycle to handle the deletions
        // delete objects from mpu bucket and overflow bucket
        return async.parallel([
            done => {
                // delete mpu bucket
                // number of objects possbile per mpu: 10,000+
                let isRunning = true;
                return async.doWhilst(doDone => {
                    const listParams = {
                        Bucket: params.MPU,
                        Prefix: params.Prefix,
                    };
                    return this.listPartsReq(listParams, (err, res) => {
                        if (err) {
                            return doDone(err);
                        }
                        isRunning = res.Parts.length < 1000 ? false : isRunning;
                        return async.mapLimit(res.Parts, 10, (item, cb) => {
                            const delParams = {
                                Bucket: params.MPU,
                                Key: item.Key,
                            };
                            return this.deleteObject(delParams, err => cb(err));
                        }, err => doDone(err));
                    });
                }, () => isRunning, err => done(err));
            },
            done => {
                // delete overflow
                // max number of objects: 10
                const listParams = {
                    Bucket: params.Overflow,
                    Prefix: params.Prefix,
                };
                return this.listPartsReq(listParams, (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    return async.mapLimit(res.Parts, 10, (item, cb) => {
                        const delParams = {
                            Bucket: params.Overflow,
                            Key: item.Key,
                        };
                        return this.deleteObject(delParams, err => cb(err));
                    }, err => done(err));
                });
            },
        ], err => callback(err));
    },

    _verifyUploadId(params, callback) {
        return this.headObject({
            Bucket: params.Bucket,
            Key: createMpuKey(params.Key, params.UploadId, 'init'),
        }, err => {
            if (err) {
                if (err.statusCode === 404) {
                    return callback(errors.NoSuchUpload);
                }
                return callback(err);
            }
            return callback();
        });
    },

    abortMultipartUpload(params, callback) {
        if (!params || !params.Key || !params.UploadId ||
            !params.Bucket || !params.MPU || !params.Overflow) {
            return callback(errors.InvalidRequest
                .customizeDescription('Missing required parameter'));
        }
        const delParams = {
            Bucket: params.Bucket,
            MPU: params.MPU,
            Overflow: params.Overflow,
            Prefix: createMpuKey(params.Key, params.UploadId),
        };
        return async.waterfall([
            next => this._verifyUploadId({
                Bucket: params.MPU,
                Key: params.Key,
                UploadId: params.UploadId,
            }, next),
            next => this._removeParts(delParams, err => next(err)),
        ], err => callback(err));
    },

    createMultipartUpload(params, callback) {
        // As google cloud does not have a create MPU function,
        // create an empty 'init' object that will temporarily store the
        // object metadata and return an upload ID to mimic an AWS MPU
        if (!params || !params.Bucket || !params.Key) {
            return callback(errors.InvalidRequest
                .customizeDescription('Missing required parameter'));
        }
        const uploadId = uuid().replace(/-/g, '');
        const mpuParams = {
            Bucket: params.Bucket,
            Key: createMpuKey(params.Key, uploadId, 'init'),
            Metadata: params.Metadata,
            ContentType: params.ContentType,
            CacheControl: params.CacheControl,
            ContentDisposition: params.ContentDisposition,
            ContentEncoding: params.ContentEncoding,
        };
        return this.putObject(mpuParams, err => {
            if (err) {
                return callback(err);
            }
            return callback(null, { UploadId: uploadId });
        });
    },

    listParts(params, callback) {
        if (!params || !params.UploadId || !params.Bucket || !params.Key) {
            return callback(errors.InvalidRequest
                .customizeDescription('Missing required parameter'));
        }
        const mpuParams = {
            Bucket: params.Bucket,
            Prefix: createMpuKey(params.Key, params.UploadId, 'parts'),
            MaxKeys: params.MaxParts,
        };
        return this.listPartsReq(mpuParams, (err, res) => {
            if (err) {
                return callback(err);
            }
            return callback(null, res);
        });
    },

    completeMultipartUpload(params, callback) {
        if (!params || !params.MultipartUpload ||
            !params.MultipartUpload.Parts || !params.UploadId ||
            !params.Bucket || !params.Key) {
            return callback(errors.InvalidRequest
                .customizeDescription('Missing required parameter'));
        }
        const partList = params.MultipartUpload.Parts;
        // verify that the part list is in order
        if (params.MultipartUpload.Parts.length <= 0) {
            return callback(errors.InvalidRequest
                .customizeDescription('You must specify at least one part'));
        }
        for (let ind = 1; ind < partList.length; ++ind) {
            if (partList[ind - 1].PartNumber >= partList[ind].PartNumber) {
                return callback(errors.InvalidPartOrder);
            }
        }
        return async.waterfall([
            next => this._verifyUploadId({
                Bucket: params.MPU,
                Key: params.Key,
                UploadId: params.UploadId,
            }, next),
            next => async.map(partList, (infoParts, cb) => {
                this.headObject({
                    Bucket: params.MPU,
                    Key: infoParts.PartName,
                }, (err, res) => {
                    if (err) {
                        return cb(err);
                    }
                    if (infoParts.PartNumber < partList.length &&
                        parseInt(res.ContentLength, 10) <
                        minimumAllowedPartSize) {
                        return cb(errors.EntityTooSmall);
                    }
                    return cb();
                });
            }, err => next(err)),
            next => (
                // first compose: in mpu bucket
                // max 10,000 => 313 parts
                // max component count per object 32
                this._splitMerge(params, partList, 'mpu1', next)
            ),
            (numParts, next) => {
                // second compose: in mpu bucket
                // max 313 => 10 parts
                // max component count per object 1024
                const parts = createMpuList(params, 'mpu1', numParts);
                return this._splitMerge(params, parts, 'mpu2', next);
            },
            (numParts, next) => {
                // copy phase: in overflow bucket
                // resetting component count by moving item between
                // different region/class buckets
                const parts = createMpuList(params, 'mpu2', numParts);
                return async.map(parts, (infoParts, cb) => {
                    const partName = infoParts.PartName;
                    const partNumber = infoParts.PartNumber;
                    const overflowKey = createMpuKey(
                        params.Key, params.UploadId, partNumber, 'overflow');
                    const copyParams = {
                        Bucket: params.Overflow,
                        Key: overflowKey,
                        CopySource: `${params.MPU}/${partName}`,
                    };
                    const copyObject = { PartName: overflowKey };
                    this.copyObject(copyParams, (err, res) => {
                        if (err) {
                            return cb(err);
                        }
                        copyObject.VersionId = res.VersionId;
                        copyObject.ETag = res.ETag;
                        return cb(null, copyObject);
                    });
                }, (err, res) => {
                    if (err) {
                        return next(err);
                    }
                    return next(null, res.length);
                });
            },
            (numParts, next) => {
                // final compose: in overflow bucket
                // number of parts to compose <= 10
                // perform final compose in overflow bucket
                const parts = createMpuList(params, 'overflow', numParts);
                const partList = parts.map(item => (
                    { PartName: item.PartName }));
                if (partList.length < 2) {
                    return next(null, partList[0].PartName);
                }
                const composeParams = {
                    Bucket: params.Overflow,
                    Key: createMpuKey(params.Key, params.UploadId, 'final'),
                    MultipartUpload: { Parts: partList },
                };
                return this._retryCompose(composeParams, 0, err => {
                    if (err) {
                        return next(err);
                    }
                    return next(null, null);
                });
            },
            (res, next) => {
                // move object from overflow bucket into the main bucket
                // retrieve initial metadata then compose the object
                const copySource = res ||
                    createMpuKey(params.Key, params.UploadId, 'final');
                return async.waterfall([
                    next => {
                        // retrieve metadata from init object in mpu bucket
                        const headParams = {
                            Bucket: params.MPU,
                            Key: createMpuKey(params.Key, params.UploadId,
                                'init'),
                        };
                        return this.headObject(headParams, (err, res) => {
                            if (err) {
                                return next(err);
                            }
                            return next(null, res.Metadata);
                        });
                    },
                    (metadata, next) => {
                        // copy the final object into the main bucket
                        const copyParams = {
                            Bucket: params.Bucket,
                            Key: params.Key,
                            Metadata: metadata,
                            MetadataDirective: 'REPLACE',
                            CopySource: `${params.Overflow}/${copySource}`,
                        };
                        this.copyObject(copyParams, (err, res) => {
                            if (err) {
                                return next(err);
                            }
                            return next(null, res);
                        });
                    },
                    (copyRes, next) => this.headObject({
                        Bucket: params.Bucket,
                        Key: params.Key,
                        VersionId: copyRes.VersionId,
                    }, (err, res) => {
                        if (err) {
                            return next(err);
                        }
                        const mpuResult = {
                            Bucket: params.Bucket,
                            Key: params.Key,
                            ETag: res.ETag,
                            ContentLength: res.ContentLength,
                            VersionId: res.VersionId,
                            MetaVersionId: res.MetaVersionId,
                            Expiration: res.Expiration,
                        };
                        return next(null, mpuResult);
                    }),
                ], (err, mpuResult) => {
                    // removing objects
                    if (err) {
                        return next(err);
                    }
                    const delParams = {
                        Bucket: params.Bucket,
                        MPU: params.MPU,
                        Overflow: params.Overflow,
                        Prefix: createMpuKey(params.Key, params.UploadId),
                    };
                    return this._removeParts(delParams, err => {
                        if (err) {
                            return next(err);
                        }
                        return next(null, mpuResult);
                    });
                });
            },
        ], (err, mpuResult) => {
            if (err) {
                return callback(err);
            }
            return callback(null, mpuResult);
        });
    },

    uploadPart(params, callback) {
        if (!params || !params.UploadId || !params.Bucket || !params.Key) {
            return callback(errors.InvalidRequest
                .customizeDescription('Missing required parameter'));
        }
        const mpuParams = {
            Bucket: params.Bucket,
            Key: createMpuKey(params.Key, params.UploadId, params.PartNumber),
            Body: params.Body,
            ContentLength: params.ContentLength,
        };
        return async.waterfall([
            next => (this._verifyUploadId(params, next)),
            next => this.putObject(mpuParams, (err, res) => {
                if (err) {
                    return next(err);
                }
                return next(null, res);
            }),
        ], (err, res) => {
            if (err) {
                return callback(err);
            }
            return callback(null, res);
        });
    },

    uploadPartCopy(params, callback) {
        if (!params || !params.UploadId || !params.Bucket || !params.Key ||
            !params.CopySource) {
            return callback(errors.InvalidRequest
                .customizeDescription('Missing required parameter'));
        }
        const mpuParams = {
            Bucket: params.Bucket,
            Key: createMpuKey(params.Key, params.UploadId, params.PartNumber),
            CopySource: params.CopySource,
        };
        return async.waterfall([
            next => (this._verifyUploadId(params, next)),
            next => this.copyObject(mpuParams, (err, res) => {
                if (err) {
                    return next(err);
                }
                const CopyPartObject = { CopyPartResult: res.CopyObjectResult };
                return next(null, CopyPartObject);
            }),
        ], (err, res) => {
            if (err) {
                return callback(err);
            }
            return callback(null, res);
        });
    },
};
