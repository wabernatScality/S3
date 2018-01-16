const assert = require('assert');
const async = require('async');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const {
    describeSkipIfNotMultiple,
    memLocation,
    fileLocation,
    awsLocation,
    awsLocationMismatch,
    gcpLocation,
    gcpLocationMismatch,
    withGCP,
} = require('../utils');

const bucket = 'buckettestmultiplebackendget';
const memObject = `memobject-${Date.now()}`;
const fileObject = `fileobject-${Date.now()}`;
const awsObject = `awsobject-${Date.now()}`;
const gcpObject = `gcpobject-${Date.now()}`;
const emptyObject = `emptyObject-${Date.now()}`;
const emptyAwsObject = `emptyObject-${Date.now()}`;
const emptyGcpObject = `emptyObject-${Date.now()}`;
const bigAwsObject = `bigObject-${Date.now()}`;
const bigGcpObject = `bigObject-${Date.now()}`;
const mismatchAwsObject = `mismatch-${Date.now()}`;
const mismatchGcpObject = `mismatch-${Date.now()}`;
const body = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);
const bigBodyLen = bigBody.length;
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const bigMD5 = 'f1c9645dbc14efddc7d8a322685f26eb';
const s3MismatchObject = {
    AWS: {
        s3LocationMismatch: awsLocationMismatch,
        s3ObjectMismatch: mismatchAwsObject,
    },
    GCP: {
        s3LocationMismatch: gcpLocationMismatch,
        s3ObjectMismatch: mismatchGcpObject,
    },
};

describe('Multiple backend get object', function testSuite() {
    this.timeout(30000);
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        before(() => {
            process.stdout.write('Creating bucket');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        after(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write('Error emptying/deleting bucket: ' +
                `${err}\n`);
                throw err;
            });
        });

        it('should return an error to get request without a valid bucket name',
            done => {
                s3.getObject({ Bucket: '', Key: 'somekey' }, err => {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 'MethodNotAllowed');
                    done();
                });
            });
        it('should return NoSuchKey error when no such object',
            done => {
                s3.getObject({ Bucket: bucket, Key: 'nope' }, err => {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 'NoSuchKey');
                    done();
                });
            });

        describeSkipIfNotMultiple('Complete MPU then get object on AWS ' +
        'location with bucketMatch: true ', () => {
            beforeEach(function beforeEachFn(done) {
                this.currentTest.key = `somekey-${Date.now()}`;
                bucketUtil = new BucketUtility('default', sigCfg);
                s3 = bucketUtil.s3;

                async.waterfall([
                    next => s3.createMultipartUpload({
                        Bucket: bucket, Key: this.currentTest.key,
                        Metadata: { 'scal-location-constraint': awsLocation,
                    } }, (err, res) => next(err, res.UploadId)),
                    (uploadId, next) => s3.uploadPart({
                        Bucket: bucket,
                        Key: this.currentTest.key,
                        PartNumber: 1,
                        UploadId: uploadId,
                        Body: 'helloworld' }, (err, res) => next(err, uploadId,
                        res.ETag)),
                    (uploadId, eTag, next) => s3.completeMultipartUpload({
                        Bucket: bucket,
                        Key: this.currentTest.key,
                        MultipartUpload: {
                            Parts: [
                                {
                                    ETag: eTag,
                                    PartNumber: 1,
                                },
                            ],
                        },
                        UploadId: uploadId,
                    }, err => next(err)),
                ], done);
            });
            it('should get object from MPU on AWS ' +
            'location with bucketMatch: true ', function it(done) {
                s3.getObject({
                    Bucket: bucket,
                    Key: this.test.key,
                }, (err, res) => {
                    assert.equal(err, null, 'Expected success but got ' +
                      `error ${err}`);
                    assert.strictEqual(res.ContentLength, '10');
                    assert.strictEqual(res.Body.toString(), 'helloworld');
                    assert.deepStrictEqual(res.Metadata,
                      { 'scal-location-constraint': awsLocation });
                    return done(err);
                });
            });
        });

        describeSkipIfNotMultiple('Complete MPU then get object on AWS ' +
        'location with bucketMatch: false ', () => {
            beforeEach(function beforeEachFn(done) {
                this.currentTest.key = `somekey-${Date.now()}`;
                bucketUtil = new BucketUtility('default', sigCfg);
                s3 = bucketUtil.s3;

                async.waterfall([
                    next => s3.createMultipartUpload({
                        Bucket: bucket, Key: this.currentTest.key,
                        Metadata: { 'scal-location-constraint':
                        awsLocationMismatch,
                    } }, (err, res) => next(err, res.UploadId)),
                    (uploadId, next) => s3.uploadPart({
                        Bucket: bucket,
                        Key: this.currentTest.key,
                        PartNumber: 1,
                        UploadId: uploadId,
                        Body: 'helloworld' }, (err, res) => next(err, uploadId,
                        res.ETag)),
                    (uploadId, eTag, next) => s3.completeMultipartUpload({
                        Bucket: bucket,
                        Key: this.currentTest.key,
                        MultipartUpload: {
                            Parts: [
                                {
                                    ETag: eTag,
                                    PartNumber: 1,
                                },
                            ],
                        },
                        UploadId: uploadId,
                    }, err => next(err)),
                ], done);
            });
            it('should get object from MPU on AWS ' +
            'location with bucketMatch: false ', function it(done) {
                s3.getObject({
                    Bucket: bucket,
                    Key: this.test.key,
                }, (err, res) => {
                    assert.equal(err, null, 'Expected success but got ' +
                      `error ${err}`);
                    assert.strictEqual(res.ContentLength, '10');
                    assert.strictEqual(res.Body.toString(), 'helloworld');
                    assert.deepStrictEqual(res.Metadata,
                      { 'scal-location-constraint': awsLocationMismatch });
                    return done(err);
                });
            });
        });

        describeSkipIfNotMultiple('with objects in all available backends ' +
            '(mem/file/AWS/GCP)', () => {
            before(() => {
                process.stdout.write('Putting object to mem\n');
                return s3.putObjectAsync({ Bucket: bucket, Key: memObject,
                    Body: body,
                    Metadata: { 'scal-location-constraint': memLocation } })
                .then(() => {
                    process.stdout.write('Putting object to file\n');
                    return s3.putObjectAsync({ Bucket: bucket, Key: fileObject,
                        Body: body,
                        Metadata:
                        { 'scal-location-constraint': fileLocation },
                    });
                })
                .then(() => {
                    process.stdout.write('Putting object to AWS\n');
                    return s3.putObjectAsync({ Bucket: bucket, Key: awsObject,
                        Body: body,
                        Metadata: {
                            'scal-location-constraint': awsLocation } });
                })
                .then(() => {
                    process.stdout.write('Putting 0-byte object to mem\n');
                    return s3.putObjectAsync({ Bucket: bucket, Key: emptyObject,
                        Metadata:
                        { 'scal-location-constraint': memLocation },
                    });
                })
                .then(() => {
                    process.stdout.write('Putting 0-byte object to AWS\n');
                    return s3.putObjectAsync({ Bucket: bucket,
                        Key: emptyAwsObject,
                        Metadata: {
                            'scal-location-constraint': awsLocation } });
                })
                .then(() => {
                    process.stdout.write('Putting large object to AWS\n');
                    return s3.putObjectAsync({ Bucket: bucket,
                        Key: bigAwsObject, Body: bigBody,
                        Metadata: {
                            'scal-location-constraint': awsLocation } });
                })
                .then(() => {
                    process.stdout.write('Putting object to GCP\n');
                    return s3.putObjectAsync({ Bucket: bucket, Key: gcpObject,
                        Body: body,
                        Metadata: {
                            'scal-location-constraint': gcpLocation } });
                })
                .then(() => {
                    process.stdout.write('Putting 0-byte object to GCP\n');
                    return s3.putObjectAsync({ Bucket: bucket,
                        Key: emptyGcpObject,
                        Metadata: {
                            'scal-location-constraint': gcpLocation } });
                })
                .then(() => {
                    process.stdout.write('Putting large object to GCP\n');
                    return s3.putObjectAsync({ Bucket: bucket,
                        Key: bigGcpObject, Body: bigBody,
                        Metadata: {
                            'scal-location-constraint': gcpLocation } });
                })
                .catch(err => {
                    process.stdout.write(`Error putting objects: ${err}\n`);
                    throw err;
                });
            });
            it('should get an object from mem', done => {
                s3.getObject({ Bucket: bucket, Key: memObject }, (err, res) => {
                    assert.equal(err, null, 'Expected success but got ' +
                        `error ${err}`);
                    assert.strictEqual(res.ETag, `"${correctMD5}"`);
                    done();
                });
            });
            it('should get a 0-byte object from mem', done => {
                s3.getObject({ Bucket: bucket, Key: emptyObject },
                (err, res) => {
                    assert.equal(err, null, 'Expected success but got ' +
                        `error ${err}`);
                    assert.strictEqual(res.ETag, `"${emptyMD5}"`);
                    done();
                });
            });
            it('should get a 0-byte object from AWS', done => {
                s3.getObject({ Bucket: bucket, Key: emptyAwsObject },
                (err, res) => {
                    assert.equal(err, null, 'Expected success but got error ' +
                        `error ${err}`);
                    assert.strictEqual(res.ETag, `"${emptyMD5}"`);
                    done();
                });
            });
            it('should get a 0-byte object from GCP', done => {
                s3.getObject({ Bucket: bucket, Key: emptyGcpObject },
                (err, res) => {
                    assert.equal(err, null, 'Expected success but got error ' +
                        `error ${err}`);
                    assert.strictEqual(res.ETag, `"${emptyMD5}"`);
                    done();
                });
            });
            it('should get an object from file', done => {
                s3.getObject({ Bucket: bucket, Key: fileObject },
                    (err, res) => {
                        assert.equal(err, null, 'Expected success but got ' +
                            `error ${err}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
            });
            it('should get an object from AWS', done => {
                s3.getObject({ Bucket: bucket, Key: awsObject },
                    (err, res) => {
                        assert.equal(err, null, 'Expected success but got ' +
                            `error ${err}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
            });
            it('should get an object from GCP', done => {
                s3.getObject({ Bucket: bucket, Key: gcpObject },
                    (err, res) => {
                        assert.equal(err, null, 'Expected success but got ' +
                            `error ${err}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
            });
            it('should get a large object from AWS', done => {
                s3.getObject({ Bucket: bucket, Key: bigAwsObject },
                    (err, res) => {
                        assert.equal(err, null, 'Expected success but got ' +
                            `error ${err}`);
                        assert.strictEqual(res.ETag, `"${bigMD5}"`);
                        done();
                    });
            });
            it('should get a large object from GCP', done => {
                s3.getObject({ Bucket: bucket, Key: bigGcpObject },
                    (err, res) => {
                        assert.equal(err, null, 'Expected success but got ' +
                            `error ${err}`);
                        assert.strictEqual(res.ETag, `"${bigMD5}"`);
                        done();
                    });
            });
            it('should get an object using range query from AWS', done => {
                s3.getObject({ Bucket: bucket, Key: bigAwsObject,
                    Range: 'bytes=0-9' },
                    (err, res) => {
                        assert.equal(err, null, 'Expected success but got ' +
                            `error ${err}`);
                        assert.strictEqual(res.ContentLength, '10');
                        assert.strictEqual(res.ContentRange,
                            `bytes 0-9/${bigBodyLen}`);
                        assert.strictEqual(res.ETag, `"${bigMD5}"`);
                        done();
                    });
            });
        });

        describeSkipIfNotMultiple('with bucketMatch set to false',
        withGCP(s3Type => {
            const { s3LocationMismatch, s3ObjectMismatch } = s3Type === 'GCP' ?
                s3MismatchObject.GCP : s3MismatchObject.AWS;
            beforeEach(done => {
                s3.putObject(
                    { Bucket: bucket, Key: s3ObjectMismatch, Body: body,
                Metadata: { 'scal-location-constraint': s3LocationMismatch } },
                err => {
                    assert.equal(err, null, `Err putting object: ${err}`);
                    done();
                });
            });

            it(`should get an object from ${s3Type}`, done => {
                s3.getObject({ Bucket: bucket, Key: s3ObjectMismatch },
                (err, res) => {
                    assert.equal(err, null, `Error getting object: ${err}`);
                    assert.strictEqual(res.ETag, `"${correctMD5}"`);
                    done();
                });
            });
        }));
    });
});
