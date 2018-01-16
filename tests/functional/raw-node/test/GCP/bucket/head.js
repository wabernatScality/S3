const assert = require('assert');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { gcpRequestRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';

describe('GCP: HEAD Bucket', () => {
    let gcpClient;
    let config;

    before(() => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
    });

    describe('without existing bucket', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.bucketName = `somebucket-${Date.now()}`;
            return done();
        });

        it('should return 404', function testFn(done) {
            gcpClient.headBucket({
                Bucket: this.test.bucketName,
            }, err => {
                assert(err);
                assert.strictEqual(err.statusCode, 404);
                return done();
            });
        });
    });

    describe('with existing bucket', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.bucketName = `somebucket-${Date.now()}`;
            process.stdout
                .write(`Creating test bucket ${this.currentTest.bucketName}\n`);
            gcpRequestRetry({
                method: 'PUT',
                bucket: this.currentTest.bucketName,
                authCredentials: config.credentials,
            }, 0, (err, res) => {
                this.currentTest.bucketObj = {
                    MetaVersionId: res.headers['x-goog-metageneration'],
                };
                return done(err);
            });
        });

        afterEach(function afterFn(done) {
            gcpRequestRetry({
                method: 'DELETE',
                bucket: this.currentTest.bucketName,
                authCredentials: config.credentials,
            }, 0, err => {
                if (err) {
                    process.stdout
                        .write(`err deleting bucket: ${err.code}\n`);
                } else {
                    process.stdout.write('Deleted bucket\n');
                }
                return done(err);
            });
        });

        it('should get bucket information', function testFn(done) {
            gcpClient.headBucket({
                Bucket: this.test.bucketName,
            }, (err, res) => {
                assert.equal(err, null, `Expected success, but got ${err}`);
                assert.deepStrictEqual(this.test.bucketObj, res);
                return done();
            });
        });
    });
});
