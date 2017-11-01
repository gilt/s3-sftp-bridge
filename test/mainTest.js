var assert = require('assert'),
    AWS = require('aws-sdk'),
    ctx,
    main,
    Promise = require('bluebird'),
    sinon = require('sinon'),
    ssh2 = require('ssh2'),
    testHelper = require('./helpers');

describe('main', function() {
  before(function() {
    var client = new ssh2.Client();
    sinon.stub(client, 'sftp').yields(null, testHelper.sftp);
    sinon.stub(client, 'connect', function(config) { client.emit('ready'); });
    sinon.stub(ssh2, 'Client').returns(client);
    sinon.stub(AWS, 'S3').returns(testHelper.s3);
    sinon.stub(AWS, 'SQS').returns(testHelper.sqs);
    process.env = testHelper.env;
    main = testHelper.require('../main');
  });

  beforeEach(function() {
    ctx = testHelper.clearContext();
  });

  after(function() {
    ssh2.Client.restore();
    AWS.S3.restore();
    AWS.SQS.restore();
  });

  afterEach(function() {
    testHelper.s3.clear();
    testHelper.sftp.clear();
    testHelper.sqs.clear();
  });

  describe('#handle()', function() {
    it('should direct to pollSftp when applicable', function() {
      testHelper.env.CONFIG = '{"test-stream":{"sftpLocation":"dir","sftpConfig":{},"s3Location":"my-bucket"}}';
      testHelper.sftp.objects['dir/my-file.txt'] = 'Hello World!';
      return testHelper.assertContextSuccess(
        main.handle({resources: ["arn:aws:events:us-east-1:1234567890:rule/test-stream"]}, ctx),
        ctx,
        function(results) {
          assert.equal(testHelper.s3.objects['my-bucket/my-file.txt'], 'Hello World!');
        }
      );
    });

    it('should direct to newS3Object when applicable', function() {
      var s3Event = {
        "Records": [
          {
            "s3": {
              "bucket": {
                "name": "bucket-name",
              },
              "object": {
                "key": "object-key",
              }
            }
          }
        ]
      }
      testHelper.s3.objects["bucket-name/object-key"] = "Hello World!"
      testHelper.env.CONFIG = '{"test-stream":{"s3Location":"bucket-name","sftpConfig":{}}}';
      return testHelper.assertContextSuccess(
        main.newS3Object(s3Event, ctx),
        ctx,
        function() {
          assert.equal(testHelper.sftp.objects["object-key"], "Hello World!");
          assert.equal(Object.keys(testHelper.sftp.objects).length, 1);
        }
      );
    });
  });

  describe('#pollSftp()', function() {
    it('should fail if the streamNames are missing', function() {
      return testHelper.assertContextFailure(
        main.pollSftp({}, ctx),
        ctx,
        /streamNames required/
      );
    });

    it('should fail if streamName has no config', function() {
      testHelper.env.CONFIG = '{}';
      return testHelper.assertContextFailure(
        main.pollSftp({resources: ["arn:aws:events:us-east-1:1234567890:rule/test-stream"]}, ctx),
        ctx,
        /streamName \[test-stream\] not found in config/
      );
    });

    it('should fail if streamName has no s3Location', function() {
      testHelper.env.CONFIG = '{"test-stream":{"sftpConfig":{}}}';
      return testHelper.assertContextFailure(
        main.pollSftp({resources: ["arn:aws:events:us-east-1:1234567890:rule/test-stream"]}, ctx),
        ctx,
        /streamName \[test-stream\] has no s3Location/
      );
    });

    it('should copy a file', function() {
      testHelper.env.CONFIG = '{"test-stream":{"sftpLocation":"dir","sftpConfig":{},"s3Location":"my-bucket"}}';
      testHelper.sftp.objects['dir/my-file.txt'] = 'Hello World!';
      return testHelper.assertContextSuccess(
        main.pollSftp({resources: ["arn:aws:events:us-east-1:1234567890:rule/test-stream"]}, ctx),
        ctx,
        function(results) {
          assert.equal(testHelper.s3.objects['my-bucket/my-file.txt'], 'Hello World!');
        }
      );
    });


    it('should copy a file if the resources property is a string instead of an array', function() {
      testHelper.env.CONFIG = '{"test-stream":{"sftpLocation":"dir","sftpConfig":{},"s3Location":"my-bucket"}}';
      testHelper.sftp.objects['dir/my-file.txt'] = 'Hello World!';
      return testHelper.assertContextSuccess(
        main.pollSftp({resources: "arn:aws:events:us-east-1:1234567890:rule/test-stream"}, ctx),
        ctx,
        function(results) {
          assert.equal(testHelper.s3.objects['my-bucket/my-file.txt'], 'Hello World!');
        }
      );
    });

    it('should copy multiple files in the same resource', function() {
      testHelper.env.CONFIG = '{"test-stream1":{"sftpLocation":"dir","sftpConfig":{},"s3Location":"my-bucket1"},"test-stream2":{"sftpLocation":"foo","sftpConfig":{},"s3Location":"my-bucket2"}}';
      testHelper.sftp.objects['dir/my-file.txt'] = 'Hello World 1!';
      testHelper.sftp.objects['foo/my-file.txt'] = 'Hello World 2!';
      return testHelper.assertContextSuccess(
        main.pollSftp({resources: ["arn:aws:events:us-east-1:1234567890:rule/test-stream1.test-stream2"]}, ctx),
        ctx,
        function(results) {
          assert.equal(testHelper.s3.objects['my-bucket1/my-file.txt'], 'Hello World 1!');
          assert.equal(testHelper.s3.objects['my-bucket2/my-file.txt'], 'Hello World 2!');
        }
      );
    });

    it('should copy multiple files in different resources', function() {
      testHelper.env.CONFIG = '{"test-stream1":{"sftpLocation":"dir","sftpConfig":{},"s3Location":"my-bucket1"},"test-stream2":{"sftpLocation":"foo","sftpConfig":{},"s3Location":"my-bucket2"}}';
      testHelper.sftp.objects['dir/my-file.txt'] = 'Hello World 1!';
      testHelper.sftp.objects['foo/my-file.txt'] = 'Hello World 2!';
      return testHelper.assertContextSuccess(
        main.pollSftp({resources: ["arn:aws:events:us-east-1:1234567890:rule/test-stream1", "arn:aws:events:us-east-1:1234567890:rule/test-stream2"]}, ctx),
        ctx,
        function(results) {
          assert.equal(testHelper.s3.objects['my-bucket1/my-file.txt'], 'Hello World 1!');
          assert.equal(testHelper.s3.objects['my-bucket2/my-file.txt'], 'Hello World 2!');
        }
      );
    });

    it('should copy a file to multiple s3 locations', function() {
      testHelper.env.CONFIG = '{"test-stream1":{"sftpLocation":"dir/subdir","sftpConfig":{},"s3Location":["my-bucket1","my-bucket2/subdir"]}}';
      testHelper.sftp.objects['dir/subdir/my-file.txt'] = 'Hello World 1!';
      return testHelper.assertContextSuccess(
        main.pollSftp({resources: ["arn:aws:events:us-east-1:1234567890:rule/test-stream1"]}, ctx),
        ctx,
        function(results) {
          assert.equal(testHelper.s3.objects['my-bucket1/my-file.txt'], 'Hello World 1!');
          assert.equal(testHelper.s3.objects['my-bucket2/subdir/my-file.txt'], 'Hello World 1!');
        }
      );
    });
  });

  describe('#newS3Object()', function() {
    var s3Event;

    beforeEach(function() {
      s3Event = {
        "Records": [
          {
            "s3": {
              "bucket": {
                "name": "bucket-name",
              },
              "object": {
                "key": "object-key",
              }
            }
          }
        ]
      }
      testHelper.s3.objects["bucket-name/object-key"] = "Hello World!"
    });

    it('should do nothing if the bucket isn\'t in config', function() {
      testHelper.env.CONFIG = '{"test-stream":{}}';
      return testHelper.assertContextSuccess(
        main.newS3Object(s3Event, ctx),
        ctx,
        function() {
          assert.equal(Object.keys(testHelper.sftp.objects).length, 0);
        }
      );
    });

    it('should do nothing if the file is marked as synched', function() {
      testHelper.env.CONFIG = '{"test-stream":{"sftpLocation":"foo","s3Location":"bucket-name","sftpConfig":{}}}';
      testHelper.s3.metadata["bucket-name/object-key"] = {"synched":"true"};
      return testHelper.assertContextSuccess(
        main.newS3Object(s3Event, ctx),
        ctx,
        function() {
          assert.equal(Object.keys(testHelper.sftp.objects).length, 0);
        }
      );
    });

    it('should copy the file to sftp and mark as synched', function() {
      testHelper.env.CONFIG = '{"test-stream":{"sftpLocation":"foo","s3Location":"bucket-name","sftpConfig":{}}}';
      assert.equal(testHelper.s3.metadata["bucket-name/object-key"], undefined);
      return testHelper.assertContextSuccess(
        main.newS3Object(s3Event, ctx),
        ctx,
        function() {
          assert.equal(testHelper.sftp.objects["foo/object-key"], "Hello World!");
          assert.equal(Object.keys(testHelper.sftp.objects).length, 1);
          assert.equal(testHelper.s3.metadata["bucket-name/object-key"]["synched"], "true");
        }
      );
    });

    it('should copy the file from S3 subdirectory to sftp', function() {
      s3Event.Records[0].s3.object.key = "sub/dir/object-key"
      testHelper.s3.objects["bucket-name/sub/dir/object-key"] = "Hello World!"
      testHelper.env.CONFIG = '{"test-stream":{"sftpLocation":"foo","s3Location":"bucket-name","sftpConfig":{}}}';
      return testHelper.assertContextSuccess(
        main.newS3Object(s3Event, ctx),
        ctx,
        function() {
          assert.equal(testHelper.sftp.objects["foo/sub/dir/object-key"], "Hello World!");
          assert.equal(Object.keys(testHelper.sftp.objects).length, 1);
        }
      );
    });

    it('should copy the file from S3 subdirectory location and ignore others', function() {
      s3Event.Records[0].s3.object.key = "sub/dir/object-key"
      testHelper.s3.objects["bucket-name/sub/dir/object-key"] = "Hello World!"
      testHelper.env.CONFIG = '{"test-stream":{"sftpLocation":"foo","s3Location":"bucket-name/sub","sftpConfig":{}},"test-stream2":{"sftpLocation":"foo","s3Location":"bucket-name/sub2","sftpConfig":{}}}';
      return testHelper.assertContextSuccess(
        main.newS3Object(s3Event, ctx),
        ctx,
        function() {
          assert.equal(testHelper.sftp.objects["foo/dir/object-key"], "Hello World!");
          assert.equal(Object.keys(testHelper.sftp.objects).length, 1);
        }
      );
    });

    it('should copy the file from S3 to a top-level directory in sftp', function() {
      testHelper.env.CONFIG = '{"test-stream":{"s3Location":"bucket-name","sftpConfig":{}}}';
      return testHelper.assertContextSuccess(
        main.newS3Object(s3Event, ctx),
        ctx,
        function() {
          assert.equal(testHelper.sftp.objects["object-key"], "Hello World!");
          assert.equal(Object.keys(testHelper.sftp.objects).length, 1);
        }
      );
    });

    it('should copy to all matching sftp', function() {
      testHelper.env.CONFIG = '{"test-stream":{"s3Location":"bucket-name","sftpConfig":{}},"test-stream2":{"sftpLocation":"foo","s3Location":"bucket-name","sftpConfig":{}}}';
      return testHelper.assertContextSuccess(
        main.newS3Object(s3Event, ctx),
        ctx,
        function() {
          assert.equal(testHelper.sftp.objects["object-key"], "Hello World!");
          assert.equal(testHelper.sftp.objects["foo/object-key"], "Hello World!");
          assert.equal(Object.keys(testHelper.sftp.objects).length, 2);
        }
      );
    });

    it('should fail if the sftp config is missing', function() {
      testHelper.env.CONFIG = '{"test-stream":{"sftpLocation":"foo","s3Location":"bucket-name"}}';
      assert.equal(Object.keys(testHelper.sqs.messages).length, 0);
      return testHelper.assertContextSuccess(
        main.newS3Object(s3Event, ctx),
        ctx,
        function() {
          // Requeued itself by writing to SQS
          assert.equal(testHelper.sqs.messages['test'].length, 1);
        }
      );
    });
  });

  describe('#getFilePathArray()', function() {
    it('should split on directory delimiter', function() {
      assert.deepEqual(main.getFilePathArray("path/to/dir"), ["path", "to", "dir"]);
    });

    it('should ignore multiple delimiters', function() {
      assert.deepEqual(main.getFilePathArray("path//to////dir"), ["path", "to", "dir"]);
    });

    it('should strip delimiters on the ends', function() {
      assert.deepEqual(main.getFilePathArray("/path/to/dir/"), ["path", "to", "dir"]);
    });
  });

  describe('#getSftpConfig()', function() {
    it('should fail if there is no sftpConfig', function() {
      return testHelper.assertFailure(
        main.getSftpConfig({}),
        /SFTP config not found/
      );
    });

    it('should return the untouched config if it exists', function() {
      return testHelper.assertSuccess(
        main.getSftpConfig({sftpConfig: {hostname: 'foo'}}),
        function(config) {
          assert.deepEqual(config, {hostname: 'foo'});
        }
      );
    });

    it('should add the decrypted privateKey if s3PrivateKey is given', function() {
      testHelper.s3.objects['private-key-bucket/test-key'] = 'my-key';
      return testHelper.assertSuccess(
        main.getSftpConfig({sftpConfig: {hostname: 'foo', s3PrivateKey: 'private-key-bucket/test-key'}}),
        function(config) {
          assert.deepEqual(config, {hostname: 'foo', privateKey: 'my-key'});
        }
      );
    });
  });

  describe('#scheduledEventResourceToStreamNames()', function() {
    it('should remove the rule/', function() {
      assert.deepEqual(main.scheduledEventResourceToStreamNames("arn:aws:events:us-east-1:1234567890:rule/test"), ["test"]);
    });

    it('should split on "."', function() {
      assert.deepEqual(main.scheduledEventResourceToStreamNames("arn:aws:events:us-east-1:1234567890:rule/test1.test2.test3"), ["test1", "test2", "test3"]);
    });
  });

  describe('#syncSftpDir()', function() {
    var sftp = Promise.promisifyAll(testHelper.sftp);

    it('should not fail if the sftp is empty', function() {
      return testHelper.assertSuccess(
        main.syncSftpDir(sftp, 'dir', 'my-bucket'),
        function() {
          assert.equal(Object.keys(testHelper.s3.objects).length, 0);
        }
      );
    });

    it('should copy a top-level file from SFTP to top-level in S3 and write to metadata', function() {
      testHelper.sftp.objects['dir/my-file.txt'] = 'Hello World!';
      return testHelper.assertSuccess(
        main.syncSftpDir(sftp, 'dir', 'my-bucket'),
        function() {
          assert.equal(testHelper.s3.objects['my-bucket/my-file.txt'], 'Hello World!');
          assert.deepEqual(testHelper.s3.metadata['my-bucket/my-file.txt'], {synched: 'true'});
        }
      );
    });

    it('should copy a top-level file from SFTP to a subdirectory in S3', function() {
      testHelper.sftp.objects['dir/my-file.txt'] = 'Hello World!';
      return testHelper.assertSuccess(
        main.syncSftpDir(sftp, 'dir', 'my-bucket/sub/dir'),
        function() {
          assert.equal(testHelper.s3.objects['my-bucket/sub/dir/my-file.txt'], 'Hello World!');
        }
      );
    });

    it('should copy a file from a subdirectory in SFTP to S3 under the subdirectory', function() {
      testHelper.sftp.objects['dir/sub-dir/my-file.txt'] = 'Hello World!';
      return testHelper.assertSuccess(
        main.syncSftpDir(sftp, 'dir', 'my-bucket'),
        function() {
          assert.equal(testHelper.s3.objects['my-bucket/sub-dir/my-file.txt'], 'Hello World!');
        }
      );
    });

    it('should copy a file from a subdirectory in SFTP to S3 under the subdirectory and the S3 subdirectory', function() {
      testHelper.sftp.objects['dir/sub-dir/my-file.txt'] = 'Hello World!';
      return testHelper.assertSuccess(
        main.syncSftpDir(sftp, 'dir', 'my-bucket/other/sub/dir'),
        function() {
          assert.equal(testHelper.s3.objects['my-bucket/other/sub/dir/sub-dir/my-file.txt'], 'Hello World!');
        }
      );
    });

    it('should copy multiple files in the same subdirectory', function() {
      testHelper.sftp.objects['dir/sub-dir/my-file1.txt'] = 'Hello World! 1';
      testHelper.sftp.objects['dir/sub-dir/my-file2.txt'] = 'Hello World! 2';
      return testHelper.assertSuccess(
        main.syncSftpDir(sftp, 'dir', 'my-bucket/other/sub/dir'),
        function() {
          assert.equal(testHelper.s3.objects['my-bucket/other/sub/dir/sub-dir/my-file1.txt'], 'Hello World! 1');
          assert.equal(testHelper.s3.objects['my-bucket/other/sub/dir/sub-dir/my-file2.txt'], 'Hello World! 2');
        }
      );
    });

    it('should copy multiple files in the multiple subdirectories', function() {
      testHelper.sftp.objects['dir/sub-dir1/my-file.txt'] = 'Hello World! 1';
      testHelper.sftp.objects['dir/sub-dir2/my-file.txt'] = 'Hello World! 2';
      testHelper.sftp.objects['dir/my-file.txt'] = 'Hello World! 3';
      return testHelper.assertSuccess(
        main.syncSftpDir(sftp, 'dir', 'my-bucket/other/sub/dir'),
        function() {
          assert.equal(testHelper.s3.objects['my-bucket/other/sub/dir/sub-dir1/my-file.txt'], 'Hello World! 1');
          assert.equal(testHelper.s3.objects['my-bucket/other/sub/dir/sub-dir2/my-file.txt'], 'Hello World! 2');
          assert.equal(testHelper.s3.objects['my-bucket/other/sub/dir/my-file.txt'], 'Hello World! 3');
        }
      );
    });

    it('should ignore the .done directory', function() {
      testHelper.sftp.objects['dir/sub-dir/my-file.txt'] = 'Hello World! 1';
      testHelper.sftp.objects['dir/sub-dir/.done/my-file2.txt'] = 'Hello World! 2';
      return testHelper.assertSuccess(
        main.syncSftpDir(sftp, 'dir', 'my-bucket/other/sub/dir'),
        function() {
          assert.equal(testHelper.s3.objects['my-bucket/other/sub/dir/sub-dir/my-file.txt'], 'Hello World! 1');
          assert.equal(Object.keys(testHelper.s3.objects).length, 1);
        }
      );
    });

    it('should purge old files from the .done directory with a default retention', function() {
      testHelper.sftp.objects['dir/.done/my-file1.txt'] = 'Hello World! 1';
      testHelper.sftp.objects['dir/.done/my-file2.txt'] = 'Hello World! 2';
      testHelper.sftp.setTime('dir/.done/my-file2.txt', 15);
      testHelper.sftp.objects['dir/sub-dir/.done/my-file3.txt'] = 'Hello World! 3';
      testHelper.sftp.objects['dir/sub-dir/.done/my-file4.txt'] = 'Hello World! 4';
      testHelper.sftp.setTime('dir/sub-dir/.done/my-file4.txt', 15);
      return testHelper.assertSuccess(
        main.syncSftpDir(sftp, 'dir', 'my-bucket/other/sub/dir'),
        function() {
          assert.equal(testHelper.sftp.objects['dir/.done/my-file1.txt'], 'Hello World! 1');
          assert.equal(testHelper.sftp.objects['dir/.done/my-file2.txt'], null);
          assert.equal(testHelper.sftp.objects['dir/sub-dir/.done/my-file3.txt'], 'Hello World! 3');
          assert.equal(testHelper.sftp.objects['dir/sub-dir/.done/my-file4.txt'], null);
          assert.equal(Object.keys(testHelper.sftp.objects).length, 2);
        }
      );
    });

    it('should purge old files from the .done directory with a configured retention', function() {
      testHelper.sftp.objects['dir/.done/my-file1.txt'] = 'Hello World! 1';
      testHelper.sftp.setTime('dir/.done/my-file1.txt', 19);
      testHelper.sftp.objects['dir/.done/my-file2.txt'] = 'Hello World! 2';
      testHelper.sftp.setTime('dir/.done/my-file2.txt', 21);
      testHelper.sftp.objects['dir/sub-dir/.done/my-file3.txt'] = 'Hello World! 3';
      testHelper.sftp.setTime('dir/sub-dir/.done/my-file3.txt', 18);
      testHelper.sftp.objects['dir/sub-dir/.done/my-file4.txt'] = 'Hello World! 4';
      testHelper.sftp.setTime('dir/sub-dir/.done/my-file4.txt', 22);
      return testHelper.assertSuccess(
        main.syncSftpDir(sftp, 'dir', 'my-bucket/other/sub/dir', 20),
        function() {
          assert.equal(testHelper.sftp.objects['dir/.done/my-file1.txt'], 'Hello World! 1');
          assert.equal(testHelper.sftp.objects['dir/.done/my-file2.txt'], null);
          assert.equal(testHelper.sftp.objects['dir/sub-dir/.done/my-file3.txt'], 'Hello World! 3');
          assert.equal(testHelper.sftp.objects['dir/sub-dir/.done/my-file4.txt'], null);
          assert.equal(Object.keys(testHelper.sftp.objects).length, 2);
        }
      );
    });
  });
});
