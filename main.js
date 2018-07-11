var AWS = require('aws-sdk'),
    Promise = require('bluebird')
    conf = Promise.promisifyAll(require('./lib/configHelper')),
    s3 = Promise.promisifyAll(require('node-s3-encryption-client')),
    awsS3 = Promise.promisifyAll(new AWS.S3()),
    sftpHelper = require('./lib/sftpHelper'),
    sqs = Promise.promisifyAll(new AWS.SQS());

exports.handle = function(event, context) {
  if (event.Records) {
    return exports.newS3Object(event, context);
  } else {
    return exports.pollSftp(event, context);
  }
}

exports.pollSftp = function(event, context) {
  return Promise.try(function() {
    var streamNames = [];
    if (event.resources) {
      if (Array.isArray(event.resources)) {
        event.resources.forEach(function(resource) {
          streamNames = streamNames.concat(exports.scheduledEventResourceToStreamNames(resource));
        });
      } else {
        streamNames = exports.scheduledEventResourceToStreamNames(event.resources);
      }
    }
    if (streamNames.length == 0) throw new Error("streamNames required for config discovery")
    return conf.getConfigAsync(context)
    .then(function(config) {
      return Promise.map(
        streamNames,
        function(streamName) {
          streamName = streamName.trim();
          if (streamName == 'poll') {
            return exports.pollSqs(context);
          } else {
            var streamConfig = config[streamName];
            if (!streamConfig) throw new Error("streamName [" + streamName + "] not found in config");
            return exports.getSftpConfig(streamConfig)
            .then(function(sftpConfig) {
              var s3Location = streamConfig.s3Location;
              if (!s3Location) throw new Error("streamName [" + streamName + "] has no s3Location");
              console.info("Attempting connection for [" + streamName + "]: host[" + sftpConfig.host + "], username[" + sftpConfig.username + "]");
              return sftpHelper.withSftpClient(sftpConfig, function(sftp) {
                return exports.syncSftpDir(sftp, streamConfig.sftpLocation || '/', s3Location, streamConfig.fileRetentionDays);
              })
              .then(function(results) {
                console.info("[" + streamName + "]: Moved " + flatten(results).length + " files from SFTP to S3");
                return results;
              });
            });
          }
        }
      );
    });
  })
  .then(function(result) {
    context.succeed(flatten(result));
  })
  .catch(function(err) {
    if (err.level == 'client-timeout') {
      console.warn('ClientTimeoutException: ' + err);
      context.succeed([]);
    } else {
      console.error('UnknownException: ' + (err.stack || err));
      context.fail(err);
      throw err;
    }
  });
}

exports.pollSqs = function(context) {
  return sqs.getQueueUrlAsync({
    QueueName: context.functionName
  })
  .then(function(queueData) {
    return Promise.mapSeries(
      Array.apply(null, {length: 10}).map(Number.call, Number),
      function(i) {
        return sqs.receiveMessageAsync({
          QueueUrl: queueData.QueueUrl,
          MaxNumberOfMessages: 10
        })
        .then(function(messages) {
          return Promise.mapSeries(
            messages.Messages || [],
            function(message) {
              return internalNewS3Object(JSON.parse(message.Body), context)
              .then(function(results) {
                return sqs.deleteMessageAsync({
                  QueueUrl: queueData.QueueUrl,
                  ReceiptHandle: message.ReceiptHandle
                })
                .then(function(data) {
                  return results;
                });
              });
            }
          );
        });
      }
    );
  });
}

function internalNewS3Object(event, context) {
  return Promise.try(function() {
    return conf.getConfigAsync(context)
    .then(function(config) {
      return Promise.map(
        event.Records,
        function(record) {
          var fullS3Path = record.s3.bucket.name + '/' + record.s3.object.key;
          var newObjectS3Path = exports.getFilePathArray(fullS3Path);
          return s3.getObjectAsync({
            Bucket: record.s3.bucket.name,
            Key: record.s3.object.key
          })
          .then(function(objectData) {
            if (!objectData.Metadata || objectData.Metadata["synched"] != "true") {
              var configKeys = Object.keys(config).filter(function(key) {
                var s3Location = config[key].s3Location;
                if (s3Location) {
                  var configS3Path = exports.getFilePathArray(s3Location);
                  return configS3Path.join('/') == newObjectS3Path.slice(0, configS3Path.length).join('/');
                }
              });
              if (configKeys.length == 0) console.warn("No configured SFTP destination for " + fullS3Path);
              return Promise.map(
                configKeys,
                function(configKey) {
                  var streamConfig = config[configKey];
                  var configS3Path = exports.getFilePathArray(streamConfig.s3Location);
                  var sftpDirPath = exports.getFilePathArray(streamConfig.sftpLocation);
                  return exports.getSftpConfig(streamConfig)
                  .then(function(sftpConfig) {
                    return sftpHelper.withSftpClient(sftpConfig, function(sftp) {
                      var sftpFileName = sftpDirPath.concat(newObjectS3Path.slice(configS3Path.length)).join('/');
                      console.info("Writing " + sftpFileName + "...");
                      return sftpHelper.writeFile(
                        sftp,
                        sftpFileName,
                        objectData.Body
                      )
                      .then(function() {
                        console.info("...done");
                        console.info("[" + configKey + "]: Moved 1 files from S3 to SFTP");
                        return sftpFileName;
                      });
                    });
                  });
                }
              )
              .then(function(sftpFiles) {
                var metadata = objectData.Metadata || {};
                metadata["synched"] = "true";
                return awsS3.copyObjectAsync({
                  Bucket: record.s3.bucket.name,
                  Key: record.s3.object.key,
                  CopySource: record.s3.bucket.name + "/" + record.s3.object.key,
                  Metadata: metadata,
                  MetadataDirective: 'REPLACE'
                });
              });
            }
          });
        }
      );
    });
  });
}

exports.newS3Object = function(event, context) {
  return internalNewS3Object(event, context)
  .then(function(result) {
    context.succeed(flatten(result));
  })
  .catch(function(err) {
    console.info("Writing failed message to queue for later processing.");
    return sqs.getQueueUrlAsync({
      QueueName: context.functionName
    })
    .then(function(queueData) {
      return sqs.sendMessageAsync({
        MessageBody: JSON.stringify(event),
        QueueUrl: queueData.QueueUrl
      });
    })
    .then(function(sqsData) {
      context.succeed(sqsData);
    })
    .catch(function(err) {
      console.error(err.stack || err);
      context.fail(err);
      throw err;
    });
  });
}

exports.getFilePathArray = function(filePath) {
  return (filePath || '').split('/').filter(function(s) { return s ? true : false });
}

exports.getSftpConfig = function(config) {
  return Promise.try(function() {
    if (!config.sftpConfig) throw new Error("SFTP config not found");
    if (config.sftpConfig.s3PrivateKey) {
      var bucketDelimiterLocation = config.sftpConfig.s3PrivateKey.indexOf("/");
      return s3.getObjectAsync({
        Bucket: config.sftpConfig.s3PrivateKey.substr(0, bucketDelimiterLocation),
        Key: config.sftpConfig.s3PrivateKey.substr(bucketDelimiterLocation + 1)
      })
      .then(function(objectData) {
        config.sftpConfig["privateKey"] = objectData.Body.toString();
        delete config.sftpConfig.s3PrivateKey;
        return config.sftpConfig;
      });
    } else return config.sftpConfig;
  })
}

exports.scheduledEventResourceToStreamNames = function(resource) {
  return resource.substr(resource.toLowerCase().indexOf("rule/") + 5).split(".");
}

exports.syncSftpDir = function(sftp, sftpDir, s3Location, fileRetentionDays, topDir, isInDoneDir) {
  topDir = topDir || sftpDir;
  fileRetentionDays = fileRetentionDays || 14; // Default to retaining files for 14 days.
  return sftp.readdirAsync(sftpDir)
  .then(function(dirList) {
    return Promise.mapSeries(
      dirList,
      function(fileInfo) {
        return Promise.try(function() {
          if (fileInfo.longname[0] == 'd') {
            return exports.syncSftpDir(sftp, sftpDir + '/' + fileInfo.filename, s3Location, fileRetentionDays, topDir, isInDoneDir || fileInfo.filename == sftpHelper.DoneDir);
          } else if (isInDoneDir) {
            // Purge files from the .done folder based on the stream config
            var fileDate = new Date(fileInfo.attrs.mtime * 1000),
                purgeDate = new Date();
            purgeDate.setDate(purgeDate.getDate() - fileRetentionDays);
            if (fileDate < purgeDate) {
              return sftp.unlinkAsync(sftpDir + '/' + fileInfo.filename);
            }
          } else {
            if (fileInfo.attrs.size <= 0) {
              return false;
            }
            return sftpHelper.processFile(sftp, sftpDir, fileInfo.filename, function(body) {
              var s3Path = exports.getFilePathArray(s3Location),
                  sftpPath = exports.getFilePathArray(sftpDir),
                  topDirPath = exports.getFilePathArray(topDir);
              var s3Bucket = s3Path.shift();
              for (var i = 0; i < topDirPath.length; i++) sftpPath.shift(); // Remove the origin path from the destination directory
              var destDir = s3Path.concat(sftpPath).join('/');
              if (destDir.length > 0) destDir += '/';
              console.info("Writing " + s3Bucket + "/" + destDir + fileInfo.filename + "...");
              return s3.putObjectAsync({
                Bucket: s3Bucket,
                Key: destDir + fileInfo.filename,
                Body: body,
                Metadata: {
                  "synched": "true"
                }
              })
              .then(function(data) {
                console.info("...done");
                return data;
              });
            });
          }
        });
      }
    );
  })
}

function flatten(arr) {
  return arr.reduce(function(a, b) {
    if (Array.isArray(b)) {
      return a.concat(flatten(b));
    } else if (b) {
      a.push(b);
      return a;
    } else {
      return a;
    }
  }, []);
}
