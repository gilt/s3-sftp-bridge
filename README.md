# s3-sftp-bridge
[![Build Status](https://travis-ci.org/gilt/s3-sftp-bridge.svg?branch=master)](https://travis-ci.org/gilt/s3-sftp-bridge)

An AWS Lambda function that syncs files between Amazon S3 and external FTP servers. For S3 => SFTP, it
will automatically sync when objects are uploaded to S3. For SFTP => S3, it will poll the SFTP server
at a given interval and copy to S3. It will maintain the origin directory structure when copying to the
destination.

After pulling files from the SFTP server, they will be moved to a '.done' subdirectory in the same directory.
This prevents us from copying the same files over and over again. It also allows easy re-sending (by copying
the file back into the original directory); the consequence is that files in the '.done' subdirectory will
be ignored.


## Quick Deploy
For more details, see the sections below. But here is the recommended list of steps to deploy thie bridge.

1. Create a CloudFormation Stack using one of the [templates](cloud_formation/s3-sftp-bridge-deploy-to-vpc.template).
2. Add the stack's output role to the [KMS key's](#kms--security) list of "Key Users".
3. Add a [config file](#configuration) named after the stack's output function name.
4. Manually setup the [SFTP polling schedule](#sftp--s3).


## Configuration
Config should be stored in a .json file in S3 according to https://github.com/gilt/aws-lambda-config#motivation.
The configuration is a map of streamName to configuration for that stream:

```
{
  "stream1": {
    "s3Location": "your-bucket-name/destination/directory",
    "sftpConfig": {
      "host": "hostname",
      "port": 2222,
      "s3PrivateKey": "another-bucket-name/path/to/private_key",
      "username": "user"
    },
    "sftpLocation": "my-directory"
  },
  "stream2": {
    "s3Location": "your-other-bucket-name/destination/directory",
    "sftpConfig": {
      "host": "hostname",
      "username": "user",
      "password": "pwd"
    },
    "sftpLocation": "my-directory"
  }
}
```

### sftpLocation
The directory (can be nested) on the SFTP side to either a) look for new files to copy to S3 or b) drop into when
copying from S3.

### s3Location
The S3 location where the files should be copied. Can include a subdirectory after the bucket name. Valid formats:
bucket-name
bucket-name/sub-directory
bucket-name/sub/directory

You can also provide an array of S3 locations if you want to sync an origin directory to multiple destination directories.
Ex:
[bucket-name, bucket2-name/subdirectory, bucket3-name/sub/directory]

### sftpConfig
A JSON object that contains any of the connection options listed here: https://www.npmjs.com/package/ssh2#client-methods.

This can also optionally include an "s3PrivateKey" property, which should be a S3 bucket/object-key path that
contains the SSH private key to use in the connection. If used, this should be encrypted and uploaded according
to https://github.com/gilt/node-s3-encryption-client.


## Configuration outside the CloudFormation template
There are a few things that you will need to (optionally) configure outside of what the provided CloudFormation
template sets up for you. These are explicitly excluded from the CF template because they are very dependent on
your own specific requirements and would be difficult to generalize into the template.

### Networking
If you lock down your SFTP server (and you should) by whitelisting client IP addresses, you will need to take a few
extra steps to ensure a consistent outgoing IP address. If you run the Bridge Lambda function outside of a VPC, you
will get a random outgoing IP address assigned by AWS. It may look like they use the same one every time, but there
are no guarantees. To explicitly assign an outgoing IP address, do the following:

1. Create a publicly-facing VPC.
2. Create subnets inside that VPC.
3. Route those subnets through a NAT.
  a. Assign an IP address to the NAT. This will be your outgoing IP address, for use in whitelisting.
  b. Open the SFTP port (22 by default) for both incoming and outgoing on the NAT device.
  c. Consider locking 3b to the specific IP address of your destination SFTP server.
4. Create a security group for this.
5. Edit the Bridge Lambda function's configuration to
  a. Use the VPC from 1.
  b. Use the subnets from 2.
  c. Use the security group from 4.
6. Whitelist the IP address from 3a on your SFTP server.

If you do #1-4 ahead of time, you can use the [s3-sftp-bridge-deploy-to-vpc.template](cloud_formation/s3-sftp-bridge-deploy-to-vpc.template)
to automatically add the Bridge function to the VPC (#5 above).

### KMS / Security
If you're client-side encrypting either the Bridge config or any private keys (see https://github.com/gilt/node-s3-encryption-client),
the Bridge Lambda function will need access to any applicable KMS keys. You can find the Role name in the CF stack outputs. 

### Triggering the sync
Two events are necessary to trigger this bridge to sync between the two systems, as detailed below.

#### S3 => SFTP
Any origin S3 buckets/locations should be set up to trigger the bridge Lambda function on the putObject event, with
all requisite permissions. The included CloudFormation template will set up a fresh S3 bucket given as a stack
property. But any additional S3 buckets + notifications will need to be setup manually.

#### SFTP => S3
The included Lambda function will need to poll the SFTP server using a scheduled event in AWS Lambda. When scheduling the
event (via CloudWatch Events), include in the "name" field a period-delimited (".") list of streamNames that match streamNames
in your config file. There can be multiple streamNames in the same event, and multiple events polling the Bridge function.

The Lambda scheduled event system allows you to schedule the event at whatever interval is appropriate for your setup.
See http://docs.aws.amazon.com/lambda/latest/dg/with-scheduled-events.html for details.

### Alerting

#### File transfer alerts

The CloudFormation template automatically sets up two metrics under a namespace that matches the stack name:

1. s3-to-sftp
2. sftp-to-s3

These can be used to ensure that files are being moved at the rate that is expected for your system; you should set up
an alarm to watch for when the transfer counts fall below a reasonable threshold. These metrics rely on log entries that
look like this:

```
[stream-name]: Moved 1 files from S3 to SFTP
[stream-name]: Moved 1 files from SFTP to S3
```

The metric filters look like this:

```
[timestamp, requestId, streamName, colon, moved = Moved, numFiles, files = files, from, orig = SFTP, to, dest = S3]
```

and

```
[timestamp, requestId, streamName, colon, moved = Moved, numFiles, files = files, from, orig = S3, to, dest = SFTP]
```

Use $numFiles as the metric value, then alert on both the count (to ensure the bridge is running) and sum (to ensure
the bridge is moving files at the rate you expect). You can alert on individual streams by editing the filter above
to only match when streamName matches your stream name.


#### Error alerts
The CloudFormation templates create a few metrics (under the s3-sftp-bridge namespace) that can be used for error
alerting. You will need to manually set up the alarms to meet your monitoring expectations. The metrics are:

1. connection-timeouts - Triggered when the attempted connection to the sftp server times out. This can probably have
   a higher alarm threshold, so avoid alerting on transient network slowness.
2. lambda-function-timeouts - Triggered when the Lambda function runs longer than the configured (5 minute) timeout.
   The alarm tolerance should be 0, since it's likely that future runs could also timeout.
3. errors - Triggered for general errors. This should be monitored for unexpected errors.


## Deployment (contributors)
After making changes, please do the following:

1. Upload this zipped repo to the com.gilt.public.backoffice/lambda_functions bucket. To produce the .zip file:

   ```
     rm -rf node_modules
     npm install --production
     zip -r s3-sftp-bridge.zip . -x *.git* -x *s3-sftp-bridge.zip* -x cloud_formation/\* -x *aws-sdk*
   ```

   Unfortunately we can't use the Github .zip file directly, because it zips the code into a subdirectory named after
   the repo; AWS Lambda then can't find the .js file containing the helper functions because it is not on the top-level.

2. Upload the edited templates from the cloud_formation directort to com.gilt.public.backoffice/cloudformation_templates.


## License
Copyright 2016 Gilt Groupe, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
