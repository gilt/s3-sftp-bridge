var Promise = require('bluebird'),
    SshClient = require('ssh2').Client;
exports.DoneDir = '.done';

// Returns a Disposer
exports.getSshClient = function(config) {
  var conn = new SshClient();
  var promise = new Promise(function(resolve, reject) {
    conn
    .on('ready', function() {
      resolve(conn);
    })
    .on('error', function(e) {
      reject(e);
     })
    .connect(config);
  });
  return promise.disposer(function(conn, promise) {
    conn.end();
  });
}

/*
sftp: SFTP client from ssh2, assumed to already be promisified.
dir: The directory where the file lives.
fileName: The file to be written, should not include any of the directory path. Can
  optionally be the file's info (from readdir), for efficiency.
process: A function with one argument, the body of the file as a Buffer.
*/
exports.processFile = function(sftp, dir, fileName, process) {
  return Promise.try(function() {
    if (fileName.filename) {
      return fileName
    } else {
      return sftp.readdirAsync(dir)
      .then(function(dirList) {
        return dirList.find(function(item) { return item.filename == fileName})
      });
    }
  })
  .then(function(fileInfo) {
    return sftp.openAsync(dir + '/' + fileInfo.filename, 'r')
    .then(function(handle) {
      var result = new Buffer(fileInfo.attrs.size);
      return sftp.readAsync(handle, result, 0, fileInfo.attrs.size, 0)
      .then(function(data) {
        return process(result);
      })
      .then(function(data) {
        return sftp.closeAsync(handle)
        .then(function() {
          return data;
        });
      })
      .then(function(data) {
        return sftp.readdirAsync(dir)
        .then(function(dirList) {
          if (!dirList.find(function(item) { return item.filename == exports.DoneDir})) return sftp.mkdirAsync(dir + '/' + exports.DoneDir);
        })
        .then(function() {
          console.log("Moving " + dir + "/" + fileInfo.filename + " to .done directory...");
          return sftp.renameAsync(dir + '/' + fileInfo.filename, dir + '/' + exports.DoneDir + '/' + getCurrentTimestamp() + '-' + fileInfo.filename);
        })
        .then(function() {
          return data;
        })
      });
    });
  });
}

// Don't attempt to use the sftp object outside of the 'process' function (i.e.
// in a .then hung off the resultant Promise) - the connection will be closed.
exports.withSftpClient = function(config, process) {
  return Promise.using(exports.getSshClient(config), function(conn) {
    return Promise.promisify(conn.sftp, {context: conn})()
    .then(function(sftp) {
      return process(Promise.promisifyAll(sftp));
    });
  });
}

/*
sftp: SFTP client from ssh2, assumed to already be promisified.
fileName: The full path of the file to be written
body: A string containing the body to write to the file. UTF-8.
*/
exports.writeFile = function(sftp, fileName, body) {
  return sftp.openAsync(fileName, 'w')
  .then(function(handle) {
    return sftp.writeAsync(handle, new Buffer(body), 0, body.length, 0)
    .then(function() {
      return sftp.closeAsync(handle);
    });
  });
}

function getCurrentTimestamp() {
  return (new Date()).toISOString().slice(0,19).replace(/-/g,"").replace(/:/g,"").replace(/T/g,"");
}
