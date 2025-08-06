'use strict';

var sinon  = require('sinon'),
    {DynamoDB}  = require('@aws-sdk/client-dynamodb'),
  { DynamoDBDocument }    = require('@aws-sdk/lib-dynamodb/'),
    Table  = require('../lib/table'),
    _      = require('lodash'),
    bunyan = require('bunyan');

exports.mockDynamoDB = function () {
  var opts = { endpoint : 'http://localhost:8000', region: 'us-west-2', apiVersion: '2012-08-10' };
  var db = new DynamoDB(opts);

  db.scan          = sinon.stub();
  db.putItem       = sinon.stub();
  db.deleteItem    = sinon.stub();
  db.query         = sinon.stub();
  db.getItem       = sinon.stub();
  db.updateItem    = sinon.stub();
  db.createTable   = sinon.stub();
  db.describeTable = sinon.stub();
  db.updateTable   = sinon.stub();
  db.deleteTable   = sinon.stub();
  db.batchGetItem  = sinon.stub();
  db.batchWriteItem = sinon.stub();

  return db;
};

exports.realDynamoDB = function () {
  var opts = { endpoint : 'http://localhost:8000', region: 'us-west-2', apiVersion: '2012-08-10' };
  return new DynamoDB(opts);
};

exports.mockDocClient = function () {
  var client = DynamoDBDocument.from(exports.mockDynamoDB(), {
    marshallOptions: {
      removeUndefinedValues: true
    }
  });

  var operations= [
    'batchGet',
    'batchWrite',
    'put',
    'get',
    'delete',
    'update',
    'scan',
    'query'
  ];

  _.each(operations, function (op) {
    client[op] = sinon.stub();
  });

  return client;
};

exports.mockSerializer = function () {
  var serializer = {
    buildKey               : sinon.stub(),
    deserializeItem        : sinon.stub(),
    serializeItem          : sinon.stub(),
    serializeItemForUpdate : sinon.stub()
  };

  return serializer;
};

exports.mockTable = function () {
  return sinon.createStubInstance(Table);
};

exports.fakeUUID = function () {
  var uuid = {
    v1: sinon.stub(),
    v4: sinon.stub()
  };

  return uuid;
};

exports.randomName = function (prefix) {
  return prefix + '_' + Date.now() + '.' + _.random(1000);
};

exports.testLogger = function() {
  return bunyan.createLogger({
    name: 'dynamo-tests',
    serializers : {err: bunyan.stdSerializers.err},
    level : bunyan.FATAL
  });
};

exports.mockClients = function () {
  var dynamodb = exports.mockDynamoDB();
  var docClient = exports.mockDocClient();
  
  return {
    docClient: docClient,
    dynamodb: dynamodb
  };
};
