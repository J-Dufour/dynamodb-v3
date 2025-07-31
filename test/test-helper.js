'use strict';

var sinon  = require('sinon'),
    Table  = require('../lib/table'),
    _      = require('lodash'),
    bunyan = require('bunyan');

// Import v3 AWS SDK modules
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');

exports.mockDynamoDBClient = function () {
  var opts = { 
    endpoint: 'http://localhost:8000', 
    region: 'us-west-2'
  };
  var client = new DynamoDBClient(opts);

  // Stub the send method with default promise resolution
  client.send = sinon.stub().resolves({});
  
  return client;
};

exports.realDynamoDBClient = function () {
  var opts = { 
    endpoint: 'http://localhost:8000', 
    region: 'us-west-2'
  };
  return new DynamoDBClient(opts);
};

exports.mockDocumentClient = function () {
  var baseClient = exports.mockDynamoDBClient();
  var docClient = DynamoDBDocumentClient.from(baseClient);
  
  // Stub the send method for DocumentClient with default promise resolution
  docClient.send = sinon.stub().resolves({});
  
  // For backward compatibility, add service property that points to base client
  docClient.service = baseClient;
  
  return docClient;
};

exports.realDocumentClient = function () {
  var baseClient = exports.realDynamoDBClient();
  return DynamoDBDocumentClient.from(baseClient);
};

// Helper function for Command matching with specific parameters
exports.matchCommand = function(CommandClassOrName, expectedInput) {
  return sinon.match(function(cmd) {
    var CommandClass;
    
    // If string name is passed, convert to actual Command class
    if (typeof CommandClassOrName === 'string') {
      CommandClass = exports.getCommandClass(CommandClassOrName);
    } else {
      CommandClass = CommandClassOrName;
    }
    
    if (!CommandClass || !(cmd instanceof CommandClass)) {
      return false;
    }
    if (!expectedInput) {
      return true; // Just match the Command type
    }
    return _.isEqual(cmd.input, expectedInput);
  });
};

// Helper function for partial Command matching (useful for complex objects)
exports.matchCommandPartial = function(CommandClass, partialInput) {
  return sinon.match(function(cmd) {
    if (!(cmd instanceof CommandClass)) {
      return false;
    }
    if (!partialInput) {
      return true;
    }
    return _.isMatch(cmd.input, partialInput);
  });
};

// Helper function to get Command class from method name (for migration ease)
exports.getCommandClass = function(operation) {
  const { 
    GetCommand, 
    PutCommand, 
    UpdateCommand, 
    DeleteCommand, 
    QueryCommand, 
    ScanCommand, 
    BatchGetCommand, 
    BatchWriteCommand 
  } = require('@aws-sdk/lib-dynamodb');
  
  const { 
    CreateTableCommand,
    DescribeTableCommand,
    UpdateTableCommand,
    DeleteTableCommand
  } = require('@aws-sdk/client-dynamodb');

  const commandMap = {
    'get': GetCommand,
    'put': PutCommand,
    'update': UpdateCommand,
    'delete': DeleteCommand,
    'query': QueryCommand,
    'scan': ScanCommand,
    'batchGet': BatchGetCommand,
    'batchWrite': BatchWriteCommand,
    'createTable': CreateTableCommand,
    'describeTable': DescribeTableCommand,
    'updateTable': UpdateTableCommand,
    'deleteTable': DeleteTableCommand,
    // Also support Command class names directly
    'GetCommand': GetCommand,
    'PutCommand': PutCommand,
    'UpdateCommand': UpdateCommand,
    'DeleteCommand': DeleteCommand,
    'QueryCommand': QueryCommand,
    'ScanCommand': ScanCommand,
    'BatchGetCommand': BatchGetCommand,
    'BatchWriteCommand': BatchWriteCommand,
    'CreateTableCommand': CreateTableCommand,
    'DescribeTableCommand': DescribeTableCommand,
    'UpdateTableCommand': UpdateTableCommand,
    'DeleteTableCommand': DeleteTableCommand
  };

  return commandMap[operation];
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

// Backward compatibility aliases for older test files
exports.mockDocClient = exports.mockDocumentClient;
exports.mockDynamoDB = exports.mockDynamoDBClient;
exports.realDynamoDB = exports.realDynamoDBClient;
