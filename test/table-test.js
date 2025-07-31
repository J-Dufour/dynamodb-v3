'use strict';

var helper = require('./test-helper'),
    _      = require('lodash'),
    Joi    = require('joi'),
    Table  = require('../lib/table'),
    Schema = require('../lib/schema'),
    Query  = require('../lib//query'),
    Scan   = require('../lib//scan'),
    Item   = require('../lib/item'),
    { CreateTableCommand } = require('@aws-sdk/client-dynamodb'),
    realSerializer = require('../lib/serializer'),
    chai   = require('chai'),
    expect = chai.expect,
    assert = chai.assert,
    sinon  = require('sinon');

chai.should();

describe('table', function () {
  var table,
      serializer,
      docClient,
      dynamodb,
      logger;

  beforeEach(function () {
    serializer = helper.mockSerializer(),
    docClient = helper.mockDocumentClient();
    dynamodb = helper.mockDynamoDBClient();
    logger = helper.testLogger();
  });

  describe('#get', function () {

    it('should get item by hash key', function (done) {
      var config = {
        hashKey: 'email'
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : { email : 'test@test.com'}
      };

      var resp = {
        Item : {email: 'test@test.com', name: 'test dude'}
      };

      docClient.send.withArgs(helper.matchCommand('GetCommand', request)).resolves(resp);

      table.get('test@test.com', function (err, account) {
        account.should.be.instanceof(Item);
        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('test dude');

        done();
      });
    });

    it('should get item by hash key with a promise', function (done) {
      var config = {
        hashKey: 'email'
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : { email : 'test@test.com'}
      };

      var resp = {
        Item : {email: 'test@test.com', name: 'test dude'}
      };

      docClient.send.withArgs(helper.matchCommand('GetCommand', request)).resolves(resp);

      table.get('test@test.com')
        .then(function (account) {
          account.should.be.instanceof(Item);
          account.get('email').should.equal('test@test.com');
          account.get('name').should.equal('test dude');

          done();
        })
        .catch(function () {
          assert(false, 'catch should not be called');
        });
    });

    it('should get item by hash and range key', function (done) {
      var config = {
        hashKey: 'name',
        rangeKey: 'email'
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          name  : 'Tim Tester',
          email : 'test@test.com'
        }
      };

      var resp = {
        Item : {email: 'test@test.com', name: 'Tim Tester'}
      };

      docClient.send.withArgs(helper.matchCommand('GetCommand', request)).resolves(resp);

      table.get('Tim Tester', 'test@test.com', function (err, account) {
        account.should.be.instanceof(Item);
        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Tester');

        done();
      });
    });

    it('should get item by hash key and options', function (done) {
      var config = {
        hashKey: 'email',
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : { email : 'test@test.com' },
        ConsistentRead: true
      };

      var resp = {
        Item : {email: 'test@test.com', name: 'test dude'}
      };

      docClient.send.withArgs(helper.matchCommand('GetCommand', request)).resolves(resp);

      table.get('test@test.com', {ConsistentRead: true}, function (err, account) {
        account.should.be.instanceof(Item);
        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('test dude');

        done();
      });
    });

    it('should get item by hashkey, range key and options', function (done) {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          name  : 'Tim Tester',
          email : 'test@test.com'
        },
        ConsistentRead: true
      };

      var resp = {
        Item : {email: 'test@test.com', name: 'Tim Tester'}
      };

      docClient.send.withArgs(helper.matchCommand('GetCommand', request)).resolves(resp);

      table.get('Tim Tester', 'test@test.com', {ConsistentRead: true}, function (err, account) {
        account.should.be.instanceof(Item);
        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Tester');

        done();
      });
    });

    it('should get item from dynamic table by hash key', function (done) {

      var config = {
        hashKey: 'email',
        tableName : function () {
          return 'accounts_2014';
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts_2014',
        Key : { email : 'test@test.com' }
      };

      var resp = {
        Item : {email: 'test@test.com', name: 'test dude'}
      };

      docClient.send.withArgs(helper.matchCommand('GetCommand', request)).resolves(resp);

      table.get('test@test.com', function (err, account) {
        account.should.be.instanceof(Item);
        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('test dude');

        done();
      });
    });

    it('should return error', function (done) {
      var config = {
        hashKey: 'email',
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      docClient.send.withArgs(helper.matchCommand('GetCommand')).rejects(new Error('Fail'));

      table.get('test@test.com', function (err, account) {
        expect(err).to.exist;
        expect(account).to.not.exist;
        done();
      });
    });

  });

  describe('#create', function () {

    it('should create valid item', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string(),
          age   : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          name  : 'Tim Test',
          age   : 23
        }
      };

      docClient.send.withArgs(helper.matchCommand('PutCommand', request)).resolves({});

      table.create(request.Item, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');

        done();
      });
    });

    it('should call apply defaults', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string().default('Foo'),
          age   : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          name  : 'Foo',
          age   : 23
        }
      };

      docClient.send.withArgs(helper.matchCommand('PutCommand', request)).resolves({});

      table.create({email : 'test@test.com', age: 23}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Foo');

        done();
      });
    });

    it('should omit null values', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string(),
          age   : Joi.number().allow(null),
          favoriteNumbers : Schema.types.numberSet().allow(null),
          luckyNumbers : Schema.types.numberSet().allow(null)
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var numberSet = sinon.match(function (value) {
        // In v3, Sets are native JavaScript Sets
        var expectedSet = new Set([1, 2, 3]);
        
        return value instanceof Set && 
               value.size === expectedSet.size &&
               [...value].every(val => expectedSet.has(val));
      }, 'NumberSet');

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          name  : 'Tim Test',
          luckyNumbers: numberSet
        }
      };

      docClient.send.withArgs(helper.matchCommand('PutCommand', request)).resolves({});

      var item = {email : 'test@test.com', name : 'Tim Test', age : null, favoriteNumbers: [], luckyNumbers: [1, 2, 3]};
      table.create(item, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');
        account.get('luckyNumbers').should.eql([1, 2, 3]);

        expect(account.toJSON()).to.have.keys(['email', 'name', 'luckyNumbers']);

        done();
      });
    });

    it('should omit empty values', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string().allow(''),
          age   : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          age   : 2
        }
      };

      docClient.send.withArgs(helper.matchCommand('PutCommand', request)).resolves({});

      table.create({email: 'test@test.com', name: '', age: 2}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('age').should.equal(2);

        done();
      });
    });

    it('should create item with createdAt timestamp', function (done) {
      var config = {
        hashKey: 'email',
        timestamps : true,
        schema : {
          email : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          createdAt : sinon.match.string
        }
      };

      docClient.send.withArgs(helper.matchCommand('PutCommand', request)).resolves({});

      table.create({email : 'test@test.com'}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('createdAt').should.exist;
        done();
      });
    });

    it('should create item with custom createdAt attribute name', function (done) {
      var config = {
        hashKey: 'email',
        timestamps : true,
        createdAt : 'created',
        schema : {
          email : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          created : sinon.match.string
        }
      };

      docClient.send.withArgs(helper.matchCommand('PutCommand', request)).resolves({});

      table.create({email : 'test@test.com'}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('created').should.exist;
        done();
      });
    });


    it('should create item without createdAt param', function (done) {
      var config = {
        hashKey: 'email',
        timestamps : true,
        createdAt : false,
        schema : {
          email : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com'
        }
      };

      docClient.send.withArgs(helper.matchCommand('PutCommand', request)).resolves({});

      table.create({email : 'test@test.com'}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        expect(account.get('createdAt')).to.not.exist;
        done();
      });
    });

    it('should create item with expected option', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name : Joi.string()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
        },
        ExpressionAttributeNames: { '#name': 'name' },
        ExpressionAttributeValues: { ':name': 'Foo Bar' },
        ConditionExpression : '(#name = :name)'
      };

      docClient.send.withArgs(helper.matchCommand('PutCommand', request)).resolves({});

      table.create({email : 'test@test.com'}, {expected: {name: 'Foo Bar'}}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        done();
      });
    });

    it('should create item with no callback', function (done) {
      var config = {
        hashKey: 'email',
        timestamps : true,
        schema : {
          email : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          createdAt : sinon.match.string
        }
      };

      docClient.send.withArgs(helper.matchCommand('PutCommand', request)).resolves({});

      table.create({email : 'test@test.com'});

      // Give it a moment for the async call to complete, then check
      setTimeout(function() {
        sinon.assert.calledWith(docClient.send, helper.matchCommand('PutCommand'));
        done();
      }, 10);
    });

    it('should return validation error', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name : Joi.string()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      table.create({email : 'test@test.com', name : [1, 2, 3]}, function (err, account) {
        expect(err).to.exist;
        expect(err).to.match(/ValidationError/);
        expect(account).to.not.exist;

        sinon.assert.notCalled(docClient.send);
        done();
      });
    });

    it('should create item with condition expression on hashkey when overwrite flag is false', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name : Joi.string()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          name : 'Bob Tester'
        },
        ExpressionAttributeNames: { '#email': 'email' },
        ExpressionAttributeValues: { ':email': 'test@test.com' },
        ConditionExpression : '(#email <> :email)'
      };

      docClient.send.withArgs(helper.matchCommand('PutCommand', request)).resolves({});

      table.create({email : 'test@test.com', name : 'Bob Tester'}, {overwrite: false}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        done();
      });
    });

    it('should create item with condition expression on hash and range key when overwrite flag is false', function (done) {
      var config = {
        hashKey: 'email',
        rangeKey: 'name',
        schema : {
          email : Joi.string(),
          name : Joi.string()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          name : 'Bob Tester'
        },
        ExpressionAttributeNames: { '#email': 'email', '#name' : 'name' },
        ExpressionAttributeValues: { ':email': 'test@test.com', ':name' : 'Bob Tester' },
        ConditionExpression : '(#email <> :email) AND (#name <> :name)'
      };

      docClient.send.withArgs(helper.matchCommand('PutCommand', request)).resolves({});

      table.create({email : 'test@test.com', name : 'Bob Tester'}, {overwrite: false}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        done();
      });
    });

    it('should create item without condition expression when overwrite flag is true', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name : Joi.string()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Item : {
          email : 'test@test.com',
          name : 'Bob Tester'
        }
      };

      docClient.send.withArgs(helper.matchCommand('PutCommand', request)).resolves({});

      table.create({email : 'test@test.com', name : 'Bob Tester'}, {overwrite: true}, function (err, account) {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        done();
      });
    });

  });

  describe('#update', function () {

    it('should update valid item', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string(),
          age   : Joi.number(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : { email : 'test@test.com'},
        ReturnValues: 'ALL_NEW',
        UpdateExpression : 'SET #name = :name, #age = :age',
        ExpressionAttributeValues : { ':name' : 'Tim Test', ':age' : 23},
        ExpressionAttributeNames : { '#name' : 'name', '#age' : 'age'}
      };

      var returnedAttributes = {
        email  : 'test@test.com',
        name   : 'Tim Test',
        age    : 23,
        scores : [97, 86]
      };

      docClient.send.withArgs(helper.matchCommand('UpdateCommand', request)).resolves({Attributes: returnedAttributes});

      var item = {email : 'test@test.com', name : 'Tim Test', age : 23};
      table.update(item, function (err, account) {
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');
        account.get('age').should.equal(23);
        account.get('scores').should.eql([97, 86]);

        done();
      });
    });

    it('should update with passed in options', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string(),
          age   : Joi.number(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : { email : 'test@test.com' },
        ReturnValues: 'ALL_OLD',
        UpdateExpression : 'SET #name = :name, #age = :age',
        ExpressionAttributeValues : { ':name_2' : 'Foo Bar', ':name' : 'Tim Test', ':age' : 23 },
        ExpressionAttributeNames : { '#name' : 'name', '#age' : 'age'},
        ConditionExpression : '(#name = :name_2)'
      };

      var returnedAttributes = {
        email  : 'test@test.com',
        name   : 'Tim Test',
        age    : 23,
        scores : [97, 86]
      };

      var item = {email : 'test@test.com', name : 'Tim Test', age : 23};

      docClient.send.withArgs(helper.matchCommand('UpdateCommand', request)).resolves({Attributes: returnedAttributes});

      table.update(item, {ReturnValues: 'ALL_OLD', expected: {name: 'Foo Bar'}}, function (err, account) {
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');
        account.get('age').should.equal(23);
        account.get('scores').should.eql([97, 86]);

        done();
      });
    });

    it('should update merge update expressions when passed in as options', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string(),
          age   : Joi.number(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : { email : 'test@test.com' },
        ReturnValues: 'ALL_NEW',
        UpdateExpression : 'SET #name = :name, #age = :age ADD #color :c',
        ExpressionAttributeValues : { ':name' : 'Tim Test', ':age' : 23, ':c' : 'red'},
        ExpressionAttributeNames : { '#name' : 'name', '#age' : 'age', '#color' : 'color'}
      };

      var returnedAttributes = {
        email  : 'test@test.com',
        name   : 'Tim Test',
        age    : 23,
        scores : [97, 86],
        color  : 'red'
      };

      var item = {email : 'test@test.com', name : 'Tim Test', age : 23};

      docClient.send.withArgs(helper.matchCommand('UpdateCommand', request)).resolves({Attributes: returnedAttributes});

      var options = {
        UpdateExpression : 'ADD #color :c',
        ExpressionAttributeValues : { ':c' : 'red'},
        ExpressionAttributeNames : { '#color' : 'color'}
      };

      table.update(item, options, function (err, account) {
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');
        account.get('age').should.equal(23);
        account.get('scores').should.eql([97, 86]);
        account.get('color').should.eql('red');

        done();
      });
    });

    it('should update valid item with a promise', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string(),
          age   : Joi.number(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : { email : 'test@test.com'},
        ReturnValues: 'ALL_NEW',
        UpdateExpression : 'SET #name = :name, #age = :age',
        ExpressionAttributeValues : { ':name' : 'Tim Test', ':age' : 23},
        ExpressionAttributeNames : { '#name' : 'name', '#age' : 'age'}
      };

      var returnedAttributes = {
        email  : 'test@test.com',
        name   : 'Tim Test',
        age    : 23,
        scores : [97, 86]
      };

      docClient.send.withArgs(helper.matchCommand('UpdateCommand', request)).resolves({Attributes: returnedAttributes});

      var item = {email : 'test@test.com', name : 'Tim Test', age : 23};
      table.update(item)
        .then(function () {
          docClient.send.calledWith(helper.matchCommand('UpdateCommand', request));

          done();
        });
    });

    it('should return error', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string(),
          age   : Joi.number(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      docClient.send.withArgs(helper.matchCommand('UpdateCommand')).rejects(new Error('Fail'));

      var item = {email : 'test@test.com', name : 'Tim Test', age : 23};

      table.update(item, function (err, account) {
        expect(err).to.exist;
        expect(account).to.not.exist;
        done();
      });
    });

    it('should reject an error with a promise', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email : Joi.string(),
          name  : Joi.string(),
          age   : Joi.number(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      docClient.send.withArgs(helper.matchCommand('UpdateCommand')).rejects(new Error('Fail'));

      var item = {email : 'test@test.com', name : 'Tim Test', age : 23};

      table.update(item)
        .then(function () {
          assert(false, 'then should not be called');
          done();
        })
        .catch(function (err) {
          expect(err).to.exist;
          done();
        });
    });

  });

  describe('#query', function () {

    it('should return query object', function () {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

      table.query('Bob').should.be.instanceof(Query);
    });
  });

  describe('#scan', function () {

    it('should return scan object', function () {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

      table.scan().should.be.instanceof(Scan);
    });
  });

  describe('#destroy', function () {

    it('should destroy valid item', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          email : 'test@test.com'
        }
      };

      docClient.send.withArgs(helper.matchCommand('DeleteCommand')).resolves({});

      serializer.buildKey.returns(request.Key);

      table.destroy('test@test.com', function () {
        serializer.buildKey.calledWith('test@test.com', null, s).should.be.true;
        docClient.send.calledWith(helper.matchCommand('DeleteCommand', request)).should.be.true;

        done();
      });
    });

    it('should take optional params', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          email : {S : 'test@test.com'}
        },
        ReturnValues : 'ALL_OLD'
      };

      docClient.send.withArgs(helper.matchCommand('DeleteCommand')).resolves({});

      serializer.buildKey.returns(request.Key);

      table.destroy('test@test.com', {ReturnValues: 'ALL_OLD'}, function () {
        serializer.buildKey.calledWith('test@test.com', null, s).should.be.true;
        docClient.send.calledWith(helper.matchCommand('DeleteCommand', request)).should.be.true;

        done();
      });
    });

    it('should parse and return attributes', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : { email : 'test@test.com' },
        ReturnValues : 'ALL_OLD'
      };

      var returnedAttributes = {
        email : 'test@test.com',
        name  : 'Foo Bar'
      };

      docClient.send.withArgs(helper.matchCommand('DeleteCommand')).resolves({Attributes: returnedAttributes});

      serializer.buildKey.returns(request.Key);
      serializer.deserializeItem.withArgs(returnedAttributes).returns(
        {email : 'test@test.com', name: 'Foo Bar'
      });

      table.destroy('test@test.com', {ReturnValues: 'ALL_OLD'}, function (err, item) {
        serializer.buildKey.calledWith('test@test.com', null, s).should.be.true;
        docClient.send.calledWith(helper.matchCommand('DeleteCommand', request)).should.be.true;

        item.get('name').should.equal('Foo Bar');

        done();
      });
    });

    it('should accept hash and range key', function (done) {
      var config = {
        hashKey: 'email',
        rangeKey: 'name',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          email : 'test@test.com',
          name : 'Foo Bar'
        }
      };

      var returnedAttributes = {
        email : 'test@test.com',
        name  : 'Foo Bar'
      };

      docClient.send.withArgs(helper.matchCommand('DeleteCommand')).resolves({Attributes: returnedAttributes});

      serializer.buildKey.returns(request.Key);
      serializer.deserializeItem.withArgs(returnedAttributes).returns(
        {email : 'test@test.com', name: 'Foo Bar'
      });

      table.destroy('test@test.com', 'Foo Bar', function (err, item) {
        serializer.buildKey.calledWith('test@test.com', 'Foo Bar', s).should.be.true;
        docClient.send.calledWith(helper.matchCommand('DeleteCommand', request)).should.be.true;

        item.get('name').should.equal('Foo Bar');

        done();
      });
    });

    it('should accept hashkey rangekey and options', function (done) {
      var config = {
        hashKey: 'email',
        rangeKey: 'name',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          email : 'test@test.com',
          name  : 'Foo Bar'
        },
        ReturnValues : 'ALL_OLD'
      };

      var returnedAttributes = {
        email : 'test@test.com',
        name  : 'Foo Bar'
      };

      docClient.send.withArgs(helper.matchCommand('DeleteCommand')).resolves({Attributes: returnedAttributes});

      serializer.buildKey.returns(request.Key);
      serializer.deserializeItem.withArgs(returnedAttributes).returns(
        {email : 'test@test.com', name: 'Foo Bar'
      });

      table.destroy('test@test.com', 'Foo Bar', {ReturnValues: 'ALL_OLD'}, function (err, item) {
        serializer.buildKey.calledWith('test@test.com', 'Foo Bar', s).should.be.true;
        docClient.send.calledWith(helper.matchCommand('DeleteCommand', request)).should.be.true;

        item.get('name').should.equal('Foo Bar');

        done();
      });
    });

    it('should serialize expected option', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          email : 'test@test.com'
        },
        ExpressionAttributeNames: { '#name': 'name' },
        ExpressionAttributeValues: { ':name': 'Foo Bar' },
        ConditionExpression : '(#name = :name)'
      };

      docClient.send.withArgs(helper.matchCommand('DeleteCommand')).resolves({});

      serializer.serializeItem.withArgs(s, {name: 'Foo Bar'}, {expected : true}).returns(request.Expected);
      serializer.buildKey.returns(request.Key);

      table.destroy('test@test.com', {expected: {name : 'Foo Bar'}}, function () {
        serializer.buildKey.calledWith('test@test.com', null, s).should.be.true;
        docClient.send.calledWith(helper.matchCommand('DeleteCommand', request)).should.be.true;

        done();
      });
    });

    it('should call delete item with a promise', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          email : 'test@test.com'
        }
      };

      docClient.send.withArgs(helper.matchCommand('DeleteCommand')).resolves({});
      table.destroy('test@test.com')
        .then(function () {
          docClient.send.calledWith(helper.matchCommand('DeleteCommand', request));

          done();
        });
    });

    it('should call delete item with hash key, options and no callback', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        Key : {
          email : 'test@test.com'
        },
        Expected : {
          name : {'Value' : 'Foo Bar'}
        }
      };

      docClient.send.withArgs(helper.matchCommand('DeleteCommand')).resolves({});
      table.destroy('test@test.com', {expected: {name : 'Foo Bar'}});

      docClient.send.calledWith(helper.matchCommand('DeleteCommand', request));

      return done();
    });
  });

  describe('#createTable', function () {
    it('should create table with hash key', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        AttributeDefinitions : [
          { AttributeName: 'email', AttributeType: 'S' }
        ],
        KeySchema: [
          { AttributeName: 'email', KeyType: 'HASH' }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      };

      dynamodb.send = sinon.stub()
        .withArgs(helper.matchCommand(CreateTableCommand, request))
        .resolves({});

      table.createTable({readCapacity : 5, writeCapacity: 5}, function (err) {
        expect(err).to.be.null;
        dynamodb.send.calledWith(helper.matchCommand(CreateTableCommand, request)).should.be.true;
        done();
      });

    });

    it('should create table with range key', function (done) {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        AttributeDefinitions : [
          { AttributeName: 'name', AttributeType: 'S' },
          { AttributeName: 'email', AttributeType: 'S' }
        ],
        KeySchema: [
          { AttributeName: 'name', KeyType: 'HASH' },
          { AttributeName: 'email', KeyType: 'RANGE' }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      };

      dynamodb.send = sinon.stub()
        .withArgs(helper.matchCommand(CreateTableCommand, request))
        .resolves({});

      table.createTable({readCapacity : 5, writeCapacity: 5}, function (err) {
        expect(err).to.be.null;
        dynamodb.send.calledWith(helper.matchCommand(CreateTableCommand, request)).should.be.true;
        done();
      });

    });

    it('should create table with secondary index', function (done) {
      var config = {
        hashKey: 'name',
        rangeKey: 'email',
        indexes : [
          { hashKey : 'name', rangeKey : 'age', name : 'ageIndex', type : 'local' }
        ],
        schema : {
          name  : Joi.string(),
          email : Joi.string(),
          age   : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts',
        AttributeDefinitions : [
          { AttributeName: 'name', AttributeType: 'S' },
          { AttributeName: 'email', AttributeType: 'S' },
          { AttributeName: 'age', AttributeType: 'N' }
        ],
        KeySchema: [
          { AttributeName: 'name', KeyType: 'HASH' },
          { AttributeName: 'email', KeyType: 'RANGE' }
        ],
        LocalSecondaryIndexes : [
          {
            IndexName : 'ageIndex',
            KeySchema: [
              { AttributeName: 'name', KeyType: 'HASH' },
              { AttributeName: 'age', KeyType: 'RANGE' }
            ],
            Projection : {
              ProjectionType : 'ALL'
            }
          }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      };

      dynamodb.send.withArgs(helper.matchCommand('CreateTableCommand')).resolves({});

      table.createTable({readCapacity : 5, writeCapacity: 5}, function (err) {
        expect(err).to.be.null;
        dynamodb.send.calledWith(helper.matchCommand('CreateTableCommand', request)).should.be.true;
        done();
      });
    });

    it('should create table with global secondary index', function (done) {
      var config = {
        hashKey: 'userId',
        rangeKey: 'gameTitle',
        indexes : [
          { hashKey : 'gameTitle', rangeKey : 'topScore', name : 'GameTitleIndex', type : 'global' }
        ],
        schema : {
          userId  : Joi.string(),
          gameTitle : Joi.string(),
          topScore  : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('gameScores', s, serializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'gameScores',
        AttributeDefinitions : [
          { AttributeName: 'userId', AttributeType: 'S' },
          { AttributeName: 'gameTitle', AttributeType: 'S' },
          { AttributeName: 'topScore', AttributeType: 'N' }
        ],
        KeySchema: [
          { AttributeName: 'userId', KeyType: 'HASH' },
          { AttributeName: 'gameTitle', KeyType: 'RANGE' }
        ],
        GlobalSecondaryIndexes : [
          {
            IndexName : 'GameTitleIndex',
            KeySchema: [
              { AttributeName: 'gameTitle', KeyType: 'HASH' },
              { AttributeName: 'topScore', KeyType: 'RANGE' }
            ],
            Projection : {
              ProjectionType : 'ALL'
            },
            ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 }
          }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      };

      dynamodb.send.withArgs(helper.matchCommand('CreateTableCommand')).resolves({});

      table.createTable({readCapacity : 5, writeCapacity: 5}, function (err) {
        expect(err).to.be.null;
        dynamodb.send.calledWith(helper.matchCommand('CreateTableCommand', request)).should.be.true;
        done();
      });
    });

    it('should create table with global secondary index', function (done) {
      var config = {
        hashKey: 'userId',
        rangeKey: 'gameTitle',
        indexes : [{
          hashKey : 'gameTitle',
          rangeKey : 'topScore',
          name : 'GameTitleIndex',
          type : 'global',
          readCapacity : 10,
          writeCapacity : 5,
          projection: { NonKeyAttributes: [ 'wins' ], ProjectionType: 'INCLUDE' }
        }],
        schema : {
          userId  : Joi.string(),
          gameTitle : Joi.string(),
          topScore  : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('gameScores', s, serializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'gameScores',
        AttributeDefinitions : [
          { AttributeName: 'userId', AttributeType: 'S' },
          { AttributeName: 'gameTitle', AttributeType: 'S' },
          { AttributeName: 'topScore', AttributeType: 'N' }
        ],
        KeySchema: [
          { AttributeName: 'userId', KeyType: 'HASH' },
          { AttributeName: 'gameTitle', KeyType: 'RANGE' }
        ],
        GlobalSecondaryIndexes : [
          {
            IndexName : 'GameTitleIndex',
            KeySchema: [
              { AttributeName: 'gameTitle', KeyType: 'HASH' },
              { AttributeName: 'topScore', KeyType: 'RANGE' }
            ],
            Projection: {
              NonKeyAttributes: [ 'wins' ],
              ProjectionType: 'INCLUDE'
            },
            ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 5 }
          }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      };

      dynamodb.send.withArgs(helper.matchCommand('CreateTableCommand')).resolves({});

      table.createTable({readCapacity : 5, writeCapacity: 5}, function (err) {
        expect(err).to.be.null;
        dynamodb.send.calledWith(helper.matchCommand('CreateTableCommand', request)).should.be.true;
        done();
      });
    });
  });

  describe('#describeTable', function () {

    it('should make describe table request', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email  : Joi.string(),
          name : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

      var request = {
        TableName: 'accounts'
      };

      dynamodb.send.withArgs(helper.matchCommand('DescribeTableCommand')).resolves({});

      table.describeTable(function (err) {
        expect(err).to.be.null;
        dynamodb.send.calledWith(helper.matchCommand('DescribeTableCommand', request)).should.be.true;
        done();
      });
    });

  });

  describe('#updateTable', function () {

    beforeEach(function () {
      var config = {
        hashKey: 'email',
        schema : {
          email  : Joi.string(),
          name : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);
    });

    it('should make update table request', function (done) {
      var request = {
        TableName: 'accounts',
        ProvisionedThroughput: { ReadCapacityUnits: 4, WriteCapacityUnits: 2 }
      };

      dynamodb.send.withArgs(helper.matchCommand('DescribeTableCommand')).resolves({});
      dynamodb.send.withArgs(helper.matchCommand('UpdateTableCommand')).resolves({});

      table.updateTable({readCapacity: 4, writeCapacity: 2}, function (err) {
        expect(err).to.be.null;
        dynamodb.send.calledWith(helper.matchCommand('UpdateTableCommand', request)).should.be.true;
        done();
      });
    });

    it('should make update table request with a promise', function (done) {
      var request = {
        TableName: 'accounts',
        ProvisionedThroughput: { ReadCapacityUnits: 2, WriteCapacityUnits: 1 }
      };

      dynamodb.send.withArgs(helper.matchCommand('DescribeTableCommand')).resolves({});
      dynamodb.send.withArgs(helper.matchCommand('UpdateTableCommand')).resolves({});

      table.updateTable({readCapacity: 2, writeCapacity: 1})
        .then(function () {
          dynamodb.send.calledWith(helper.matchCommand('UpdateTableCommand', request)).should.be.true;
          done();
        })
        .catch(function () {
          assert(false, 'catch should not be called.');
          done();
        });
    });
  });

  describe('#deleteTable', function () {

    beforeEach(function () {
      var config = {
        hashKey: 'email',
        schema : {
          email  : Joi.string(),
          name : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);
    });

    it('should make delete table request', function (done) {
      var request = {
        TableName: 'accounts'
      };

      dynamodb.send.withArgs(helper.matchCommand('DeleteTableCommand')).resolves({});

      table.deleteTable(function (err) {
        expect(err).to.be.null;
        dynamodb.send.calledWith(helper.matchCommand('DeleteTableCommand', request)).should.be.true;
        done();
      });
    });

    it('should make delete table request with a promise', function (done) {
      var request = {
        TableName: 'accounts',
      };

      dynamodb.send.withArgs(helper.matchCommand('DeleteTableCommand')).resolves({});

      table.deleteTable()
        .then(function () {
          dynamodb.send.calledWith(helper.matchCommand('DeleteTableCommand', request)).should.be.true;

          done();
        })
        .catch(function () {
          assert(false, 'catch should not be called.');
          done();
        });
    });
  });

  describe('#tableName', function () {

    it('should return given name', function () {
      var config = {
        hashKey: 'email',
        schema : {
          email  : Joi.string(),
          name : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

      table.tableName().should.eql('accounts');
    });

    it('should return table name set on schema', function () {
      var config = {
        hashKey: 'email',
        tableName : 'accounts-2014-03',
        schema : {
          email  : Joi.string(),
          name : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

      table.tableName().should.eql('accounts-2014-03');
    });

    it('should return table name returned from function on schema', function () {
      var d = new Date();
      var dateString = [d.getFullYear(), d.getMonth() + 1].join('_');

      var nameFunc = function () {
        return 'accounts_' + dateString;
      };

      var config = {
        hashKey: 'email',
        tableName : nameFunc,
        schema : {
          email  : Joi.string(),
          name : Joi.string(),
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

      table.tableName().should.eql('accounts_' + dateString);
    });

  });

  describe('hooks', function () {

    describe('#create', function () {

      it('should call before hooks', function (done) {

        var config = {
          hashKey: 'email',
          schema : {
            email  : Joi.string(),
            name : Joi.string(),
            age : Joi.number()
          }
        };

        var s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

        var item = {email : 'test@test.com', name : 'Tim Test', age : 23};
        docClient.send.withArgs(helper.matchCommand('PutCommand')).resolves({});

        serializer.serializeItem.withArgs(s, {email : 'test@test.com', name : 'Tommy', age : 23}).returns({});

        table.before('create', function (data, next) {
          expect(data).to.exist;
          data.name = 'Tommy';

          return next(null, data);
        });

        table.before('create', function (data, next) {
          expect(data).to.exist;
          data.age = '25';

          return next(null, data);
        });

        table.create(item, function (err, item) {
          expect(err).to.not.exist;
          item.get('name').should.equal('Tommy');
          item.get('age').should.equal('25');

          return done();
        });
      });

      it('should return error when before hook returns error', function (done) {
        var config = {
          hashKey: 'email',
          schema : {
            email  : Joi.string(),
            name : Joi.string(),
            age : Joi.number()
          }
        };

        var s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

        table.before('create', function (data, next) {
          return next(new Error('fail'));
        });

        table.create({email : 'foo@bar.com'}, function (err, item) {
          expect(err).to.exist;
          expect(item).to.not.exist;

          return done();
        });
      });

      it('should call after hook', function (done) {
        var config = {
          hashKey: 'email',
          schema : {
            email  : Joi.string(),
            name : Joi.string(),
            age : Joi.number()
          }
        };

        var s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

        var item = {email : 'test@test.com', name : 'Tim Test', age : 23};
        docClient.send.withArgs(helper.matchCommand('PutCommand')).resolves({});

        serializer.serializeItem.withArgs(s, item).returns({});

        table.after('create', function (data) {
          expect(data).to.exist;

          return done();
        });

        table.create(item, function () {} );
      });
    });

    describe('#update', function () {

      it('should call before hook', function (done) {
        var config = {
          hashKey: 'email',
          schema : {
            email  : Joi.string(),
            name : Joi.string(),
            age : Joi.number()
          }
        };

        var s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

        var item = {email : 'test@test.com', name : 'Tim Test', age : 23};
        docClient.send.withArgs(helper.matchCommand('UpdateCommand')).rejects(null, {});

        serializer.serializeItem.withArgs(s, item).returns({});

        serializer.buildKey.returns({email: {S: 'test@test.com' }});
        var modified = {email : 'test@test.com', name : 'Tim Test', age : 44};
        serializer.serializeItemForUpdate.withArgs(s, 'PUT', modified).returns({});

        serializer.deserializeItem.returns(modified);
        docClient.send.withArgs(helper.matchCommand('UpdateCommand')).resolves({});

        var called = false;
        table.before('update', function (data, next) {
          var attrs = _.merge({}, data, {age: 44});
          called = true;
          return next(null, attrs);
        });

        table.after('update', function () {
          expect(called).to.be.true;
          return done();
        });

        table.update(item, function () {} );
      });

      it('should return error when before hook returns error', function (done) {
        var config = {
          hashKey: 'email',
          schema : {
            email  : Joi.string(),
            name : Joi.string(),
            age : Joi.number()
          }
        };

        var s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

        table.before('update', function (data, next) {
          return next(new Error('fail'));
        });

        table.update({}, function (err) {
          expect(err).to.exist;
          err.message.should.equal('fail');

          return done();
        });
      });

      it('should call after hook', function (done) {
        var config = {
          hashKey: 'email',
          schema : {
            email  : Joi.string(),
            name : Joi.string(),
            age : Joi.number()
          }
        };

        var s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

        var item = {email : 'test@test.com', name : 'Tim Test', age : 23};
        docClient.send.withArgs(helper.matchCommand('UpdateCommand')).rejects(null, {});

        serializer.serializeItem.withArgs(s, item).returns({});

        serializer.buildKey.returns({email: {S: 'test@test.com' }});
        serializer.serializeItemForUpdate.returns({});

        serializer.deserializeItem.returns(item);
        docClient.send.withArgs(helper.matchCommand('UpdateCommand')).resolves({});

        table.after('update', function () {
          return done();
        });

        table.update(item, function () {} );
      });
    });

    it('#destroy should call after hook', function (done) {
      var config = {
        hashKey: 'email',
        schema : {
          email  : Joi.string(),
          name : Joi.string(),
          age : Joi.number()
        }
      };

      var s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, dynamodb, logger);

      docClient.send.withArgs(helper.matchCommand('DeleteCommand')).resolves({});
      serializer.buildKey.returns({});

      table.after('destroy', function () {
        return done();
      });

      table.destroy('test@test.com', function () {} );
    });
  });
});

