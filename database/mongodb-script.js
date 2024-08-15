db = db.getSiblingDB('source');
db.createCollection('users');
db.users.insertOne({firstname: 'saksit', lastname: 'chobngan', email: 'saksit_ch@gmail.com', age: 22});
