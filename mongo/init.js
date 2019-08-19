
db.createCollection("driverRate");
db.createCollection("requestRate");


db.driverRate.createIndex({location:"2d"});
db.requestRate.createIndex({location:"2d"});