// MongoDB 초기화 스크립트
db = db.getSiblingDB('onebailey');

// 사용자 생성
db.createUser({
  user: 'trader',
  pwd: 'secure_password',
  roles: [
    {
      role: 'readWrite',
      db: 'onebailey'
    },
    {
      role: 'dbAdmin',
      db: 'onebailey'
    }
  ]
});

// 기본 컬렉션 생성
db.createCollection('trades');
db.createCollection('portfolio');
db.createCollection('settings');
db.createCollection('backtest_results');
db.createCollection('daily_performance');

print('MongoDB 초기화 완료');
