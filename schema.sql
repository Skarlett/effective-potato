create table if not exists Coins(
   ID INTEGER primary key autoincrement,
   name varchar(60),
   symbol varchar(6),

   unique(name, symbol)
);

create table if not exists PriceIndex(
   ID INTEGER primary key autoincrement,
   time_stamp DATETIME DEFAULT CURRENT_TIMESTAMP not null,

   coin INTEGER not null,
   price_per_coin float not null,
   volume INTEGER not null,
   market_cap INTEGER not null,

   FOREIGN KEY(coin) REFERENCES Coins(ID)
);
