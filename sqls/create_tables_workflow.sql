-- SQLite
CREATE TABLE passenger_bronze (
    id VARCHAR PRIMARY KEY,
    date_registered VARCHAR NULL,
    country_code VARCHAR NULL
);

CREATE TABLE passenger_silver (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_registered DATETIME NOT NULL,
    country_code VARCHAR NULL
);

CREATE TABLE passenger_gold (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_registered DATETIME NOT NULL,
    country_code VARCHAR NULL
);

CREATE TABLE booking_bronze (
    id VARCHAR PRIMARY KEY,
    id_passenger VARCHAR NOT NULL,
    date_created VARCHAR NULL,
    date_close VARCHAR NULL
);

CREATE TABLE booking_silver (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_passenger INTEGER NOT NULL,
    date_created DATETIME NOT NULL,
    date_close DATETIME NOT NULL,
    FOREIGN KEY(id_passenger) REFERENCES passenger_silver(id)
);

CREATE TABLE booking_gold (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_passenger INTEGER NOT NULL,
    date_created DATETIME NOT NULL,
    date_close DATETIME NOT NULL,
    FOREIGN KEY(id_passenger) REFERENCES passenger_gold(id)
);
