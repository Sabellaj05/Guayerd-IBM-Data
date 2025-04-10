-- init scrit to create user and grant permissions

-- create db the first time
CREATE DATABASE IF NOT EXISTS db_fundacion_final;

-- create user
CREATE USER IF NOT EXISTS 'don'@'%' IDENTIFIED BY 'don';

-- permissions
GRANT ALL PRIVILEGES ON db_fundacion_final.* TO 'don'@'%';

-- flush to apply
FLUSH PRIVILEGES;
