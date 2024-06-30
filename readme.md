# netty-mysql-connector

## Introduction
This is MySQL connector based on netty. It implements [MySQL native protocol](https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html), not X DevAPI.

Compared to [MySQL Connector/J](https://dev.mysql.com/doc/connector-j/en/), this connector provides async methods, and do not support JDBC, because JDBC APIs are not async.

This project is not finished.

## Example
There are some examples in tests. You can start from HelloTest.java. Before runnning test, install MySQL database and run script BeforeTests.sql.