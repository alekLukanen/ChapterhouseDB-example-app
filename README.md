# ChapterhouseDB Example App

A simple example app that demonstrates how to use ChapterhouseDB.

## Querying Local Files With DuckDB
```sql
create secret locals3mock3 (
  TYPE S3,
  KEY_ID "key",
  SECRET "secret",
  ENDPOINT "localhost:9090",
  URL_STYLE "path",
  USE_SSL false
);

select * from 's3://default/chdb/table-state/part-data/table1/0/d_2_0.parquet';
```

## List Files in S3Mock
```bash
AWS_ACCESS_KEY_ID="key" AWS_SECRET_ACCESS_KEY="secret" aws --endpoint-url http://localhost:9090 s3 ls s3://default/ --recursive
```
