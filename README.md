# ChapterhouseDB Example App

A simple example app that demonstrates how to use ChapterhouseDB.

## List Files in S3Mock
```bash
AWS_ACCESS_KEY_ID="key" AWS_SECRET_ACCESS_KEY="secret" aws --endpoint-url http://localhost:9090 s3 ls s3://default/ --recursive
```
```
