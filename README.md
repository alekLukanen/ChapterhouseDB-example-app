# ChapterhouseDB Example App

A simple example app that demonstrates how to use ChapterhouseDB.

## Setup

In my case I have a cluster of two Raspberry PI computers with host names `pi0` and `pi1`.
The deployments rely on having two nodes in your Kubernetes cluster. If you wish to change
the preferred host names you can change the `preferredHostname` for each of the deployments
in the `helm-deploy/chdb-ex/values.yaml` file. You will also need to change the container 
registry in the `Tiltfile` file to match the container registry you are using. Change
all occurrences of `pi0:30000` to your container registry.

After making the necessary changes you can run
```
tilt up
```
This will deploy the example application to your Kubernetes cluster.

## View Images in Container Registry

You can view the images in the given registry by using a url like this
```
http://pi0:30000/v2/_catalog
```

## Querying Local Files With DuckDB
```sql
create secret locals3mock3 (
  TYPE S3,
  KEY_ID "minioadmin",
  SECRET "minioadmin",
  ENDPOINT "pi0:30006",
  URL_STYLE "path",
  USE_SSL false
);

select * from 's3://chdb-test-warehouse/chdb/table-state/part-data/table1/0/d_2_0.parquet';
```

Validate that there aren't any duplicates
```sql
select column1, count(*) num_items from 's3://chdb-test-warehouse/chdb/table-state/part-data/table1/*/*.parquet' group by column1 having count(*) > 1 order by column1;
```

Summary statistics
```sql
select count(*) count, sum(column1) sum, max(column1) max, min(column1) min from 's3://chdb-test-warehouse/chdb/table-state/part-data/table1/*/*.parquet';
```

## List Files in S3Mock
```bash
AWS_ACCESS_KEY_ID="key" AWS_SECRET_ACCESS_KEY="secret" aws --endpoint-url http://localhost:9090 s3 ls s3://default/ --recursive
```
