ElasticSearch XDrive Plugin Documentation
=========================================

ElasticSearch XDrive plugin is an interface between elasticsearch and deepgreen for real-time operations.  It is used for real-time operation with fast and small dataset return from Elasticsearch.  The plugin queries the elasticsearch via the REST APIs and return the result to deepgreen via xdrive.  For data loading or bulk operation, please use esload_spq instead.

ElasticSearch supported versions
--------------------------------
Elastic xdrive plugin supports elasticsearch v5.5 or above.  The plugin supports both AWS ES and open source.  

ElasticSearch Plugin deployment 
-------------------------------
We assume elasticsearch cluster is behind a load balancer with single access endpoint.  ElasticSearch plugin supports AWS ES cluster with authentication and open source ES cluster.

To deploy esplugin, run xdrctl to deploy esplugin to each data nodes.

	% xdrctl deployplugin xdrive.toml esplugin

ElasticSearch plugin configuration settings
-------------------------------------------
For example, ES Cluster has single service endpoint esnode and have 4 elasticsearch data nodes (esnode0, esnode1, esnode2, esnode3) with 5 shards (0, 1, 2, 3, 4).  Amazon ES domain is behind elastic load balancer (ELB) so the load is evenly distributed by ELB.   

The configuration is specified in xdrive mount point (xdrive.toml).
```
[[xdrive2.mount]]
name="es" # mount point name
argv = ["esplugin_spq", "es_endpoint", "index", "nshards", "aws_access_id", "aws_access_key"]
# conf="index=indexname,nshards=5,access_key_id=key_id,secret_access_key=secret"  # fill in the index name and number of shards of the index and access key of AWS ES service
```

If using non-AWS ES cluster, you may ignore the setting of access_key_id and secret_access_key.

Sample xdrive.toml Configuration for all deepgreen nodes,
```
[[xdrive2.mount]]
name = "es"
argv = ["esplugin_spq", "http://localhost:9200", "index", "5", "aws_access_id", "aws_access_key"]
```



READ Operation
--------------
External Table SQL Schema

```
DROP EXTERNAL TABLE IF EXISTS esr;
CREATE EXTERNAL TABLE esr
      (
        _id  text,
        _type text,
        _routing text,
        name text,
        age int,
        last_updated bigint
        )
LOCATION ('xdrive://localhost:31416/es/')
FORMAT 'SPQ';
```

* Column \_id, \_type and \_routing are mandatory.

Query SQL for query elasticsearch via deepgreen
The query SQL contains the elasticsearch query string embedded with dg\_utils.xdrive\_query($$\*$$).  See the example below:

```
SELECT name, email from esr where dg_utils.xdrive_query($$q=name:eric lam +email:eric*&_type=external&routing=offline$$);
```


See the link below for search uri request parameters.

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-uri-request.html

Parameters allowed in the query.

| Name  | Description |
|-------|-------------|
| q  | The query string (maps to the query_string query, seeQuery String Query for more details).  See more: https://www.elastic.co/guide/en/elasticsearch/reference/5.6/query-dsl-query-string-query.html |
| df | The default field to use when no field prefix is defined within the query. |
| analyzer | The analyzer name to be used when analyzing the query string. |
| analyze_wildcard | Should wildcard and prefix queries be analyzed or not. Defaults to false. |
| batched_reduce_size | The number of shard results that should be reduced at once on the coordinating node. This value should be used as a protection mechanism to reduce the memory overhead per search request if the potential number of shards in the request can be large. |
| default_operator | The default operator to be used, can be AND or OR. Defaults to OR. |
| lenient | If set to true will cause format based failures (like providing text to a numeric field) to be ignored. Defaults to false. |
| explain | For each hit, contain an explanation of how scoring of the hits was computed. |
| \_source | Set to false to disable retrieval of the \_source field. You can also retrieve part of the document by using \_source\_include & \_source\_exclude (see the request bodydocumentation for more details) |
| stored\_fields | The selective stored fields of the document to return for each hit, comma delimited. Not specifying any value will cause no fields to return. |
| sort | Sorting to perform. Can either be in the form of fieldName, or fieldName:asc/fieldName:desc. The fieldName can either be an actual field within the document, or the special \_score name to indicate sorting based on scores. There can be several sort parameters (order is important). | 
| track\_scores | When sorting, set to true in order to still track scores and return them as part of each hit. |
| timeout | A search timeout, bounding the search request to be executed within the specified time value and bail with the hits accumulated up to that point when expired. Defaults to no timeout. |
| terminate\_after | The maximum number of documents to collect for each shard, upon reaching which the query execution will terminate early. If set, the response will have a boolean field terminated\_early to indicate whether the query execution has actually terminated\_early. Defaults to no terminate\_after. |
| from | The starting from index of the hits to return. Defaults to 0. |
| size | The number of hits to return. Defaults to 10. |
| search\_type | The type of the search operation to perform. Can bedfs\_query\_then\_fetch or query\_then\_fetch. Defaults to query\_then\_fetch. See Search Type for more details on the different types of search that can be performed. | 
| routing | routing  | 
| \_type | type of the index |

Load Test Data
--------------
To test the elasticsearch plugin, you can download the elasticsearch test data https://github.com/oliver006/elasticsearch-test-data

Use the command below to insert the random generated data into ES and index "test_data" will be created with 1000 documents uploaded to ES.

	$ python es_test_data.py --es_url=http://localhost:9200 --num_of_shards=5

Query ElasticSearch via psql
-----------------------------

```
wetestdata=# select * from esr where dg_utils.xdrive_query($$q=name:Rh*&size=50$$) limit 10;
         _id          |   _type   | _routing |   name    |  age  | last_updated  
----------------------+-----------+----------+-----------+-------+---------------
 AV8pQPeUGpZ_PbHsjbYB | test_type |          | rhFi      | 69362 | 1508168139000
 AV8pQPeVGpZ_PbHsjbfw | test_type |          | RHZ2OrD8  | 43937 | 1507961176000
 AV8pQPsVGpZ_PbHsjdlP | test_type |          | rh60      | 45217 | 1508828996000
 AV8pQP06GpZ_PbHsjfTS | test_type |          | RhgN4K    | 85223 | 1510134961000
 AV8pQP06GpZ_PbHsjfT_ | test_type |          | RhUbaR    | 24479 | 1507699341000
 AV8pQP-RGpZ_PbHsjhZj | test_type |          | rhSdHlJ   | 54039 | 1507593983000
 AV8pQQEMGpZ_PbHsjib1 | test_type |          | RH6nl     | 25762 | 1507845612000
 AV8pQQGVGpZ_PbHsjjGX | test_type |          | rHkWSVXU  | 90604 | 1510685123000
 AV8pQQGVGpZ_PbHsjjIs | test_type |          | RhO8Z40y1 | 87279 | 1505747834000
 AV8pQQTiGpZ_PbHsjlso | test_type |          | rH8LdzmE6 | 15953 | 1510403194000
(10 rows)
```


WRITE operation
---------------
To create a writable external table,

```
DROP EXTERNAL TABLE IF EXISTS esw;
CREATE WRITABLE EXTERNAL TABLE esw
      (
        _id  text,
        _type text,
        _routing text,
        name text,
        age int,
        gender text
        )
LOCATION ('xdrive://localhost:31416/es/')
FORMAT 'SPQ';
```
* Column \_id must be a non-empty

and use INSERT SQL statement to insert data into elasticsearch
```
wetestdata=# insert into esw values ('abc', 'abc_type', '', 'eric', 40, 1508527944000);
INSERT 0 1

wetestdata=# select * from esr where dg_utils.xdrive_query($$q=name:eric$$) ;
 _id |  _type   | _routing | name | age | last_updated  
-----+----------+----------+------+-----+---------------
 abc | abc_type |          | eric |  40 | 1508527944000

```


