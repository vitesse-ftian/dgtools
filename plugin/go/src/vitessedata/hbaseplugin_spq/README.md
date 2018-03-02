Hbase Xdrive Plugin
===================

Hbase xdrive plugin is to connect hbase via hbase client API.  The plugin only supports Read operation but not Write operation.  The reason does not support Write operation is that the bulk load operation in hbase is an offline operation which is different from xdrive plugin architecture.

Sample configuration in xdrive.toml,
```
[[xdrive2.mount]]
name = "hbase"
argv = ["hbaseplugin_spq", "hbasehost", "hduser", ",", "&"]

# hbasehost is the hostname of the hbase
# hduser is the username of the hdfs user
# default field_separator is ",";
# default token_separator is "&";
```

User configuration,

| Parameter name | Default parameter value | Description | Optional | 
| -------------- | ----------------------- | ----------- | -------- |
| root | hostname | hostname of hbase | No | 
| conf.user | empty | hbase username.  Can be empty. | Yes |
| conf.field_separator | , (comma) | Separator inside the parameter value | Yes |
| conf.token_separator | & | Separator between the parameter name-value pairs | Yes | 

The readonly external database table schema,
```
DROP EXTERNAL TABLE IF EXISTS hbr;
CREATE EXTERNAL TABLE hbr(        
_row text,        
_column text,        
_value text,        
_timestamp bigint)
LOCATION ('xdrive://localhost:31416/hbase/hbasetablename')FORMAT 'SPQ';
```


The columns with underscore such as _row, _column, _value, _timestamp are mandatory fields and data type must be followed from the table shown above.  They are direct mapped to hbase table. 

**Note:  Deepgreen uses text instead of byte array which is different from hbase.**

Hbase table name are specified in the CREATE TABLE SQL statement.

```
LOCATION ('xdrive://localhost:31416/hbase/hbasetablename')FORMAT 'SPQ';
```

To access the data via SQL statement,
```
select * from hbr where dg_utils.xdrive_query($$column=cf:a&column=cf:b&QualifierFilter=binary,eq,xyz$$);
```

"&" is token_separator and "," is field_separator.  You may change the separators in xdrive.toml


Hbase Filtering
---------------
Filter are referenced from the Hbase filter client API.  

https://www.cloudera.com/documentation/enterprise/5-5-x/topics/admin_hbase_filtering.html

Comparison Operators
* LESS (<)  -> lt 
* LESS_OR_EQUAL (<=)  -> le
* EQUAL (=)  -> eq
* NOT_EQUAL (!=) -> ne
* GREATER_OR_EQUAL (>=) -> ge
* GREATER (>)  -> gt
* NO_OP (no operation) -> no

Comparators
* BinaryComparator - lexicographically compares against the specified byte array using the Bytes.compareTo(byte[], byte[]) method.  → binary
* BinaryPrefixComparator - lexicographically compares against a specified byte array. It only compares up to the length of this byte array. → binaryprefix
* RegexStringComparator - compares against the specified byte array using the given regular expression. Only EQUAL and NOT_EQUAL comparisons are valid with this comparator. [NOT SUPPORTED YET]  → regex
* SubStringComparator - tests whether or not the given substring appears in a specified byte array. The comparison is case insensitive. Only EQUAL and NOT_EQUAL comparisons are valid with this comparator. → substring

**Filter Terminology Conversion from hbase to hbase xdrive plugin**

Hbase xdrive plugin naming convention is case sensitive.

| Hbase Filtering Naming Convention | Hbase Xdrive Plugin Naming Convention |
|-----------------------------------|---------------------------------------|
| < | lt |
| <= | le |
| = | eq | 
| != | ne | 
| >= | ge |
| > | gt | 
| NO_OP (no operation) | no | 
| BinaryComparator | binary | 
| BinaryPrefixComparator | binaryprefix | 
| RegexStringComparator | regex | 
| SubStringComparator | substring |
| LongComparator | long | 
| AND | and | 
| OR | or |
| XOR | xor |

To write a CompareFilter, you have to specify 
1. Comparator type such as bitwise, binary, binaryprefix, regex, substring or long.
2. Operators are lt, le, eq, ne, ge, gt and no.  For bitwise operators are and, or, xor. 
3. Filter string

For example, you want to filter the result having the substring “abc”.  You can write the following filter query:

```
substring,eq,abc
```


Parameter name-value pair of the hbase query
Here is the list of parameters,

| Parameter name | Parameter value | Note |
| -------------- | --------------- | ---- |
| column | column_family:column_name,  e.g. cf:a | Multiple columns can be specified by having multiple column name-value pairs.  e.g. column=cf:a&column=cf:b |
| limit | integer | limit=10 |
| offset | integer | offset=20 |
| startrow | rowkey | startrow=start |
| stoprow | rowkey | stoprow=stop |
| timerange | starttime,endtime | e.g. 123,456 |
| ColumnCountGetFilter | integer | ColumnCountGetFilter=10 |
| ColumnPaginationFilter | limit,offset | e.g.ColumnPaginationFilter=10,100 |
| ColumnPrefixFilter | prefix | ColumnPrefixFilter=prefix |
| ColumnRangeFilter | mincol,maxcol,minColumnInclusive,maxColumnInclusive | e.g ColumnRangeFilter=c1,c5,true,true |
| DependentColumnFilter | For binary or string filter, columnfamily:qualifier,[binary\|long\|binaryprefix\|substring],[lt\|le\|eq\|ne\|ge\|gt],filterstring or For bitwise, columnfamily:qualifier,bits,[and\|or\|xor],filterstring | e.g.  DependentColumnFilter=cf:a,substring,eq,apple  |
| FamilyFilter | [binary\|long\|binaryprefix\|substring],[lt\|le\|eq\|ne\|ge\|gt],filterstring or for bitwise, bits,[and\|or\|xor],filterstring | FamilyFilter=binary,ne,filter |
| FirstKeyOnlyFilter | bool | FirstKeyOnlyFilter=true | 
| FirstKeyValueMatchingQualifiersFilter | list of qualifier q1,q2,...,qN | FirstKeyValueMatchingQualifiersFilter=q1,q2,q3 |
| InclusiveStopFilter | key of stoprow | InclusiveStopFilter=stoprow | 
| KeyOnlyFilter | bool | KeyOnlyFilter=true |
| MultipleColumnPrefixFilter | list of prefixes prefix1,prefix2,...,prefixN | MultipleColumnPrefixFilter=p1,p2,p3 |
| PageFilter | pagesize integer | PageFilter=20 |
| PrefixFilter | prefix | PrefixFilter=prefix | 
| QualifierFilter | [binary\|long\|binaryprefix\|substring],[lt\|le\|eq\|ne\|ge\|gt],filterstring or for bitwise, bits,[and\|or\|xor],filterstring | QualifierFilter=binary,eq,qualifier  |
| RandomRowFilter | float | RandomRowFilter=0.9 |
| RowFilter | [binary\|long\|binaryprefix\|substring],[lt\|le\|eq\|ne\|ge\|gt],filterstring or for bitwise, bits,[and\|or\|xor],filterstring | RowFilter=bitwise,and,abc |
| SingleColumnValueFilter | For binary or string filter, columnfamily:qualifier,[binary\|long\|binaryprefix\|substring],[lt\|le\|eq\|ne\|ge\|gt],filterstring or For bitwise, columnfamily:qualifier,bits,[and\|or\|xor],filterstring | e.g.  SingleColumnValueFilter=cf:a,substring,eq,apple |
| SingleColumnValueExcludeFilter | For binary or string filter, columnfamily:qualifier,[binary\|long\|binaryprefix\|substring],[lt\|le\|eq\|ne\|ge\|gt],filterstring or For bitwise, columnfamily:qualifier,bits,[and\|or\|xor],filterstring | e.g.  SingleColumnValueExcludeFilter=cf:a,substring,eq,apple |
| SkipFilter | | Not supported yet |
| TimestampsFilter | list of timestamps (int64) ts1,ts2,...,tsN | TimestampsFilter=123,456,789 |
| ValueFilter | [binary\|long\|binaryprefix\|substring],[lt\|le\|eq\|ne\|ge\|gt],filterstring or for bitwise, bits,[and\|or\|xor],filterstring | ValueFilter=bitwise,and,abc |
| WhileMatchFilter | | Not supported yet |
| FuzzyRowFilter | | Not supported yet |
| RowRangeFilter | startrow, stoprow, startrowinclusive, stoprowinclusive | Mulitple row ranges can be specified by multiple name-value pairs.  e.g RowRangeFilter=start1,stop1,false,false&RowRangeFilter=start2,stop2,false,true |



