# Kafka Connect transforms for Qlik Replicate  

[![Last Version](https://img.shields.io/github/tag-pre/michelin/kafka-connect-transforms-qlik-replicate.svg)](https://github.com/michelin/kafka-connect-transforms-qlik-replicate/releases)
[![Build Status](https://github.com/michelin/kafka-connect-transforms-qlik-replicate/workflows/Build/badge.svg)](https://github.com/michelin/kafka-connect-transforms-qlik-replicate/actions)    
[![License](https://img.shields.io/badge/license-Apache-green.svg)](https://opensource.org/licenses/Apache-2.0)
![Github Downloads](https://img.shields.io/github/downloads/michelin/kafka-connect-transforms-qlik-replicate/total)
![Github Start](https://img.shields.io/github/stars/michelin/kafka-connect-transforms-qlik-replicate.svg)

## ExtractNewRecordState  
  
*Disclaimer:* This work is inspired by [Debezium ExtractNewRecordState SMT](https://debezium.io/documentation/reference/1.2/configuration/event-flattening.html). We take the opportunity to thanks the whole Debezium team for the quality of their work and for making it available for all.

### Change event structure  

Qlik Replicate change events are composed by 3 parts:  
- `headers`: which includes  
  - `operation`: The SQL operation: `INSERT`, `UPDATE`, `DELETE` or `REFRESH` (ie. initial loading)
  - `changeSequence`: A monotonically increasing change sequence that is commom to all change tables of a task   
  - `timestamp`: The original change UTC timestamp.
  - `streamPosition`: The source stream offset position. 
  - `transactionId`: The ID of the transaction that the change record belongs to. 
  - `changeMask`: Hexadecimal sting to detect which columns have changed (internal purpose).  
  - `columnMask`: Hexadecimal sting to present which columns are present (internal purpose).
  - `transactionEventCounter`: The sequence number of the current operation in the transaction.
  - `transactionLastEvent`: Flag to mark the last change of a transaction.
    
 * `beforeData`: The database record before the change.  
 * `data`: The database record after the change    
   
For example, an `UPDATE` change event looks like this:  
```json  
{
	"data": {
		"id": 1,
		"email": "foo@bar.com"
	},
	"beforeData": {
		"id": 1,
		"email": "john@doe.com"
	},
	"headers": {
		"operation": "UPDATE",
		"changeSequence": "100",
		"timestamp": 1000,
		"streamPosition": "1",
		"transactionId": "1",
		"changeMask": "1",
		"columnMask": "1",
		"transactionEventCounter": 1,
		"transactionLastEvent": true
	}
}
```  
  
The structure provides all the information Qlik Replicate has.   
However, the rest of the Kafka ecosystem usually expect the data in a more simple format like:  
```json
{  
    "id": 1,
    "email": "john@doe.com"
}  
```   

## Getting Started  
 
Download the latest jar on the [release page](https://github.com/michelin/kafka-connect-transforms-qlik-replicate/releases).

The SMT jar should be present in the `plugin.path` with the other Kafka connector/SMT jars.

The configuration of the `ExtractNewRecordState` SMT is made in your Kafka Connect sink connector's configuration.

Given the following Qlik Replicate change event message:
```
{
	"data": null,
	"beforeData": {
		"id": 1,
		"email": "john@doe.com"
	},
	"headers": {
		"operation": "DELETE",
		"changeSequence": "100",
		"timestamp": 1000,
		"streamPosition": "1",
		"transactionId": "1",
		"changeMask": "1",
		"columnMask": "1",
		"transactionEventCounter": 1,
		"transactionLastEvent": true
	}
}
```

We can use the following configuration
```  
"transforms": "extract",
"transforms.extract.type": "com.michelin.kafka.connect.transforms.qlik.replicate.ExtractNewRecordState",
"transforms.extract.delete.mode": "soft",
"transforms.extract.add.fields": "operation,timestamp",
"transforms.extract.add.headers": "transactionId"
```  

**`delete.mode=soft`**

The SMT adds `__deleted` and set it to true whenever a delete operation is processed.

**`add.fields=operation,timestamp`**

Adds change event metadata (coming from the `headers` property). 
It will add a `__operation` and `__timestamp` field to teh simplified Kafka record.

**`add.headers=transactionId`**

Same as `add.fields` but with the Kafka record headers.

We will obtain the following Kafka message:
```
{  
    "id": "1",
    "email": "john@doe.com
    "__deleted": "true",
    "__operation": "DELETE",
    "__timestamp": "1000"
}
```
and a header `"__transactionId: "1"`.

## How it works  

The `ExtractNewRecordState` SMT (Single Message Transformation) extract the `data` field form Qlik Replicate change event in a Kafka record. 
The value of the `data` field will replace the original message content to build a simpler Kafka record.

### Update Management

When a record is updated, the `beforeData` will represent the previous state and `data` the current.
By extracting the `data` field, the SMT exposes the updated record.
If you plan to sink the updated record into a database, please use the `upsert` semantic (INSERT OR UPDATE).
 
### DDL Management

By comparing the `beforeData` and `data` properties, one is able to detect the source table modifications (add/remove columns).
The SMT will automatically build the new Schema corresponding to the new table structure. 

### Delete management

A database `DELETE` operation generate a change event like this:
```
{
	"beforeData": null,
	"data": {
		"id": 1,
		"email": "john@doe.com"
	},
	"headers": {
		"operation": "DELETE",
		"changeSequence": "100",
		"timestamp": 1000,
		"streamPosition": "1",
		"transactionId": "1",
		"changeMask": "1",
		"columnMask": "1",
		"transactionEventCounter": 1,
		"transactionLastEvent": true
	}
}
```
The change event `data` has captured the record state and the `headers.operation` is set to `DELETE`.
 
`ExtractNewRecordState` SMT proposes 3 options:
- Produce a tombstone record (Use case: the JDBCSinkConnector can be configured to convert a tombstones into an actual SQL delete statement on the Sink repository).
- Skip the record to prevent any downstream delete management.
- Do a *soft delete* (aka. logical delete), the SMT will add a `__deleted` field to each message and toggle to `true` whenever a delete operation is processed.

## Qlik Replicate Envelope

Qlick Replicate permits to wrap the change event data with an envelope like this:
```json
{
	"magic": "atMSG",
	"type": "DT",
	"headers": null,
	"messageSchemaId": null,
	"messageSchema": null,
	"message": {
		"data": {
			"id": 1,
			"email": "foo@bar.com"
		},
		"beforeData": {
			"id": 1,
			"email": "john@doe.com"
		},
		"headers": {
			"operation": "UPDATE",
			"changeSequence": "100",
			"timestamp": 1000,
			"streamPosition": "1",
			"transactionId": "1",
			"changeMask": "1",
			"columnMask": "1",
			"transactionEventCounter": 1,
			"transactionLastEvent": true
		}
	}
}
```

The SMT detects the presence of the envelop and unwrap automatically to produce a simple message.

## Schema management
 
Qlik Replicate relies on JSON to represent a change event. 
Using schemas is highly recommended when you're building production-grade application but if your need a quick and dirty test, 
the SMT can process  messages with and without schemas.
 
Note: Schemas are mandatory for some components like `JdbcSinkConnector` for instance...
 
## Properties

|Property name|Default| Description|
|--|--|--|
|`delete.mode`|`apply`| Delete events will produce a tombstone. This property permits to choose between 3 modes: <br>- `apply`: A tombstone will be produce (mainly to triggers the JDBCSinkConnector delete support).<br> - `skip`: The Kafka record will be dropped and not passed to the downstream processing components.<br> - `soft`: Adds a `__deleted` field to each message. When an insert or update is processed the `_deleted` field is set to false. <br><br> When a delete occurs, teh SMT will use the `beforeData` property and set the `__deleted` field to true. 
|`add.fields`|`""`| Adds a field based on change event metadata (coming exclusively from the `headers` property).<br>The field name will be prefixed by `__` (double underscores). The field value will be converted to string. |
|`add.headers`|`""`|Adds a header based on change event metadata (coming exclusively from the `headers` property).<br>The header name will be prefixed by `__` (double underscores).|

## ReplicateTimestampConverter

### Microseconds management
Qlik replicate use a microseconds precision when representing a timestamp. 
As for now, the [`TimestampComverter`](https://docs.confluent.io/current/connect/transforms/timestampconverter.html) only supports the milliseconds precision.
The `ReplicateTimestampConverter` SMT is a duplication of `TimestampComverter` which use a microseconds as default timestamp precsion rather than milliseconds.

Note: This SMT is very Qlik Replicate specific and should be removed when `TimestampConverter` will support microseconds. 

## Properties
See the official documentation
https://docs.confluent.io/current/connect/transforms/timestampconverter.html

## How to release

The release process relies on Maven Release plugin.
Run `./mvnw release:prepare`, it will ask you for the version name, SCM tag name and the nest development version name.
Once finished, you obtain a tag and the pom.xml has been updated.
If everything looks good, run `./mvnw release:perform` to confirm the changes otherwise run `./mvnw release:rollback`. 

Once done go to Github Release page, you'll see a `Draft` release, this has been automatically generated when the sources haven been tagged.
Select the tag you just made and define name for the release (the tag version is perfect).
The changelog is already generated via [release drafter](https://github.com/release-drafter/release-drafter).
If everything is ok, publish the release. The artifact will be automatically build and attached to the release.
 
# License
Kafka Connect SMT for Qlik Replicate is released under the MIT License. See the bundled [LICENSE](LICENSE) file for details. 
