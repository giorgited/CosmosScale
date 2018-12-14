# Cosmos Auto Scaler

CosmosAutoScaler provides auto-scale capabilities to increase the performance of the operations while keeping the cost to the minimum. Cosmos autoscale handles the single and bulk operations seperately. During the single operation, CosmosAutoScaler will send requests by keeping the RU minimum until it recieves a 429, in which case it will start incrementing the RU by 500 until either the max RU is reached, operation is succesful or maximum retry of 10 is reached.

During the bulk operations, CosmosAutoScaler will scale the collection up to the maximum RU defined by the user to provide the best performance, and scale back down based on the elapsed inactivity time period. Inactivity time that system checks for varies between 10seconds, 1 minute and 3 minutes based on the complexity of the most recent activity. 


## Benchmarking Bulk Operations
![CosmosAutoScaler Benchmarking](https://github.com/giorgited/CosmosScale/blob/dev/Benchmarking.PNG)

## MetaData Storage Options
When initializing CosmosAutoScaler, user must provide the desired meta data storage options. This allows the CosmosAutoScaler to keep track of activities such as latest operation and latest scale. These are used to identify when to scale up/down any given collection.

PermamentCosmosCollection - with this option, CosmosAutoScaler will create a collection in the given database of cosmos where the library will store the activites. (This option is recommended if the library will be used by many machines.)

InMemoryCollection - with this option, CosmosAutoScaler will create various in-memory collections to keep track of the activites. (This option is recommended if the library will ONLY be used by at most of one machine at a time).

### Usage

Create a cosmos scale operator for each collection that you are wanting to run operations on. In the following example, we set 600RU as minimum and 1500RU as maxmium. Database is called NewDatabase1, 
with collection name of "test". CosmosScaleOperator does require to pass down the DocumentCleint.


```
 DocumentClient _client = new DocumentClient(new Uri("COSMOS_URL"), "COSMOS_PASS",
              new ConnectionPolicy
              {
                  ConnectionMode = ConnectionMode.Direct,
                  ConnectionProtocol = Protocol.Tcp,
                  RequestTimeout = TimeSpan.FromMinutes(5) // this is important depending on the concurrency 
              });
```

```
CosmosScaleOperator op = new CosmosScaleOperator(600, 15000, "NewDatabase1", "test", _client);  
await op.InitializeResourcesAsync(StateMetaDataStorage.PermamentCosmosCollection);  
```


To do CRUD operations, simply call the repsective function for 1-N documents.
```
_cosmosOperator.QueryCosmos<OBJ>("SELECT * FROM C");
await _cosmosOperator.InsertDocumentAsync(some_document);
await _cosmosOperator.DeleteDocumentAsync(some_document.id);
await _cosmosOperator.ReplaceDocument(some_document.id, new_document);

_cosmosOperator.BulkInsertDocuments(insertObjects);

```

## Authors

* **Giorgi (Gio) Tediashvili** - *Initial work* - [Gio](https://github.com/giorgited)

See also the list of [contributors](https://github.com/giorgited/CosmosScale/contributors) who participated in this project.


