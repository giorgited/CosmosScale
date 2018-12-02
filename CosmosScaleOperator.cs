using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace CosmosScale
{
    public class CosmosScaleOperator
    {
        private DocumentClient _client;
        private int _minRu, _maxRu;
        private Uri _collectionUri;
        private string _databaseName, _collectionName;
        private string _collectionPartitionKey;
        private const int _maximumRetryCount = 30;

        public CosmosScaleOperator(int minimumRu, int maximumRu, string databaseName, string collectionName, DocumentClient client, string partitionKeyPropertyName = "/id")
        {
            _client = client;
            _minRu = minimumRu;
            _maxRu = maximumRu;
            _databaseName = databaseName;
            _collectionName = collectionName;
            _collectionPartitionKey = partitionKeyPropertyName;
            _collectionUri = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName);
        }

        public IEnumerable<T> QueryCosmos<T>(string cosmosSqlQuery, int maxItemCount = -1, bool QueryCrossPartition = true)
        {
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = maxItemCount, EnableCrossPartitionQuery = QueryCrossPartition };

            return _client.CreateDocumentQuery<T>(
                    _collectionUri,
                    new SqlQuerySpec()
                    {
                        QueryText = cosmosSqlQuery
                    },
                    queryOptions);
        }

        #region INSERT
        public async Task<CosmosOperationResponse> InsertDocumentAsync(object document)
        {
            CosmosOperationResponse result = new CosmosOperationResponse();
            return await InsertDocumentAsync(document, 0, result);
        }
        private async Task<CosmosOperationResponse> InsertDocumentAsync(object document, int retryCount, CosmosOperationResponse result)
        {
            try
            {
                await _client.CreateDocumentAsync(_collectionUri, document);
                result.Success = true;
                result.TotalRetries = retryCount;
                return result;
            }
            catch (Exception e)
            {
                if (e.Message.Contains("Request rate is large"))
                {
                    if (retryCount > _maximumRetryCount)
                    {
                        result.Success = false;
                        result.TotalRetries = retryCount;
                        return result;
                    }
                    else
                    {
                        var op = await ScaleLogic.ScaleUpCollectionAsync(_client, _databaseName, _collectionName, _minRu);
                        result.ScaleOperations.Add(op);
                        return await InsertDocumentAsync(document, retryCount++, result);
                    }
                }
                else
                {
                    throw;
                }
            }

        }
        #endregion

        #region DELETE
        public async Task<CosmosOperationResponse> DeleteDocumentAsync(string id, object partitionKey = null)
        {
            CosmosOperationResponse result = new CosmosOperationResponse();
            return await DeleteDocumentAsync(id, 0, result, partitionKey);
        }

        private async Task<CosmosOperationResponse> DeleteDocumentAsync(string id, int retryCount, CosmosOperationResponse result, object partitionKey = null)
        {
            try
            {
                var docUri = UriFactory.CreateDocumentUri(_databaseName, _collectionName, id);
                if (partitionKey == null)
                {
                    await _client.DeleteDocumentAsync(docUri, new RequestOptions() { PartitionKey = new PartitionKey(id) });
                }
                else
                {
                    await _client.DeleteDocumentAsync(docUri, new RequestOptions() { PartitionKey = new PartitionKey(partitionKey) });
                }
                result.Success = true;
                result.TotalRetries = retryCount;
                return result;
            }
            catch (Exception e)
            {
                if (e.Message.Contains("Request rate is large"))
                {
                    if (retryCount > _maximumRetryCount)
                    {
                        result.Success = false;
                        result.TotalRetries = retryCount;
                        return result;
                    }
                    else
                    {
                        var op = await ScaleLogic.ScaleUpCollectionAsync(_client, _databaseName, _collectionName, _minRu);
                        result.ScaleOperations.Add(op);
                        return await DeleteDocumentAsync(id, retryCount++, result, partitionKey);
                    }
                }
                else
                {
                    throw;
                }
            }
        }
        #endregion

        public void UpdateDocument()
        {

        }
    }

    public class CosmosOperationResponse
    {
        public bool Success { get; set; }
        public List<ScaleOperation> ScaleOperations { get; set; } = new List<ScaleOperation>();
        public int TotalRetries { get; set; }
    }

    public class ScaleOperation
    {
        public int ScaledFrom { get; set; }
        public int ScaledTo { get; set; }
        public DateTimeOffset OperationTime { get; set; }
        public bool ScaledSuccess { get; set; } = true;
        public string ScaleFailReason { get; set; }
    }
}