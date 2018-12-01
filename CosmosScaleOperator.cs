using System;

using Microsoft.Azure.Documents.Client;

namespace CosmosScale
{
    public class CosmosScaleOperator
    {
        private DocumentClient _client;
        private int _minRu, _maxRu;
        private string _databaseName, _collectionName;

        public CosmosScaleOperator(int minimumRu, int maximumRu, string databaseName, string collectionName, DocumentClient client)
        {
            _client = client;
            _minRu = minimumRu;
            _maxRu = maximumRu;
            _databaseName = databaseName;
            _collectionName = collectionName;
        }
        public void GetDocument()
        {

        }
        public void InsertDocument()
        {

        }

        public void DeleteDocument()
        {

        }

        public void UpdateDocument()
        {

        }

    }
}