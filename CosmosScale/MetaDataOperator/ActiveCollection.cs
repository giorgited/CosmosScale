using System;
using System.Collections.Generic;
using System.Text;

namespace CosmosScale.MetaDataOperator
{
    public class ActiveCollection
    {
        public ActiveCollection(string databaseName, string collectionName, int minimumRU)
        {
            DatabaseName = databaseName;
            CollectionName = collectionName;
            MinimumRU = minimumRU;
        }

        public string DatabaseName { get; set; }
        public string CollectionName { get; set; }
        public int MinimumRU { get; set; }
        public string MetaDataType { get; } = "ActiveCollection";
    }
}
