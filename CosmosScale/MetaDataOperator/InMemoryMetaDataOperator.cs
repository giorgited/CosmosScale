using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CosmosScale.Models;

namespace CosmosScale.MetaDataOperator
{
    internal class InMemoryMetaDataOperator : IMetaDataOperator
    {
        private static ConcurrentDictionary<Tuple<string, string>, Tuple<DateTimeOffset, ActivityStrength>> latestActivty = new ConcurrentDictionary<Tuple<string, string>, Tuple<DateTimeOffset, ActivityStrength>>();
        private static ConcurrentBag<ActiveCollection> collectionsCacheAside = new ConcurrentBag<ActiveCollection>();

        public async Task AddActiveCollection(string databaseName, string collectionName, int minimumRu)
        {
            if (!collectionsCacheAside.Any(c => c.DatabaseName == databaseName && c.CollectionName == collectionName))
            {
                //item for this db and collection exist, min RU will NOT be replaced, min RU should be chosen globaly
                collectionsCacheAside.Add(new ActiveCollection(databaseName, collectionName, minimumRu));
            }
        }

        public async Task AddActivity(string databaseName, string collectionName, DateTimeOffset date, ActivityStrength activityStrength)
        {
            latestActivty[new Tuple<string, string>(databaseName, collectionName)] = new Tuple<DateTimeOffset, ActivityStrength>(date, activityStrength);
        }

        public IEnumerable<ActiveCollection> GetAllActiveCollections()
        {
            return collectionsCacheAside.AsEnumerable();
        }

        public Activity GetLatestActivity(string databaseName, string collectionName)
        {
            if (latestActivty.TryGetValue(new Tuple<string, string>(databaseName, collectionName), out var res))
            {
                return new Activity(databaseName, collectionName, res.Item1, res.Item2);
            }

            return null;
        }
    }
}
