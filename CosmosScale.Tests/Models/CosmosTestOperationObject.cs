using System;
using System.Collections.Generic;
using System.Text;

namespace CosmosScale.Tests
{
    public class CosmosTestOperationObject
    {
        public string id { get; set; } = Guid.NewGuid().ToString();
        public int SomeRandomProperty { get; set; }
        public int SomeRandomProperty2 { get; set; }
    }
}
