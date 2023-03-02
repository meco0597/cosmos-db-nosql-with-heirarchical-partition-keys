// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

using Microsoft.Azure.Cosmos;
using System;
using System.Diagnostics;
using System.Net;
using System.Reflection.Metadata;

Stopwatch timer = new Stopwatch();

// New instance of CosmosClient class
using CosmosClient client = new(
    accountEndpoint: Environment.GetEnvironmentVariable("COSMOS_ENDPOINT")!,
    authKeyOrResourceToken: Environment.GetEnvironmentVariable("COSMOS_KEY")!
);

Database database = await client.CreateDatabaseIfNotExistsAsync(
    id: "hierarchical-poc-db"
);
Console.WriteLine($"Created database:\t{database.Id}");

// Container reference with creation if it does not alredy exist
// List of partition keys, in hierarchical order. You can have up to three levels of keys.
List<string> subpartitionKeyPaths = new List<string> {
    "/tenantId",
    "/subscriptionId",
};

// Create container properties object
ContainerProperties containerProperties = new ContainerProperties(
    id: "hierarchical-container",
    partitionKeyPaths: subpartitionKeyPaths
);

// Create container - subpartitioned by TenantId -> UserId -> SessionId
Container container = await database.CreateContainerIfNotExistsAsync(containerProperties, throughput: 400);
Console.WriteLine($"Created container:\t{container.Id}");

// Create new object and upsert (create or replace) to container
string name = "default";

List<Guid> tenantIds = new List<Guid>()
{
    Guid.NewGuid(),
    Guid.NewGuid(),
    Guid.NewGuid(),
    Guid.NewGuid(),
    Guid.NewGuid(),
};

List<Guid> subscriptionIds = new List<Guid>()
{
    Guid.NewGuid(),
    Guid.NewGuid(),
    Guid.NewGuid(),
    Guid.NewGuid(),
    Guid.NewGuid(),
};

string scope = $"_subscriptions_{subscriptionIds[0]}";

Consent newConsent = new Consent(
    id: $"{scope}_{name}_{tenantIds[0]}",
    tenantId: tenantIds[0].ToString(),
    subscriptionId: subscriptionIds[0].ToString(),
    name: name,
    scope: scope,
    status: true
);

// Build the full partition key path
PartitionKey partitionKey = new PartitionKeyBuilder()
    .Add(newConsent.tenantId) 
    .Add(newConsent.subscriptionId)
    .Build();

Consent createdConsentItem = await container.CreateItemAsync<Consent>(
    item: newConsent,
    partitionKey: partitionKey
);
Console.WriteLine($"Created item:\t{createdConsentItem.id}\t[{createdConsentItem.tenantId}]\t[{createdConsentItem.subscriptionId}]\n\n");

// Point read item from container using the id and partitionKey
timer.Start();
var readItem = await container.ReadItemAsync<Consent>(
    id: newConsent.id,
    partitionKey: partitionKey
);
Console.WriteLine($"Request Charge for Point Read WITH Hierarchical Partition Key\t{readItem.RequestCharge}");
Console.WriteLine($"Time Elapsed(ms):\t{timer.ElapsedMilliseconds}\n\n");


Random rand = new Random();
List<double> readRequestCharges = new List<double>();
timer.Restart();
for (int i = 0; i < 100; i++)
{
    string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    string newName = new string(Enumerable.Repeat(chars, 8)
        .Select(s => s[rand.Next(8)]).ToArray());

    int index = rand.Next(0, tenantIds.Count);
    string tenantId = tenantIds[index].ToString();
    string subscriptionId = subscriptionIds[index].ToString();
    string newScope = $"_subscriptions_{subscriptionId}";

    Consent consent = new Consent(
        id: $"{newScope}_{newName}_{tenantId}",
        tenantId: tenantId.ToString(),
        subscriptionId: subscriptionId.ToString(),
        name: newName,
        scope: newScope,
        status: true
    );

    PartitionKey newPartitionKey = new PartitionKeyBuilder()
        .Add(consent.tenantId)
        .Add(consent.subscriptionId)
        .Build();
    
    var newlyCreatedConsentItem = await container.CreateItemAsync<Consent>(
        item: consent,
        partitionKey: newPartitionKey
    );
  
    var readRequest = await container.ReadItemAsync<Consent>(
        id: newlyCreatedConsentItem.Resource.id,
        partitionKey: newPartitionKey
    );
    readRequestCharges.Add(readRequest.RequestCharge);
}
Console.WriteLine($"Created and read 1000 items. Time Elapsed(ms):\t{timer.ElapsedMilliseconds}");
Console.WriteLine($"Average read request charge:\t{readRequestCharges.Average()}\n\n");




// Create query using a SQL string to do a cross partition read
var query = new QueryDefinition(
    query: "SELECT * FROM p WHERE p.name = @name"
).WithParameter("@name", "default");

timer.Restart();
using FeedIterator<Consent> feed = container.GetItemQueryIterator<Consent>(
    queryDefinition: query
);

while (feed.HasMoreResults)
{
    FeedResponse<Consent> response = await feed.ReadNextAsync();
    foreach (Consent item in response)
    {
        Console.WriteLine($"Found item:\t{item.name}");
    }
    Console.WriteLine($"Request Charge for query WITHOUT Hierarchical Partition Key\t{response.RequestCharge}");
    Console.WriteLine($"Time Elapsed(ms):\t{timer.ElapsedMilliseconds}\n\n");
}




// Create query using a single level partition read
query = new QueryDefinition(
    query: "SELECT * FROM p WHERE p.tenantId = @tenantId"
).WithParameter("@tenantId", tenantIds[0]);

timer.Restart();
using FeedIterator<Consent> feed2 = container.GetItemQueryIterator<Consent>(
    queryDefinition: query
);

while (feed2.HasMoreResults)
{
    FeedResponse<Consent> response = await feed2.ReadNextAsync();
    foreach (Consent item in response)
    {
        Console.WriteLine($"Found item:\t{item.name}");
    }
    Console.WriteLine($"Request Charge for query WITH ONE level of Hierarchical Partition Key\t{response.RequestCharge}");
    Console.WriteLine($"Time Elapsed(ms):\t{timer.ElapsedMilliseconds}\n\n");
}



// Create query using a double level partition read
query = new QueryDefinition(
    query: "SELECT * FROM p WHERE p.tenantId = @tenantId AND p.subscriptionId = @subscriptionId"
).WithParameter("@tenantId", tenantIds[0])
.WithParameter("@subscriptionId", subscriptionIds[0]);

timer.Restart();
using FeedIterator<Consent> feed3 = container.GetItemQueryIterator<Consent>(
    queryDefinition: query
);

while (feed3.HasMoreResults)
{
    FeedResponse<Consent> response = await feed3.ReadNextAsync();
    foreach (Consent item in response)
    {
        Console.WriteLine($"Found item:\t{item.name}");
    }
    Console.WriteLine($"Request Charge for query WITH TWO level of Hierarchical Partition Key\t{response.RequestCharge}");
    Console.WriteLine($"Time Elapsed(ms):\t{timer.ElapsedMilliseconds}\n\n");
}


timer.Stop();