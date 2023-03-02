// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

// C# record representing an item in the container
public record Consent(
    string id,
    string tenantId,
    string subscriptionId,
    string name,
    string scope,
    bool status
);
