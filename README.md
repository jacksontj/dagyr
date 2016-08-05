# dagyr
A library for running DAGs against a context.

The motivation here is to create a flexible, pluggable library for DAGs execution.


# dag_proxy
Sketch of what a proxy based on dagyr

## High-Level Operation
The DAG execution is allowed to modify the request/response in hooks
(ingress, egress, etc.) which will then be used by the proxy.
The basic request flow is:

```
accept -> create state -> ingress hook -> make downstream request -> egress hook -> serve response
```

## RequestState
This object encapsulates the http request/response state. This is what the
underlying proxy uses to determine how to handle the request. This contains 4
objects: pristine_request, request, pristine_response, response. "pristine" meaning
untouched-- specifically that we don't allow modification of these entries as
processing_nodes may want to switch based off of the original request/response
before mutation.

### RequestState object format
Note: this is still a rough skeleton, so this will change dramatically

These objects are really just dicts of data. The data today looks something like:
```
    {
        'code': 200,
        'headers': {'X-Foo': 'bar'},
        'body': 'the body!',
    }
```

These objects are used by the underlying proxy to represent the transaction at
the various phases. This will be expanded to include transaction_overrideable
configuration (timeouts, lb options, etc.).
