# dag_proxy
Sketch of what a proxy where all configuration was DAGs

# parts
effectively we want a DAG runner, which has in itself a DAG, config, etc. which
transactions end up stepping through (in their own object or w/e for tracking).
The configs etc should be encapsulated in the runner, so that when a new config
is loaded, we don't have to deal with pointers moving-- new transactions will
simply be scheduled on the new DAG runner

# control DAG
This DAG is responsible for the overall flow of request handling. This defines which
other DAGs to run, as well as how to handle error conditions.

# dynamic_dags
This is a map of namespace -> key -> DAG. The intent here is to allow the user
to create an arbitrary number of layers (objects that need to be matched) that
you can associate DAGs with

# TODO/Questions
    - listen options (certs, ports, etc.) -- setup DAG, run once per config load
    - what to do with invalid return type (how to handle the error)-- abort DAG?
    - where does the namespace start (state. or request.)
        -- change from `state` to `context` and state can be one of the values
         this way things can access "config" (which they aren't supposed to mutate)
         to allow for lookaside tables etc.
    - data access layer thingy-- for caching data as trie, etc.
    - comments on nodes

# notes:
    - fragments can have default values
