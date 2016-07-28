

def fragment(request_state, frag_spec, frag_args):
    '''A fragment
    request_state: object that encapsulates request, response, pristine versions, and some temp space
    frag_spec: some metadata that defines what the frag_args might be (lookasides, types, etc.)
    frag_args: arguments
    '''
    attr_parts = frag_args['attribute'].split('.')

    # otherwise we'll assume all is well
    thing = request_state
    # get to the correc thing, so we can do appropriate sets
    for p in attr_parts[:-1]:
        try:
            thing = getattr(thing, p)
        except AttributeError:
            thing = thing[p]

    # TODO: global cache of converted datastructures (trie etc.)
    # check if the item we want is in the value list
    return thing[attr_parts[-1]] in frag_args['values']
