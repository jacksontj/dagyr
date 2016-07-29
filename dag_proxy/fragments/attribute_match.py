

def fragment(ctx, frag_spec, frag_args):
    '''A fragment
    ctx: object that encapsulates request, response, pristine versions, and some temp space
    frag_spec: some metadata that defines what the frag_args might be (lookasides, types, etc.)
    frag_args: arguments
    '''
    # TODO: global cache of converted datastructures (trie etc.)
    # check if the item we want is in the value list
    return ctx.getattr_dotted(frag_args['attribute']) in frag_args['values']
