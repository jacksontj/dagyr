

def fragment(ctx, frag_spec, frag_args):
    '''A fragment
    ctx: object that encapsulates request, response, pristine versions, and some temp space
    frag_spec: some metadata that defines what the frag_args might be (lookasides, types, etc.)
    frag_args: arguments
    '''
    # TODO: global cache of converted datastructures (trie etc.)
    # check if the item we want is in the value list

    # TODO: this expansion really belongs up a few layers-- since it only needs
    # to happen once, and ideally we'd resolve all these before marking the
    # config as good-- as it may not link/resolve
    if frag_spec['values'].get('option_data'):
        return ctx.getattr_dotted(frag_args['attribute']) in ctx.options[frag_args['values']]
    else:
        return ctx.getattr_dotted(frag_args['attribute']) in frag_args['values']
