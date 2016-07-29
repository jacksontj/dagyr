

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
    values = None
    if frag_spec['values'].get('option_data'):
        values = ctx.options[frag_args['values']]
    else:
        values = frag_args['values']

    # convert to a trie (for more scaleable lookups)
    values_trie = ctx.convert_item('trie', tuple(values))
    return ctx.getattr_dotted(frag_args['attribute']) in values_trie
