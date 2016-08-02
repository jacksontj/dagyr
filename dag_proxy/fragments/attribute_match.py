

def fragment(context, frag_args):
    '''A fragment
    context: object that encapsulates request, response, pristine versions, and some temp space
    frag_args: arguments
    '''
    # TODO: global cache of converted datastructures (trie etc.)
    # check if the item we want is in the value list

    # convert to a trie (for more scaleable lookups)
    values_trie = context.dag_config.convert_item('trie', tuple(frag_args['values']))
    return context.getattr_dotted(frag_args['attribute']) in values_trie
