'''Determine if a given 'attribute' is in 'values'
'''


def processing_node(context, arg_spec, raw_args, resolved_args):
    # convert to a trie (for more scaleable lookups)
    values_trie = context.dag_config.convert_item('trie', tuple(resolved_args['values']))
    return context.getattr_dotted(resolved_args['attribute']) in values_trie
