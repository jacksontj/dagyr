'''Determine if a given 'attribute' is in 'values'
'''

def attribute_match(context, arg_spec, raw_args, resolved_args):
    # convert to a trie (for more scaleable lookups)
    values_trie = context.dag_config.convert_item('trie', tuple(resolved_args['values']))
    return context.getattr_dotted(resolved_args['attribute']) in values_trie

# spec of what this processing_function does
processing_function_type = {
    'name': 'attribute_match',
    'info': 'match an attribute against a set of values',
    'info_url': 'http://someplace.com',
    # this node_type has the effect of setting a "domain DAG name"
    'func': attribute_match,
    # this is what the args are, and potentially how to resolve them (look it up)
    # TODO: support optional arguments, default values, etc.
    'arg_spec': {
        # this is a thing that will be passed a key to lookup the value
        'attribute': {
            'name': 'a readable name',
            'info': 'name of the attribute to match',
            # TODO: allow defining multiple types?
            'type': 'string',
        },
        # this just gets a value-- just use it
        'values': {
            'name': 'a readable name',
            'info': 'list of values to match against',
            'type': 'list',  # TODO: define type of values?
        },
    },
}
