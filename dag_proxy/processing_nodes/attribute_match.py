

def processing_node(context, arg_spec, raw_args, resolved_args):
    '''A processing_node
    context: object that encapsulates request, response, pristine versions, and some temp space
    arg_spec: the specification for what each argument is (including name, type, etc.)
    raw_args: raw arguments from configuration, not resolved through global_option_data
    resolved_args: arguments after being resolved through global_option_data (if applicable)
    '''
    # check if the item we want is in the value list

    # convert to a trie (for more scaleable lookups)
    values_trie = context.dag_config.convert_item('trie', tuple(resolved_args['values']))
    return context.getattr_dotted(resolved_args['attribute']) in values_trie
