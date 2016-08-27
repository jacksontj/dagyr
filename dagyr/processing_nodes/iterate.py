'''An example to repeatedly call a processing_node up to N times waiting for a specific return
'''

def processing_node(context, arg_spec, raw_args, resolved_args):
    '''A processing_node

    Arguments:
        context: object that encapsulates context (this is Dagyr context as well as user-context)
        arg_spec: the specification for what each argument is (including name, type, etc.)
        raw_args: raw arguments from configuration, not resolved through global_option_data
        resolved_args: arguments after being resolved through global_option_data (if applicable)
    '''
    for x in xrange(0, raw_args['iterate_limit']):
        # TODO: less messy... this is a bit convoluted
        r = context.dag_config.processing_node_funcs[raw_args['processing_node_name']](context, {}, {} ,{})
        if r == raw_args['expected_value']:
            return True
    return False
