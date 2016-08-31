'''An example to document what all the parts of a processing_node

A processing node is the base for the "nodes" in a DAG. Processing nodes are called
with context and a set of arguments to return a result. That result will be used
to traverse the DAG
'''

def example_function_type(context, arg_spec, raw_args, resolved_args):
    '''A processing_node

    Arguments:
        context: object that encapsulates context (this is Dagyr context as well as user-context)
        arg_spec: the specification for what each argument is (including name, type, etc.)
        raw_args: raw arguments from configuration, not resolved through global_option_data
        resolved_args: arguments after being resolved through global_option_data (if applicable)
    '''
    # do something with the input here
    # for example, you could get something from context using dotted notation:
    #   context.getattr_dotted(resolved_args['attribute'])
    #
    # or do some data type conversions
    #   values_trie = context.dag_config.convert_item('trie', tuple(resolved_args['values']))

    # regardless, just ensure to return something
    return True


# spec of what this processing_function does
processing_function_type = {
    'name': 'name of this processing_function_type',
    'info': 'an info string about what this might do',
    'func': example_function_type,
    'arg_spec': {
        'argA': {
            'name': 'longer name for argA',
            'info': 'an info string about what argA is',
            'type': 'string',  # the type of argA
        },
    },
}
