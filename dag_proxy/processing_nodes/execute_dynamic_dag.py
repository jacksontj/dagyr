

def processing_node(context, arg_spec, raw_args, resolved_args):
    '''A processing_node
    context: object that encapsulates request, response, pristine versions, and some temp space
    arg_spec: the specification for what each argument is (including name, type, etc.)
    raw_args: raw arguments from configuration, not resolved through global_option_data
    resolved_args: arguments after being resolved through global_option_data (if applicable)
    '''
    key = '{0}_{1}'.format(
        resolved_args['dag_prefix'],
        context.getattr_dotted(resolved_args['suffix_key']),
    )
    context.setattr_dotted('next_dag', key)
