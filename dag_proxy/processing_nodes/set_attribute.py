

def processing_node(context, arg_spec, raw_args, resolved_args):
    '''A processing_node
    context: object that encapsulates request, response, pristine versions, and some temp space
    arg_spec: the specification for what each argument is (including name, type, etc.)
    raw_args: raw arguments from configuration, not resolved through global_option_data
    resolved_args: arguments after being resolved through global_option_data (if applicable)
    '''
    try:
        context.setattr_dotted(resolved_args['attribute'], resolved_args['value'])
        return True
    except:
        import logging
        logging.error('foo', exc_info=True)
        return False
