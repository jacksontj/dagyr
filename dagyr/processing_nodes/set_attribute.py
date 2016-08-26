'''Set `attribute` (in context) to `value`
'''
import logging

def processing_node(context, arg_spec, raw_args, resolved_args):
    try:
        context.setattr_dotted(resolved_args['attribute'], resolved_args['value'])
        return True
    except:
        logging.error('Unable to set attribute', exc_info=True)
        return False
