

def fragment(context, frag_spec, frag_args):
    '''A fragment
    context: object that encapsulates request, response, pristine versions, and some temp space
    frag_spec: some metadata that defines what the frag_args might be (lookasides, types, etc.)
    frag_args: arguments
    '''
    try:
        context.setattr_dotted(frag_args['attribute'], frag_args['value'])
        return True
    except:
        import logging
        logging.error('foo', exc_info=True)
        return False
