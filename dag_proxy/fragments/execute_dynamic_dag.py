

def fragment(context, frag_args):
    '''A fragment
    context: object that encapsulates request, response, pristine versions, and some temp space
    frag_args: arguments
    '''
    key = '{0}_{1}'.format(
        frag_args['dag_prefix'],
        context.getattr_dotted(frag_args['suffix_key']),
    )
    context.setattr_dotted('next_dag', key)
