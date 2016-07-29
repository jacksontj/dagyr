

def fragment(ctx, frag_spec, frag_args):
    '''A fragment
    ctx: object that encapsulates request, response, pristine versions, and some temp space
    frag_spec: some metadata that defines what the frag_args might be (lookasides, types, etc.)
    frag_args: arguments
    '''
    key = '{0}_{1}'.format(
        frag_args['dag_prefix'],
        ctx.getattr_dotted(frag_args['suffix_key']),
    )
    ctx.setattr_dotted('state.next_dag', key)
