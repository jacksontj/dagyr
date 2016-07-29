

def fragment(ctx, frag_spec, frag_args):
    '''A fragment
    ctx: object that encapsulates request, response, pristine versions, and some temp space
    frag_spec: some metadata that defines what the frag_args might be (lookasides, types, etc.)
    frag_args: arguments
    '''
    ctx.state.next_dag = (
        frag_args['dynamic_dag_namespace'],
        ctx.getattr_dotted(frag_args['dynamic_dag_key'])
    )
