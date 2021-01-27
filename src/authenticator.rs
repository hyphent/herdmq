use crate::types::ClientCredential;

pub struct Authenticator<C, S, P>
where 
C: Fn(&ClientCredential) -> bool + Sync + Send + 'static, 
S: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static,
P: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static
{
  pub connection: C,
  pub subscribe: S,
  pub publish: P
}

impl <C, S, P> Authenticator<C, S, P>
where 
C: Fn(&ClientCredential) -> bool + Sync + Send + 'static, 
S: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static,
P: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static
{
  pub fn new(connection: C, subscribe: S, publish: P) -> Authenticator<C, S, P> {
    Authenticator {
      connection,
      subscribe,
      publish
    }
  }
}
