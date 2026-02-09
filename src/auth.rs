use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::header::{AUTHORIZATION, HeaderValue};
use tower::Service;

/// User-implemented token source for authentication.
///
/// Provides bearer tokens for gRPC requests. Users bring their own
/// implementation — no coupling to any specific auth crate.
pub trait TokenSource: Send + Sync + 'static {
    fn token(
        &self,
    ) -> impl Future<Output = Result<String, Box<dyn std::error::Error + Send + Sync>>> + Send;
}

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Tower service layer that injects `Authorization: Bearer <token>` into requests.
#[derive(Clone)]
pub(crate) struct AuthService<S> {
    inner: S,
    token_source: Arc<dyn TokenSourceDyn>,
}

/// Object-safe wrapper for TokenSource.
trait TokenSourceDyn: Send + Sync {
    fn token_boxed(&self) -> BoxFuture<'_, Result<String, BoxError>>;
}

impl<T: TokenSource> TokenSourceDyn for T {
    fn token_boxed(&self) -> BoxFuture<'_, Result<String, BoxError>> {
        Box::pin(self.token())
    }
}

impl<S> AuthService<S> {
    pub(crate) fn new<T: TokenSource>(inner: S, token_source: T) -> Self {
        Self {
            inner,
            token_source: Arc::new(token_source),
        }
    }
}

impl<S, Body> Service<http::Request<Body>> for AuthService<S>
where
    S: Service<http::Request<Body>> + Clone + Send + 'static,
    S::Future: Send,
    S::Error: std::error::Error + Send + Sync + 'static,
    Body: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner
            .poll_ready(cx)
            .map_err(|e| Box::new(e) as BoxError)
    }

    fn call(&mut self, mut req: http::Request<Body>) -> Self::Future {
        let token_source = self.token_source.clone();
        let mut inner = self.inner.clone();
        // Swap so the clone (which is ready) is used for this call
        std::mem::swap(&mut self.inner, &mut inner);

        Box::pin(async move {
            let token = token_source.token_boxed().await?;
            let val = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|e| Box::new(e) as BoxError)?;
            req.headers_mut().insert(AUTHORIZATION, val);
            inner.call(req).await.map_err(|e| Box::new(e) as BoxError)
        })
    }
}
