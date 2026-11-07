//! How a request proves who it is, and the principal it resolves to.

use super::*;

/// How a request proves who it is. Either way, each request resolves to a
/// verified [`Principal`].
pub enum LocalCredentialPolicy {
    /// One shared secret standing in for every caller, mapped to one fixed
    /// principal. Only the local development harness constructs it.
    DevelopmentSharedBearer {
        token: [u8; 32],
        principal: Principal,
    },
    /// Signed dataset grants: the application backend mints, this process only
    /// verifies, and each request carries its own identity.
    DatasetGrant(Box<GrantVerifier>),
}

impl fmt::Debug for LocalCredentialPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DevelopmentSharedBearer { principal, .. } => formatter
                .debug_struct("DevelopmentSharedBearer")
                .field("subject", &principal.subject())
                .finish_non_exhaustive(),
            Self::DatasetGrant(verifier) => formatter
                .debug_tuple("DatasetGrant")
                .field(verifier)
                .finish(),
        }
    }
}

impl LocalCredentialPolicy {
    /// Turns a presented credential into the principal it names.
    ///
    /// `authorization_generation` is the policy's current generation, passed in
    /// rather than remembered, so a grant minted against a superseded policy is
    /// refused the moment it is presented.
    pub(super) fn resolve(
        &self,
        presented: &str,
        authorization_generation: u64,
    ) -> Result<ResolvedCredential, BootstrapHttpError> {
        let now = now_unix_seconds().map_err(|_| BootstrapHttpError::Unavailable)?;
        self.resolve_at(presented, authorization_generation, now)
    }

    pub(super) fn resolve_at(
        &self,
        presented: &str,
        authorization_generation: u64,
        now_unix_seconds: u64,
    ) -> Result<ResolvedCredential, BootstrapHttpError> {
        match self {
            Self::DevelopmentSharedBearer { token, principal } => {
                if !valid_bearer(presented, token) {
                    return Err(BootstrapHttpError::Unauthorized);
                }
                Ok(ResolvedCredential {
                    principal: principal.clone(),
                    expires_at_unix_seconds: None,
                    // A shared secret carries no signed capability. What the
                    // development harness may write is decided by its
                    // `MutationCommandPolicy`, never by this credential.
                    commands: GrantCommandCapabilities::default(),
                })
            }
            Self::DatasetGrant(verifier) => {
                let verified = verifier
                    .verify(&GrantPresentation {
                        token: presented,
                        now_unix_seconds,
                        authorization_generation,
                    })
                    // Every refusal is Unauthorized. Distinguishing expiry from
                    // a bad signature here would tell a caller which half of a
                    // guess was right, so only the server's log names the reason.
                    .map_err(|error| {
                        tracing::info!(%error, "a dataset grant was refused");
                        BootstrapHttpError::Unauthorized
                    })?;
                let (principal, expires_at_unix_seconds, commands) = verified.into_parts();
                Ok(ResolvedCredential {
                    principal,
                    expires_at_unix_seconds: Some(expires_at_unix_seconds),
                    commands,
                })
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct ResolvedCredential {
    pub(super) principal: Principal,
    pub(super) expires_at_unix_seconds: Option<u64>,
    /// The authenticated command capability the grant carried, empty for every
    /// grant that carried none. It is never consulted by a read path.
    pub(super) commands: GrantCommandCapabilities,
}

impl ResolvedCredential {
    pub(super) fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Leaves one second between the capability deadline computed on the
    /// verifier's whole-second clock and the grant's exclusive expiry. If
    /// scheduling consumes that margin, bootstrap-api's authorization-expiry
    /// guard rejects the plan instead of extending it.
    pub(super) fn capability_ttl(
        &self,
        configured: Duration,
        now_unix_seconds: u64,
    ) -> Option<Duration> {
        match self.expires_at_unix_seconds {
            None => Some(configured),
            Some(expires_at) => {
                let bounded_seconds = expires_at.checked_sub(now_unix_seconds)?.checked_sub(1)?;
                (bounded_seconds != 0).then(|| configured.min(Duration::from_secs(bounded_seconds)))
            }
        }
    }

    pub(super) fn authorization_expiry(
        &self,
        now: OffsetDateTime,
        configured_ttl: Duration,
    ) -> Result<OffsetDateTime, BootstrapHttpError> {
        match self.expires_at_unix_seconds {
            Some(expires_at) => OffsetDateTime::from_unix_timestamp(
                i64::try_from(expires_at).map_err(|_| BootstrapHttpError::Unavailable)?,
            )
            .map_err(|_| BootstrapHttpError::Unavailable),
            None => Ok(now
                + time::Duration::try_from(configured_ttl)
                    .map_err(|_| BootstrapHttpError::Unavailable)?
                + time::Duration::seconds(1)),
        }
    }
}

impl std::ops::Deref for ResolvedCredential {
    type Target = Principal;

    fn deref(&self) -> &Self::Target {
        self.principal()
    }
}

pub(super) fn valid_bearer(value: &str, expected: &[u8; 32]) -> bool {
    SecretToken::from_urlsafe(value)
        .map(|token| bool::from(token.as_bytes().as_slice().ct_eq(expected.as_slice())))
        .unwrap_or(false)
}
