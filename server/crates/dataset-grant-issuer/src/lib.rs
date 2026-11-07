//! Minting of signed dataset grants: the application-backend side of the
//! grants `dataset-grant` verifies.
//!
//! A separate crate, so a server build that links only the verifier contains no
//! code that can mint a grant.

use std::collections::BTreeMap;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use dataset_grant::{
    encode_grant_token, fence_order_signing_input, grant_signing_input, tenant_order_signing_input,
    DatasetGrantClaims, FenceOrderClaims, GrantAttributeValue, GrantCommandCapability,
    GrantVerificationKey, TenantAction, TenantOrderClaims, FENCE_ORDER_VERSION, GRANT_VERSION,
    MAX_FENCE_ORDER_LIFETIME_SECONDS, MAX_GRANT_LIFETIME_SECONDS,
    MAX_TENANT_ORDER_LIFETIME_SECONDS, TENANT_ORDER_VERSION,
};
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use sync_protocol::{change::RevocationBarrier, revocation_barrier_signing_preimage};
use thiserror::Error;

/// An Ed25519 signing key together with the id verifiers know it by.
pub struct GrantIssuer {
    key_id: String,
    audience: String,
    key_pair: Ed25519KeyPair,
}

impl GrantIssuer {
    /// Loads an issuer from a PKCS#8 v2 Ed25519 document.
    pub fn from_pkcs8(
        key_id: impl Into<String>,
        audience: impl Into<String>,
        pkcs8: &[u8],
    ) -> Result<Self, IssuerError> {
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8).map_err(|_| IssuerError::InvalidKey)?;
        Ok(Self {
            key_id: key_id.into(),
            audience: audience.into(),
            key_pair,
        })
    }

    /// Generates a fresh issuer, returning it with its PKCS#8 document: the
    /// private key, which the caller decides where to store.
    pub fn generate(
        key_id: impl Into<String>,
        audience: impl Into<String>,
    ) -> Result<(Self, Vec<u8>), IssuerError> {
        let random = SystemRandom::new();
        let document =
            Ed25519KeyPair::generate_pkcs8(&random).map_err(|_| IssuerError::KeyGeneration)?;
        let pkcs8 = document.as_ref().to_vec();
        let issuer = Self::from_pkcs8(key_id, audience, &pkcs8)?;
        Ok((issuer, pkcs8))
    }

    /// The verification key a deployment should be configured with.
    pub fn verification_key(&self) -> Result<GrantVerificationKey, IssuerError> {
        let public = self.key_pair.public_key().as_ref();
        let bytes: [u8; 32] = public.try_into().map_err(|_| IssuerError::InvalidKey)?;
        GrantVerificationKey::new(self.key_id.clone(), bytes).map_err(IssuerError::Key)
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// The document a Calque deployment's `--grant-verification-key-env`
    /// name must hold for this issuer's grants to verify. It holds only the
    /// public key.
    pub fn verification_key_environment(&self) -> Result<String, IssuerError> {
        let public: [u8; 32] = self
            .key_pair
            .public_key()
            .as_ref()
            .try_into()
            .map_err(|_| IssuerError::InvalidKey)?;
        serde_json::to_string(&VerificationKeyEnvironment {
            key_id: self.key_id.clone(),
            public_key: URL_SAFE_NO_PAD.encode(public),
        })
        .map_err(|_| IssuerError::Serialization)
    }

    /// Mints a read-only grant valid for `lifetime_seconds` from
    /// `issued_at_unix_seconds`. It carries no command capability, so it
    /// authorizes no write.
    pub fn issue(
        &self,
        subject: impl Into<String>,
        attributes: BTreeMap<String, GrantAttributeValue>,
        authorization_generation: u64,
        issued_at_unix_seconds: u64,
        lifetime_seconds: u64,
    ) -> Result<String, IssuerError> {
        self.issue_with_commands(
            subject,
            attributes,
            Vec::new(),
            authorization_generation,
            issued_at_unix_seconds,
            lifetime_seconds,
        )
    }

    /// Mints a grant that also carries an explicit command capability.
    ///
    /// `commands` is checked for the shapes every verifier refuses, so the
    /// issuer cannot hand out a capability that nothing will honour.
    pub fn issue_with_commands(
        &self,
        subject: impl Into<String>,
        attributes: BTreeMap<String, GrantAttributeValue>,
        commands: Vec<GrantCommandCapability>,
        authorization_generation: u64,
        issued_at_unix_seconds: u64,
        lifetime_seconds: u64,
    ) -> Result<String, IssuerError> {
        if lifetime_seconds == 0 || lifetime_seconds > MAX_GRANT_LIFETIME_SECONDS {
            // Refusing here rather than at presentation keeps the issuer from
            // handing out a grant that every verifier would reject.
            return Err(IssuerError::LifetimeOutOfRange);
        }
        let mut named = std::collections::BTreeSet::new();
        for capability in &commands {
            let mut operations = std::collections::BTreeSet::new();
            if capability.collection.is_empty()
                || capability.operations.is_empty()
                || !capability
                    .operations
                    .iter()
                    .all(|operation| operations.insert(*operation))
                || !named.insert(capability.collection.as_str())
            {
                return Err(IssuerError::UnusableCommandCapability);
            }
        }
        let claims = DatasetGrantClaims {
            version: GRANT_VERSION,
            key_id: self.key_id.clone(),
            audience: self.audience.clone(),
            subject: subject.into(),
            attributes,
            authorization_generation,
            issued_at_unix_seconds,
            expires_at_unix_seconds: issued_at_unix_seconds
                .checked_add(lifetime_seconds)
                .ok_or(IssuerError::LifetimeOutOfRange)?,
            commands,
        };
        self.issue_claims(&claims)
    }

    /// Signs exactly the claims given, without supplying or checking any of
    /// them, so the verifier's negative vectors can be validly signed.
    pub fn issue_claims(&self, claims: &DatasetGrantClaims) -> Result<String, IssuerError> {
        let payload = serde_json::to_vec(claims).map_err(|_| IssuerError::Serialization)?;
        let signature = self.key_pair.sign(&grant_signing_input(&payload));
        Ok(encode_grant_token(&payload, signature.as_ref()))
    }

    /// Orders the deployment to retire `revoked_generation` and put
    /// `effective_generation` in force, valid for `lifetime_seconds` from
    /// `issued_at_unix_seconds`.
    pub fn issue_fence_order(
        &self,
        revoked_generation: u64,
        effective_generation: u64,
        issued_at_unix_seconds: u64,
        lifetime_seconds: u64,
    ) -> Result<String, IssuerError> {
        if lifetime_seconds == 0 || lifetime_seconds > MAX_FENCE_ORDER_LIFETIME_SECONDS {
            return Err(IssuerError::LifetimeOutOfRange);
        }
        if effective_generation <= revoked_generation {
            return Err(IssuerError::GenerationNotAdvanced);
        }
        self.issue_fence_order_claims(&FenceOrderClaims {
            version: FENCE_ORDER_VERSION,
            key_id: self.key_id.clone(),
            audience: self.audience.clone(),
            revoked_generation,
            effective_generation,
            issued_at_unix_seconds,
            expires_at_unix_seconds: issued_at_unix_seconds
                .checked_add(lifetime_seconds)
                .ok_or(IssuerError::LifetimeOutOfRange)?,
        })
    }

    /// Signs exactly the order given, for the verifier's negative vectors;
    /// see [`Self::issue_claims`].
    pub fn issue_fence_order_claims(
        &self,
        claims: &FenceOrderClaims,
    ) -> Result<String, IssuerError> {
        let payload = serde_json::to_vec(claims).map_err(|_| IssuerError::Serialization)?;
        let signature = self.key_pair.sign(&fence_order_signing_input(&payload));
        Ok(encode_grant_token(&payload, signature.as_ref()))
    }

    /// Orders the deployment to `action` one tenant, valid for
    /// `lifetime_seconds` from `issued_at_unix_seconds`, under an id of 16
    /// random bytes the deployment acts on once.
    pub fn issue_tenant_order(
        &self,
        action: TenantAction,
        tenant: GrantAttributeValue,
        issued_at_unix_seconds: u64,
        lifetime_seconds: u64,
    ) -> Result<String, IssuerError> {
        if lifetime_seconds == 0 || lifetime_seconds > MAX_TENANT_ORDER_LIFETIME_SECONDS {
            return Err(IssuerError::LifetimeOutOfRange);
        }
        let mut id = [0_u8; 16];
        ring::rand::SecureRandom::fill(&SystemRandom::new(), &mut id)
            .map_err(|_| IssuerError::Randomness)?;
        self.issue_tenant_order_claims(&TenantOrderClaims {
            version: TENANT_ORDER_VERSION,
            key_id: self.key_id.clone(),
            audience: self.audience.clone(),
            order_id: URL_SAFE_NO_PAD.encode(id),
            action,
            tenant,
            issued_at_unix_seconds,
            expires_at_unix_seconds: issued_at_unix_seconds
                .checked_add(lifetime_seconds)
                .ok_or(IssuerError::LifetimeOutOfRange)?,
        })
    }

    /// Signs exactly the order given, for the verifier's negative vectors;
    /// see [`Self::issue_claims`].
    pub fn issue_tenant_order_claims(
        &self,
        claims: &TenantOrderClaims,
    ) -> Result<String, IssuerError> {
        let payload = serde_json::to_vec(claims).map_err(|_| IssuerError::Serialization)?;
        let signature = self.key_pair.sign(&tenant_order_signing_input(&payload));
        Ok(encode_grant_token(&payload, signature.as_ref()))
    }

    /// Signs one revocation barrier Calque drafted, stamping this issuer's key
    /// id into it first, because the key id is part of what is signed.
    pub fn sign_revocation_barrier(
        &self,
        barrier: &mut RevocationBarrier,
    ) -> Result<(), IssuerError> {
        barrier.signing_key_id = self.key_id.clone();
        let preimage =
            revocation_barrier_signing_preimage(barrier).map_err(|_| IssuerError::Serialization)?;
        barrier.signature = self.key_pair.sign(&preimage).as_ref().to_vec();
        Ok(())
    }
}

impl std::fmt::Debug for GrantIssuer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GrantIssuer")
            .field("key_id", &self.key_id)
            .field("audience", &self.audience)
            .finish_non_exhaustive()
    }
}

/// The shape `calque serve` parses out of its grant-verification-key
/// environment variable.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct VerificationKeyEnvironment {
    key_id: String,
    public_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IssuerError {
    #[error("issuer key is not a usable PKCS#8 Ed25519 key")]
    InvalidKey,
    #[error("issuer key generation failed")]
    KeyGeneration,
    #[error("the system's random source failed")]
    Randomness,
    #[error("grant lifetime must be positive and within the maximum grant lifetime")]
    LifetimeOutOfRange,
    #[error("fence order must move to a later generation than the one it retires")]
    GenerationNotAdvanced,
    #[error("grant command capability names no collection, no operation, or one of them twice")]
    UnusableCommandCapability,
    #[error("grant claims could not be serialized")]
    Serialization,
    #[error("issuer verification key is unusable: {0}")]
    Key(#[source] dataset_grant::GrantKeyError),
}
