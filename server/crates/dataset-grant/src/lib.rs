//! Verification of short-lived signed dataset grants.
//!
//! The application backend authenticates its principals and mints a grant;
//! Calque only verifies one. A grant carries an identity and, if the issuer put
//! one there, a command capability. It cannot name a table, column, row
//! predicate or scope: the read projection comes from
//! [`authz::AuthorizationPolicy`], so a grant cannot ask for more data than the
//! policy gives its principal. A read grant never implies write permission.
//!
//! # Framing
//!
//! `base64url(payload).base64url(signature)`, unpadded. The signature covers
//! [`GRANT_SIGNING_DOMAIN`] followed by the exact payload bytes, so a grant
//! stays safe even if its key signs something else. There is no algorithm
//! field: Ed25519 is chosen by this code rather than announced by the token, so
//! algorithm confusion has nowhere to enter.

use std::collections::{BTreeMap, BTreeSet};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use ring::signature::{UnparsedPublicKey, ED25519};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use authz::{Principal, PrincipalResolver, PrincipalValue};

mod fence;
mod tenant_order;

pub use fence::{
    fence_order_signing_input, FenceOrderClaims, FenceOrderError, VerifiedFenceOrder,
    FENCE_ORDER_SIGNING_DOMAIN, FENCE_ORDER_VERSION, MAX_FENCE_ORDER_LIFETIME_SECONDS,
};
pub use tenant_order::{
    tenant_order_signing_input, TenantAction, TenantOrderClaims, TenantOrderError,
    VerifiedTenantOrder, MAX_TENANT_ORDER_LIFETIME_SECONDS, TENANT_ORDER_SIGNING_DOMAIN,
    TENANT_ORDER_VERSION,
};

/// Domain separator prepended to the payload before signing and verifying.
pub const GRANT_SIGNING_DOMAIN: &[u8] = b"calque.dataset-grant.v1\0";

/// The only payload version this build accepts.
pub const GRANT_VERSION: u32 = 1;

/// Ed25519 public keys are 32 bytes.
const ED25519_PUBLIC_KEY_BYTES: usize = 32;

/// Bounds on untrusted input, applied before any allocation-heavy work.
const MAX_TOKEN_BYTES: usize = 8 * 1024;
const MAX_PAYLOAD_BYTES: usize = 4 * 1024;
const MAX_KEY_ID_BYTES: usize = 128;
const MAX_AUDIENCE_BYTES: usize = 256;
const MAX_GRANT_ATTRIBUTES: usize = 64;
/// Bounds on the command capability, applied to authenticated claims.
const MAX_GRANT_COMMAND_CAPABILITIES: usize = 64;
const MAX_GRANT_COLLECTION_BYTES: usize = 128;

/// Upper bound on a grant's validity window.
///
/// Expiry bounds how long a disconnected client keeps stale access, so a
/// distant expiry weakens revocation. A longer window is rejected rather than
/// truncated, so an issuer never believes a long-lived grant was honoured.
pub const MAX_GRANT_LIFETIME_SECONDS: u64 = 15 * 60;

/// A verification key and the identifier the issuer stamps into its grants.
#[derive(Clone)]
pub struct GrantVerificationKey {
    key_id: String,
    public_key: [u8; ED25519_PUBLIC_KEY_BYTES],
}

impl GrantVerificationKey {
    pub fn new(
        key_id: impl Into<String>,
        public_key: [u8; ED25519_PUBLIC_KEY_BYTES],
    ) -> Result<Self, GrantKeyError> {
        let key_id = key_id.into();
        if key_id.is_empty() || key_id.len() > MAX_KEY_ID_BYTES {
            return Err(GrantKeyError::InvalidKeyId);
        }
        if key_id
            .bytes()
            .any(|byte| !byte.is_ascii_graphic() || byte == b'.')
        {
            return Err(GrantKeyError::InvalidKeyId);
        }
        Ok(Self { key_id, public_key })
    }
}

impl std::fmt::Debug for GrantVerificationKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The key itself is public, but printing it invites treating this type
        // as interchangeable with a signing key, which is not public.
        formatter
            .debug_struct("GrantVerificationKey")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GrantKeyError {
    #[error("grant key id is empty, oversized, or contains a reserved character")]
    InvalidKeyId,
    #[error("grant verifier requires at least one verification key")]
    NoKeys,
    #[error("grant verification key ids must be distinct")]
    DuplicateKeyId,
    #[error("grant audience is empty or oversized")]
    InvalidAudience,
}

/// One presented grant, together with the facts the deployment must supply.
pub struct GrantPresentation<'a> {
    /// The `base64url(payload).base64url(signature)` token as presented.
    pub token: &'a str,
    /// Seconds since the Unix epoch, from the deployment's clock.
    pub now_unix_seconds: u64,
    /// The authorization generation currently in force, from the compiled
    /// policy. A grant minted against a superseded generation fails closed.
    pub authorization_generation: u64,
}

/// Verifies grants against a rotating set of issuer keys.
pub struct GrantVerifier {
    keys: BTreeMap<String, UnparsedPublicKey<[u8; ED25519_PUBLIC_KEY_BYTES]>>,
    audience: String,
}

impl GrantVerifier {
    /// Builds a verifier for one audience over one or more issuer keys.
    /// Several keys let an issuer rotate without downtime.
    pub fn new(
        audience: impl Into<String>,
        keys: impl IntoIterator<Item = GrantVerificationKey>,
    ) -> Result<Self, GrantKeyError> {
        let audience = audience.into();
        if audience.is_empty() || audience.len() > MAX_AUDIENCE_BYTES {
            return Err(GrantKeyError::InvalidAudience);
        }
        let mut map = BTreeMap::new();
        for key in keys {
            if map.contains_key(&key.key_id) {
                return Err(GrantKeyError::DuplicateKeyId);
            }
            map.insert(
                key.key_id.clone(),
                UnparsedPublicKey::new(&ED25519, key.public_key),
            );
        }
        if map.is_empty() {
            return Err(GrantKeyError::NoKeys);
        }
        Ok(Self {
            keys: map,
            audience,
        })
    }

    /// Verifies a presented grant and returns its authenticated facts.
    ///
    /// Framing and size are checked first, then the signature, and only then
    /// the claims. Every failed check rejects the grant; none repairs it.
    pub fn verify(
        &self,
        presented: &GrantPresentation<'_>,
    ) -> Result<VerifiedDatasetGrant, GrantError> {
        if presented.token.len() > MAX_TOKEN_BYTES {
            return Err(GrantError::TokenTooLarge);
        }
        let (encoded_payload, encoded_signature) = presented
            .token
            .split_once('.')
            .ok_or(GrantError::InvalidFraming)?;
        if encoded_signature.contains('.') {
            return Err(GrantError::InvalidFraming);
        }
        let payload = URL_SAFE_NO_PAD
            .decode(encoded_payload)
            .map_err(|_| GrantError::InvalidEncoding)?;
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(GrantError::TokenTooLarge);
        }
        let signature = URL_SAFE_NO_PAD
            .decode(encoded_signature)
            .map_err(|_| GrantError::InvalidEncoding)?;

        // The key id is read from bytes that are not yet trusted, which is safe
        // only because it selects among keys this deployment already trusts and
        // the signature is then checked with that exact key. Since the key id is
        // itself inside the signed payload, pointing a grant at a different
        // trusted key invalidates its signature.
        // A repeated attribute name is refused here, before the signature check;
        // a refusal trusts nothing in the bytes it refuses.
        let claims: DatasetGrantClaims =
            serde_json::from_slice(&payload).map_err(|error| payload_error(&error))?;
        if claims.version != GRANT_VERSION {
            return Err(GrantError::UnsupportedVersion(claims.version));
        }
        let key = self
            .keys
            .get(&claims.key_id)
            .ok_or(GrantError::UnknownKeyId)?;

        key.verify(&grant_signing_input(&payload), &signature)
            .map_err(|_| GrantError::InvalidSignature)?;

        // Everything below here is authenticated.
        if claims.audience != self.audience {
            return Err(GrantError::AudienceMismatch);
        }
        if claims.authorization_generation != presented.authorization_generation {
            return Err(GrantError::AuthorizationGenerationMismatch {
                grant: claims.authorization_generation,
                current: presented.authorization_generation,
            });
        }
        if claims.expires_at_unix_seconds <= claims.issued_at_unix_seconds {
            return Err(GrantError::InvalidValidityWindow);
        }
        if claims.expires_at_unix_seconds - claims.issued_at_unix_seconds
            > MAX_GRANT_LIFETIME_SECONDS
        {
            return Err(GrantError::LifetimeTooLong);
        }
        if presented.now_unix_seconds < claims.issued_at_unix_seconds {
            return Err(GrantError::NotYetValid);
        }
        if presented.now_unix_seconds >= claims.expires_at_unix_seconds {
            return Err(GrantError::Expired);
        }
        if claims.attributes.len() > MAX_GRANT_ATTRIBUTES {
            return Err(GrantError::TooManyAttributes);
        }

        let attributes = claims
            .attributes
            .into_iter()
            .map(|(name, value)| Ok((name, value.into_principal_value()?)))
            .collect::<Result<BTreeMap<_, _>, GrantError>>()?;
        let principal =
            Principal::new(claims.subject, attributes).map_err(GrantError::Principal)?;
        Ok(VerifiedDatasetGrant {
            principal,
            expires_at_unix_seconds: claims.expires_at_unix_seconds,
            commands: verify_command_capabilities(claims.commands)?,
        })
    }
}

/// Turns the claimed capability into the authenticated one, or refuses.
///
/// A malformed entry refuses the whole grant rather than being dropped, so an
/// issuer never believes it minted a write capability no verifier honours.
fn verify_command_capabilities(
    claimed: Vec<GrantCommandCapability>,
) -> Result<GrantCommandCapabilities, GrantError> {
    if claimed.len() > MAX_GRANT_COMMAND_CAPABILITIES {
        return Err(GrantError::TooManyCommandCapabilities);
    }
    let mut by_collection: BTreeMap<String, BTreeSet<GrantCommandOperation>> = BTreeMap::new();
    for capability in claimed {
        if capability.collection.is_empty()
            || capability.collection.len() > MAX_GRANT_COLLECTION_BYTES
            || !capability
                .collection
                .bytes()
                .all(|byte| byte.is_ascii_graphic())
        {
            return Err(GrantError::MalformedCommandCapability);
        }
        if capability.operations.is_empty() {
            return Err(GrantError::MalformedCommandCapability);
        }
        let mut operations = BTreeSet::new();
        for operation in capability.operations {
            if !operations.insert(operation) {
                return Err(GrantError::MalformedCommandCapability);
            }
        }
        if by_collection
            .insert(capability.collection, operations)
            .is_some()
        {
            // The insert keeps the last entry for a collection, so without
            // this refusal a repeated one would be resolved in the attacker's
            // favour.
            return Err(GrantError::DuplicateCommandCapability);
        }
    }
    Ok(GrantCommandCapabilities { by_collection })
}

/// Authenticated grant facts that downstream capability issuance must retain.
///
/// Expiry and the command capability stay off [`Principal`], which the read
/// policy evaluates: authority from this presentation stays bounded by this
/// grant, and a write capability never reaches a read policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedDatasetGrant {
    principal: Principal,
    expires_at_unix_seconds: u64,
    commands: GrantCommandCapabilities,
}

impl VerifiedDatasetGrant {
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    pub fn into_principal(self) -> Principal {
        self.principal
    }

    pub const fn expires_at_unix_seconds(&self) -> u64 {
        self.expires_at_unix_seconds
    }

    /// The authenticated command capability, empty unless the issuer signed
    /// one. A deployment may narrow this against what it mounts; nothing may
    /// widen it.
    pub const fn commands(&self) -> &GrantCommandCapabilities {
        &self.commands
    }

    pub fn into_parts(self) -> (Principal, u64, GrantCommandCapabilities) {
        (self.principal, self.expires_at_unix_seconds, self.commands)
    }
}

impl std::ops::Deref for VerifiedDatasetGrant {
    type Target = Principal;

    fn deref(&self) -> &Self::Target {
        self.principal()
    }
}

impl std::fmt::Debug for GrantVerifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GrantVerifier")
            .field("audience", &self.audience)
            .field("key_ids", &self.keys.keys())
            .finish()
    }
}

impl PrincipalResolver<GrantPresentation<'_>> for GrantVerifier {
    type Error = GrantError;

    fn resolve(&self, context: &GrantPresentation<'_>) -> Result<Principal, Self::Error> {
        self.verify(context)
            .map(VerifiedDatasetGrant::into_principal)
    }
}

/// Builds the exact byte string that a grant signature covers, for issuer and
/// verifier alike.
pub fn grant_signing_input(payload: &[u8]) -> Vec<u8> {
    let mut signed = Vec::with_capacity(GRANT_SIGNING_DOMAIN.len() + payload.len());
    signed.extend_from_slice(GRANT_SIGNING_DOMAIN);
    signed.extend_from_slice(payload);
    signed
}

/// Frames a payload and its signature into a presentable token.
pub fn encode_grant_token(payload: &[u8], signature: &[u8]) -> String {
    format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(payload),
        URL_SAFE_NO_PAD.encode(signature)
    )
}

/// The signed grant payload, shared by issuers and the verifier.
///
/// Holding claims is not authority: only [`GrantVerifier::verify`] turns them
/// into a [`Principal`], which cannot be deserialized. `deny_unknown_fields`
/// stops a grant from carrying a table, column or predicate that a later
/// reader might honour.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatasetGrantClaims {
    pub version: u32,
    pub key_id: String,
    pub audience: String,
    pub subject: String,
    /// Each name at most once: a payload that repeats one does not parse.
    #[serde(default, deserialize_with = "unique_attributes")]
    pub attributes: BTreeMap<String, GrantAttributeValue>,
    pub authorization_generation: u64,
    pub issued_at_unix_seconds: u64,
    pub expires_at_unix_seconds: u64,
    /// The command capability. Absent and empty both authorize no command.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commands: Vec<GrantCommandCapability>,
}

/// The message [`unique_attributes`] refuses a repeated name with.
/// `serde_json` keeps a deserializer's error only as text, so [`payload_error`]
/// recognizes this one by it.
const REPEATED_ATTRIBUTE: &str = "dataset grant attributes repeat a name";

/// Reads `attributes`, refusing a name that appears twice.
///
/// serde's map reader keeps the last of a repeated key, so a payload naming
/// `organization_id` twice would verify as whichever value comes last: an
/// issuer that checked the first value would have signed a principal it never
/// meant to.
fn unique_attributes<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, GrantAttributeValue>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct UniqueNames;

    impl<'de> serde::de::Visitor<'de> for UniqueNames {
        type Value = BTreeMap<String, GrantAttributeValue>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an object of attribute names to typed values")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            let mut attributes = BTreeMap::new();
            while let Some((name, value)) = map.next_entry::<String, GrantAttributeValue>()? {
                if attributes.insert(name, value).is_some() {
                    return Err(serde::de::Error::custom(REPEATED_ATTRIBUTE));
                }
            }
            Ok(attributes)
        }
    }

    deserializer.deserialize_map(UniqueNames)
}

/// The refusal for a payload that does not parse as [`DatasetGrantClaims`].
fn payload_error(error: &serde_json::Error) -> GrantError {
    // The message is followed by the position, " at line 1 column N".
    if error.is_data() && error.to_string().starts_with(REPEATED_ATTRIBUTE) {
        GrantError::DuplicateAttribute
    } else {
        GrantError::MalformedPayload
    }
}

/// One collection's command capability, as the signed payload carries it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GrantCommandCapability {
    /// The collection id the application's configuration declares. A grant
    /// names no PostgreSQL object.
    pub collection: String,
    /// A non-empty set without repeats. An empty list is refused, never read
    /// as "all".
    pub operations: Vec<GrantCommandOperation>,
}

/// One operation a command capability may carry.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum GrantCommandOperation {
    Insert,
    Update,
    Delete,
}

impl GrantCommandOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

/// The authenticated command capability of one verified grant.
///
/// Only [`GrantVerifier::verify`] constructs one, so a capability cannot exist
/// without a signature over the collections and operations it carries.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GrantCommandCapabilities {
    by_collection: BTreeMap<String, BTreeSet<GrantCommandOperation>>,
}

impl GrantCommandCapabilities {
    pub fn is_empty(&self) -> bool {
        self.by_collection.is_empty()
    }

    pub fn len(&self) -> usize {
        self.by_collection.len()
    }

    /// Whether the grant authorizes this operation on this collection, matched
    /// literally: no prefix, pattern or wildcard.
    pub fn permits(&self, collection: &str, operation: GrantCommandOperation) -> bool {
        self.by_collection
            .get(collection)
            .is_some_and(|operations| operations.contains(&operation))
    }

    pub fn operations(&self, collection: &str) -> Option<&BTreeSet<GrantCommandOperation>> {
        self.by_collection.get(collection)
    }

    pub fn collections(&self) -> impl ExactSizeIterator<Item = &str> {
        self.by_collection.keys().map(String::as_str)
    }
}

/// The wire form of a verified principal attribute.
///
/// `int8`, `bytes` and `uuid` travel as strings because a 64-bit integer does
/// not survive a JSON number in a JavaScript issuer. Attributes are verified
/// and carried on the [`Principal`]; under a partitioned scope the one its
/// policy names selects the tenant whose rows the grant reads.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum GrantAttributeValue {
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(String),
    Bytes(String),
    Uuid(String),
}

impl GrantAttributeValue {
    pub(crate) fn into_principal_value(self) -> Result<PrincipalValue, GrantError> {
        Ok(match self {
            Self::Bool(value) => PrincipalValue::Bool(value),
            Self::Int2(value) => PrincipalValue::Int2(value),
            Self::Int4(value) => PrincipalValue::Int4(value),
            Self::Int8(value) => PrincipalValue::Int8(
                value
                    .parse::<i64>()
                    .map_err(|_| GrantError::MalformedAttribute)?,
            ),
            Self::Bytes(value) => PrincipalValue::Bytes(
                hex::decode(&value).map_err(|_| GrantError::MalformedAttribute)?,
            ),
            Self::Uuid(value) => {
                let bytes = hex::decode(&value).map_err(|_| GrantError::MalformedAttribute)?;
                PrincipalValue::Uuid(
                    bytes
                        .try_into()
                        .map_err(|_| GrantError::MalformedAttribute)?,
                )
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GrantError {
    #[error("dataset grant exceeds its size bound")]
    TokenTooLarge,
    #[error("dataset grant is not exactly one payload and one signature")]
    InvalidFraming,
    #[error("dataset grant is not valid unpadded base64url")]
    InvalidEncoding,
    #[error("dataset grant payload is not a well-formed grant")]
    MalformedPayload,
    #[error("dataset grant version {0} is not supported")]
    UnsupportedVersion(u32),
    #[error("dataset grant names no key this deployment trusts")]
    UnknownKeyId,
    #[error("dataset grant signature verification failed")]
    InvalidSignature,
    #[error("dataset grant was issued for a different audience")]
    AudienceMismatch,
    #[error(
        "dataset grant authorization generation {grant} is not the current generation {current}"
    )]
    AuthorizationGenerationMismatch { grant: u64, current: u64 },
    #[error("dataset grant expiry does not follow its issue time")]
    InvalidValidityWindow,
    #[error("dataset grant validity window exceeds the maximum grant lifetime")]
    LifetimeTooLong,
    #[error("dataset grant is not yet valid")]
    NotYetValid,
    #[error("dataset grant has expired")]
    Expired,
    #[error("dataset grant carries too many attributes")]
    TooManyAttributes,
    #[error("dataset grant repeats an attribute name")]
    DuplicateAttribute,
    #[error("dataset grant carries too many command capabilities")]
    TooManyCommandCapabilities,
    #[error("dataset grant command capability is malformed")]
    MalformedCommandCapability,
    #[error("dataset grant repeats a command capability collection")]
    DuplicateCommandCapability,
    #[error("dataset grant attribute value is malformed for its declared type")]
    MalformedAttribute,
    #[error("dataset grant does not describe a usable principal: {0}")]
    Principal(#[source] authz::PrincipalError),
}
