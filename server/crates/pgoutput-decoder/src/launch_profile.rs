use thiserror::Error;

/// The only `pgoutput` profile accepted by production source sessions in v1:
/// protocol 1 with messages on and streaming, binary and two-phase off.
///
/// Construction is closed: [`Self::V1`] is the only value. Decoder fixtures
/// inside this crate may exercise protocol v2, but cannot turn those fixtures
/// into a production stream configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PgoutputLaunchProfile(());

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PgoutputProfileError {
    #[error("PGOUTPUT_PUBLICATION_REQUIRED: publication_names must not be empty")]
    PublicationRequired,
}

impl PgoutputLaunchProfile {
    pub const V1: Self = Self(());

    pub const fn protocol_version(self) -> u32 {
        1
    }

    pub const fn two_phase(self) -> bool {
        false
    }

    /// Builds the exact option set shared by persistent and temporary streams.
    ///
    /// `publication_names` must already be quoted for PostgreSQL's option
    /// grammar by the maintained database driver boundary.
    pub fn start_replication_options(
        self,
        publication_names: &str,
    ) -> Result<[(&'static str, &str); 6], PgoutputProfileError> {
        if publication_names.is_empty() {
            return Err(PgoutputProfileError::PublicationRequired);
        }
        Ok([
            ("proto_version", "1"),
            ("publication_names", publication_names),
            ("messages", "true"),
            ("streaming", "off"),
            ("binary", "false"),
            ("two_phase", "false"),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launch_profile_builds_the_exact_persistent_and_temporary_option_set() {
        let expected = [
            ("proto_version", "1"),
            ("publication_names", "\"application_publication\""),
            ("messages", "true"),
            ("streaming", "off"),
            ("binary", "false"),
            ("two_phase", "false"),
        ];

        let persistent = PgoutputLaunchProfile::V1
            .start_replication_options("\"application_publication\"")
            .unwrap();
        let temporary = PgoutputLaunchProfile::V1
            .start_replication_options("\"application_publication\"")
            .unwrap();
        assert_eq!(persistent, expected);
        assert_eq!(temporary, expected);
    }

    #[test]
    fn launch_profile_refuses_an_empty_publication_list() {
        assert_eq!(
            PgoutputLaunchProfile::V1
                .start_replication_options("")
                .unwrap_err(),
            PgoutputProfileError::PublicationRequired
        );
    }
}
