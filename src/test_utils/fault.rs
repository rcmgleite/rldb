//! Module that contains utility functions for fault injection in test code

#[derive(Debug, Clone)]
pub enum When {
    Always,
    Never,
}

/// A fault is an error that is returned based on the [`When`]
#[derive(Clone, Debug)]
pub struct Fault {
    pub when: When,
}

impl Default for Fault {
    fn default() -> Self {
        Self { when: When::Never }
    }
}
