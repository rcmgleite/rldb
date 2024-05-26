#[derive(Debug, Clone)]
pub enum When {
    Always,
    Never,
}

#[derive(Clone, Debug)]
pub struct Fault {
    pub when: When,
}

impl Default for Fault {
    fn default() -> Self {
        Self { when: When::Never }
    }
}
