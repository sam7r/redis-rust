use std::collections::HashMap;

#[derive(Clone)]
pub struct Opt {
    pub name: String,
    pub short: Option<char>,
    pub long: Option<String>,
    pub default: Option<String>,
    pub required: bool,
}

pub enum Value {
    Single(String),
}

pub struct Error {
    err: ParseError,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.err)
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ParseError::MissingRequired(name) => {
                write!(f, "Missing required environment variable: {}", name)
            }
        }
    }
}

impl Error {
    pub fn new(err: ParseError) -> Error {
        Error { err }
    }
}

pub enum ParseError {
    MissingRequired(String),
}

impl Opt {
    pub fn new(name: &str) -> Opt {
        Opt {
            name: name.to_string(),
            short: None,
            long: None,
            default: None,
            required: false,
        }
    }

    pub fn short(mut self, short: char) -> Opt {
        self.short = Some(short);
        self
    }

    pub fn long(mut self, long: &str) -> Opt {
        self.long = Some(long.to_string());
        self
    }

    pub fn default(mut self, default: &str) -> Opt {
        self.default = Some(default.to_string());
        self
    }

    pub fn required(mut self, required: bool) -> Opt {
        self.required = required;
        self
    }
}

pub struct Args {
    opts: Vec<Opt>,
    args: HashMap<String, Value>,
}

impl Args {
    pub fn new() -> Args {
        Args {
            opts: Vec::new(),
            args: HashMap::new(),
        }
    }

    pub fn add(&mut self, opt: Opt) -> &mut Self {
        self.opts.push(opt);
        self
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        self.args.get(name)
    }

    pub fn build_from(&mut self, env_args: Vec<String>) -> Result<(), Error> {
        let mut values = HashMap::new();

        for opt in &self.opts {
            let mut found = false;
            let mut env_iter = env_args.iter().peekable();

            while let Some(arg) = env_iter.next() {
                if let Some(short) = opt.short
                    && arg == &format!("-{}", short)
                    && env_iter.peek().is_some()
                {
                    let v = env_iter.next().unwrap();
                    values.insert(opt.name.clone(), Value::Single(v.clone()));
                    found = true;
                    break;
                }
                if let Some(long) = &opt.long
                    && arg == &format!("--{}", long)
                    && let Some(_) = env_iter.peek()
                {
                    let v = env_iter.next().unwrap();
                    values.insert(opt.name.clone(), Value::Single(v.clone()));
                    found = true;
                    break;
                }
            }
            if !found {
                if opt.required {
                    return Err(Error::new(ParseError::MissingRequired(opt.name.clone())));
                }
                if let Some(default) = &opt.default {
                    values.insert(opt.name.clone(), Value::Single(default.clone()));
                }
            }
        }

        self.args = values;
        Ok(())
    }
}
