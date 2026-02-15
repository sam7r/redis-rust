use super::args::{Args, Opt, Value};
use std::env;

pub struct Config {
    pub host: String,
    pub port: String,
    pub replica_of: Option<Value>,
}

impl Config {
    pub fn new() -> Self {
        let mut args = Args::new();
        args.add(
            Opt::new("PORT")
                .short('p')
                .long("port")
                .default("6379")
                .required(false),
        );
        args.add(
            Opt::new("HOST")
                .short('h')
                .long("host")
                .default("127.0.0.1")
                .required(false),
        );
        args.add(Opt::new("REPLICA_OF").long("replicaof").required(false));

        let env_args: Vec<String> = env::args().collect();

        args.build_from(env_args).unwrap_or_else(|err| {
            eprintln!("error parsing arguments: {}", err);
            std::process::exit(1);
        });

        let Some(Value::Single(host)) = args.get("HOST") else {
            eprintln!("invalid host");
            std::process::exit(1);
        };

        let Some(Value::Single(port)) = args.get("PORT") else {
            eprintln!("invalid port number");
            std::process::exit(1);
        };

        let replica_of = args.get("REPLICA_OF");

        Config {
            host: host.clone(),
            port: port.clone(),
            replica_of,
        }
    }
}
