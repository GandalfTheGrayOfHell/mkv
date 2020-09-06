mod record;
mod hash;
mod remote;
mod mkv;

use mkv::Minikeyvalue;

use clap::{App, Arg};

fn main() {
	let matches = App::new("Minikeyvalue")
					.version("0.1.0")
					.author("Tanishq Jain <tanishqjain1002@gmail.com>")
					.about("A Rust port of minikeyvalue (https://github.com/geohot/minikeyvalue)")
					.usage("Usage: ./mkv <server, rebuild, rebalance> [FLAGS] [OPTIONS]")
					.arg(Arg::with_name("command")
							.help("Command to run from server, rebalance, rebuild")
							.required(true)
							.index(1))
					.arg(Arg::with_name("port")
							.short("p")
							.long("port")
							.value_name("PORT")
							.help("Port for the server to listen on")
							.default_value("3000")
							.takes_value(true))
					.arg(Arg::with_name("fallback")
							.short("f")
							.long("fallback")
							.value_name("PATH")
							.help("Fallback server for missing keys")
							.default_value("")
							.takes_value(true))
					.arg(Arg::with_name("replicas")
							.short("r")
							.long("replicas")
							.value_name("INT")
							.help("Amount of replicas to make of the data")
							.default_value("3")
							.takes_value(true))
					.arg(Arg::with_name("subvolumes")
							.short("s")
							.long("subvolumes")
							.value_name("INT")
							.help("Amount of subvolumes, disks per machine")
							.default_value("10")
							.takes_value(true))
					.arg(Arg::with_name("volumes")
							.short("v")
							.long("volumes")
							.value_name("PATH")
							.help("Volumes to use for storage, comma separated")
							.default_value("")
							.takes_value(true))
					.arg(Arg::with_name("unlink")
							.short("u")
							.long("unlink")
							.default_value("true")
							.help("Force UNLINK before DELETE"))
					.get_matches();

	let command = matches.value_of("command").unwrap();
	
	let volumes: Vec<String> = matches.value_of("volumes").unwrap().split(',').map(|x| x.to_string()).collect();
	let fallback = matches.value_of("fallback").unwrap().to_string();
	let replicas = matches.value_of("replicas").unwrap().parse::<i32>().expect("could not parse replicas");
	let subvolumes = matches.value_of("subvolumes").unwrap().parse::<i32>().expect("could not parse subvolumes");
	let protect = matches.value_of("unlink").unwrap_or("false").parse::<bool>().unwrap();
	let port = matches.value_of("port").unwrap().parse::<u16>().expect("could not parse port");

	if command != "server" && command != "rebalance" && command != "rebuild" {
		panic!("{}", matches.usage());
	}

	if matches.value_of("database").unwrap() == "" {
		panic!("Need a path to the database");
	}

	if volumes.len() < matches.value_of("replicas").unwrap().parse::<usize>().expect("Cannot parse replicas to INT") {
		panic!("Need at least as many volumes as replicas");
	}	

	let mut mkv = Minikeyvalue::new(volumes, fallback, replicas, subvolumes, protect, port);

	if command == "server" {
		mkv.server();
	} else if command == "rebalance" {
		mkv.rebalance();
	} else if command == "rebuild" {
		mkv.rebuild();
	}
}
