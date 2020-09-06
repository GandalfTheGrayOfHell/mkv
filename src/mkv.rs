use std::u8;
use std::str;
use std::mem::drop;
use std::net::SocketAddr;

use std::{fmt, num::ParseIntError};
use std::sync::{Mutex, Arc};
use std::collections::HashMap;

use crate::hash::*;
use crate::remote::*;
use crate::record::{Record, Deleted};

use ascii::AsciiString;
use serde::{Deserialize, Serialize};
use tiny_http::{Server, Method, Response, Header};


#[derive(Clone)]
pub struct RebalanceRequest {
	key: String,
	volumes: Vec<String>,
	kvolumes: Vec<String>,
}

#[derive(Clone)]
pub struct RebuildRequest {
	vol: String,
	url: String,
}

#[derive(Clone, Deserialize, Serialize)]
struct File {
	name: String,
	file_type: String,
	time: String,
}


struct FileWrapper(Vec<File>);

impl FileWrapper {
	fn new() -> Self {
		Self {
			0: vec![],
		}
	}
}

#[derive(Clone, Deserialize, Serialize, Default)]
struct ListResponse {
	next: String,
	keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeHexError {
	OddLength,
	ParseInt(ParseIntError),
}

impl From<ParseIntError> for DecodeHexError {
	fn from(e: ParseIntError) -> Self {
		DecodeHexError::ParseInt(e)
	}
}

impl fmt::Display for DecodeHexError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			DecodeHexError::OddLength => "input string has an odd number of bytes".fmt(f),
			DecodeHexError::ParseInt(e) => e.fmt(f),
		}
	}
}

impl std::error::Error for DecodeHexError {}

#[derive(Clone)]
pub struct Minikeyvalue {
	db: HashMap<String, String>,
	lock: Arc<Mutex<HashMap<String, u8>>>, 
	volumes: Vec<String>,
	fallback: String,
	replicas: i32,
	subvolumes: i32,
	port: u16,
	protect: bool,
}

impl Minikeyvalue {
	pub fn new(volumes: Vec<String>, fallback: String, replicas: i32, subvolumes: i32, protect: bool, port: u16) -> Self {
		Self {
			db: HashMap::new(),
			lock: Arc::new(Mutex::new(HashMap::new())),
			volumes,
			fallback,
			replicas,
			subvolumes,
			port,
			protect,
		}
	}

	pub fn unlock_key(&self, key: &str) {
		let mut map = self.lock.lock().unwrap();
		map.remove(key);
		drop(map);
	}

	pub fn lock_key(&self, key: &str) -> bool {
		let mut map = self.lock.lock().unwrap();

		if map.contains_key(key) { return false; }
		
		map.insert(key.to_string(), 1);
		drop(map);

		true
	}

	pub fn get_record(&self, key: &str) -> Record {
		match self.db.get(key) {
			Some(val) => val.to_string().into(),
			None => Record::new(),
		}
	}

	// If `None` is returned, it is the first insertion else if `String` is returned,
	// then it updates the previous record and returns the old value. Will never fail
	// probably.
	pub fn put_record(&mut self, key: &str, rec: Record) -> Option<String> {
		self.db.insert(key.to_string(), rec.into())
	}

	pub fn rebuild(&mut self) {
		self.db.clear();

		let mut reqs = Vec::<RebuildRequest>::with_capacity(20000);

		for vol in self.volumes.iter() {
			let mut has_subvolumes = false;

			for f in get_files(&format!("http://{}/", vol)).0 {
				if f.name.len() == 4 && f.name.starts_with("sv") && f.file_type == "directory" {
					if let Some(v) = parse_volume(format!("{}/{}", vol, f.name)) {
						reqs.push(v);
					}

					has_subvolumes = true;
				}
			}

			if !has_subvolumes {
				if let Some(v) = parse_volume(vol.to_string()) {
					reqs.push(v);
				}	
			}
		}

		for _i in 0..128 {
			crossbeam::scope(|scope| {
				scope.spawn({
					let reqs_c = reqs.clone();
					let mut that = self.clone();

					move |_| {
						for req in reqs_c.iter() {
							for f in get_files(&req.url).0 {
								rebuild(&mut that, &req.vol, &f.name);
							}
						}
					}
				});
			}).expect("rebalance: crossbeam failed");
		}
	}

	pub fn rebalance(&mut self) {
		let mut reqs = Vec::<RebalanceRequest>::with_capacity(20000);

		for (key, value) in self.db.iter() {
			let rec = Record::from(value.to_string());
			let kvolumes = key_to_volume(key, &self.volumes, self.replicas, self.subvolumes);

			reqs.push(RebalanceRequest {
				key: key.to_string(),
				kvolumes,
				volumes: rec.rvolumes,
			});
		}

		for _i in 0..16 {
			crossbeam::scope(|scope| {
				let reqs_c = reqs.clone();
				let mut that = self.clone();

				scope.spawn(move |_| {
					for req in reqs_c.iter() {
						rebalance(&mut that, req);
					}
				});	
			}).expect("rebalance: crossbeam failed");
		}
	}

	pub fn server(&mut self) {
		println!("[OK] Listening on 127.0.0.1:{}", self.port);
		let addr = SocketAddr::from(([127, 0, 0, 1], self.port));

		let server = Server::http(addr).unwrap();
		let method_unlink = &Method::NonStandard(AsciiString::from_ascii("UNLINK").unwrap());
		let method_rebalance = &Method::NonStandard(AsciiString::from_ascii("REBALANCE").unwrap());

		for mut req in server.incoming_requests().by_ref() {
			let split = req.url().split('?').map(|x| x.to_string()).collect::<Vec<String>>();
			let key = split[0].clone();
			let q = split[1].clone();

			let method = req.method();

			let mut query = HashMap::new();

			let qs = q.split('&').collect::<Vec<&str>>();

			qs.iter()
				.for_each(|x| {
				for (_i, v) in x.split('=')
								.collect::<Vec<&str>>()
								.chunks(2)
								.enumerate() {
					query.insert(v[0], v[1]);
				}
			});

			if query.len() > 0 {
				if method == &Method::Get {
					req.respond(Response::empty(403)).expect("error while responding");
					return;
				}

				let operation = &qs[0];
				match *operation {
					"link" | "unlinked" => {
						let q_limit = query.get("limit");

						let mut limit = 0;

						let qlimit = if q_limit.is_some() {
							q_limit.unwrap()
						} else {
							&""
						};

						if qlimit != &"" {
							match qlimit.parse::<i32>() {
								Ok(nlimit) => limit = nlimit,
								Err(_e) => {
									req.respond(Response::empty(400)).expect("error while responding");
									return;
								},
							}
						}

						let mut counter = 0;
						let mut keys = Vec::<String>::new();
						let mut next = String::new();

						for (k, v) in self.db.iter() {
							let rec = Record::from(v.clone());

							if (rec.deleted != Deleted::No && operation == &"list") || (rec.deleted != Deleted::Soft && operation == &"unlinked") {
								continue;
							}

							if counter > 1000000 {
								req.respond(Response::empty(403)).expect("error while responding");
								return;
							}

							if limit > 0 && keys.len() as i32 == limit {
								next = k.to_string();
							}

							counter += 1;
							keys.push(k.to_string());
						}

						let lsw = match serde_json::to_string(&ListResponse {next, keys}) {
							Ok(v) => v,
							Err(_e) => {
								req.respond(Response::empty(500)).expect("error while responding");
								return;
							}
						};

						let mut headers = Vec::new();
						headers.push(Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap());

						let bytes = lsw.as_bytes();
						let resp = Response::new(200.into(), headers, bytes, Some(bytes.len()), None);
						req.respond(resp).expect("error while responding");
						continue;
					}
					_ => {
						req.respond(Response::empty(403)).expect("error while responding");
						continue;
					}
				}
			} // end query handler

			
			if method == &Method::Put || method == &Method::Delete || method == method_unlink || method == method_rebalance {
				if !self.lock_key(&key) {
					req.respond(Response::empty(409)).expect("error while responding");
					continue;
				}

				self.unlock_key(&key);
			}

			if method == &Method::Get ||  method == &Method::Head {
				let rec = self.get_record(&key);
				
				let mut remote = String::new();
				let mut resp = Response::empty(404);

				if rec.hash.len() != 0 {
					let header = Header::from_bytes(&b"Content-Md5"[..], rec.hash.as_bytes()).unwrap();
					resp.add_header(header);
				}

				if rec.deleted == Deleted::Soft || rec.deleted == Deleted::Hard {
					if self.fallback == "" {
						let header = Header::from_bytes(&b"Content-Length"[..], &b"0"[..]).unwrap();
						resp.add_header(header);
						resp = Response::with_status_code(resp, 404);
						req.respond(resp).expect("error while responding");
						continue;
					}
				} else {
					let kvolumes = key_to_volume(&key, &self.volumes, self.replicas, self.subvolumes);

					if needs_rebalance(&rec.rvolumes, &kvolumes) {
						eprintln!("On wrong volumes, needs rebalance");
					}

					let mut good = false;
					for rvol in rec.rvolumes.iter() {
						remote = format!("http://{}{}", rvol, key_to_path(&key));

						if remote_head(&remote) {
							good = true;
							break;
						}
					}

					if !good {
						let header = Header::from_bytes(&b"Content-Length"[..], &b"0"[..]).unwrap();
						resp.add_header(header);
						resp = Response::with_status_code(resp, 404);
						req.respond(resp).expect("error while responding");
						continue;
					}
				}

				resp = Response::with_header(resp, Header::from_bytes(&b"Location"[..], remote).unwrap());
				resp = Response::with_header(resp, Header::from_bytes(&b"Content-Length"[..], &b"0"[..]).unwrap());
				resp = Response::with_status_code(resp, 302);

				req.respond(resp).expect("error while responding");
				continue;
			} else if method == &Method::Put {
				let mut flag = false;
				for head in &req.headers().to_vec() {
					if head.field.as_str() == "Content-Length" && head.value == "0" {
						flag = true;	
					}
				}

				if flag == true {
					req.respond(Response::empty(411)).expect("error while responding");
					continue;
				}

				let rec = self.get_record(&key);
				if rec.deleted == Deleted::No {
					req.respond(Response::empty(403)).expect("error while responding");
					continue;
				}

				let kvolumes = key_to_volume(&key, &self.volumes, self.replicas, self.subvolumes);


				self.put_record(&key, Record {rvolumes: kvolumes.clone(), deleted: Deleted::Soft, hash: "".to_string()}); // TODO: not handling errors here

				let mut buffer = Vec::<u8>::new();
				req.as_reader().read_to_end(&mut buffer).unwrap(); // TODO: handle error
				for i in 0..kvolumes.len() {
					let remote = format!("http://{}{}", kvolumes[i], key_to_path(&key));
					match remote_put(&remote, buffer.len(), &str::from_utf8(&buffer).to_owned().unwrap().to_string()) {
						Err(_e) => flag = true,
						Ok(_v) => flag = false,
					}

					if flag == true { break; }
				}

				if flag == true {
					eprintln!("replica write failed");
					req.respond(Response::empty(500)).expect("error while responding");
					continue;
				}

				let hash = format!("{}", String::from_utf8(md5::compute(buffer).0.to_vec()).unwrap());
				self.put_record(&key, Record {rvolumes: kvolumes, deleted: Deleted::No, hash }); // TODO: not handling errors here

				req.respond(Response::empty(201)).expect("error while responding");
			} else if method == &Method::Delete || method == method_unlink {
				let unlink = method == method_unlink;

				let rec = self.get_record(&key);

				if rec.deleted == Deleted::Hard || (unlink && rec.deleted == Deleted::Soft) {
					req.respond(Response::empty(404)).expect("error while responding");
					return;
				}
				
				if !unlink && self.protect && rec.deleted == Deleted::No {
					req.respond(Response::empty(403)).expect("error while responding");
					return;
				}

				self.put_record(&key, Record {
					rvolumes: rec.rvolumes.clone(), 
					deleted: Deleted::Soft,
					hash: rec.hash
				});

				if !unlink {
					let mut delete_error = false;
					for volume in rec.rvolumes {
						let remote = format!("http://{}{}", volume, key_to_path(&key));

						match remote_delete(remote) {
							Err(_e) => delete_error = true,
							Ok(_v) => {},
						}
					}

					if delete_error {
						req.respond(Response::empty(500)).expect("error while responding");
						return;
					}

					self.db.remove(&key);
				}

				req.respond(Response::empty(204)).expect("error while responding");
			} else if method == method_rebalance {
				let rec = self.get_record(&key);

				if rec.deleted != Deleted::No {
					req.respond(Response::empty(404)).expect("error while responding");
					return;
				}

				let kvolumes = key_to_volume(&key, &self.volumes, self.replicas, self.subvolumes);
				let rbreq = RebalanceRequest { key, volumes: rec.rvolumes, kvolumes };

				if !rebalance(self, &rbreq) {
					req.respond(Response::empty(400)).expect("error while responding");
					return;
				}

				req.respond(Response::empty(204)).expect("error while responding");
			}
		} // loop
	} // fn
}


pub fn rebuild(that: &mut Minikeyvalue, vol: &str, name: &str) -> bool {
	let mut buf = Vec::new();
	buf.resize((name.len() + 3) / 12, 0);
	let bytes_decoded = match base64::decode_config_slice(name, base64::STANDARD, &mut buf) {
		Ok(v) => v,
		Err(e) => {
			eprintln!("rebuild: base64 decode error: {}", e);
			return false;
		}
	};

	buf.resize(bytes_decoded, 0);							
	let key = std::str::from_utf8(&buf).expect("rebuild: cannot unwrap buffer");
	let kvolumes = key_to_volume(key, &that.volumes, that.replicas, that.subvolumes);

	if !that.lock_key(key) {
		eprintln!("rebuild: lock key issue");
		return false;
	}

	that.unlock_key(key);

	let rec = match that.db.get(key) {
		Some(v) => {
			Record::from(v.clone())
		}
		None => {
			Record {
				rvolumes: vec![vol.to_string()],
				deleted: Deleted::No,
				hash: String::new(),
			}
		}
	};

	let mut pvalues = Vec::<String>::new();
	for v in &kvolumes {
		for v2 in &rec.rvolumes {
			if *v == *v2 {
				pvalues.push(v.to_string());
			}
		}
	}

	for v2 in &rec.rvolumes {
		let mut insert = true;
		
		for v in &kvolumes {
			if *v == *v2 {
				insert = false;
				break;
			}
		}

		if insert { pvalues.push(v2.to_string()); }
	}

	that.put_record(key, Record {
		rvolumes: pvalues,
		deleted: Deleted::No,
		hash: String::new(),
	});
	
	true
}

pub fn rebalance(that: &mut Minikeyvalue, req: &RebalanceRequest) -> bool {
	let kp = key_to_path(&req.key);

	let mut rvolumes = Vec::<String>::new();
	for rv in &req.volumes {
		if remote_head(&format!("http://{}{}", rv, kp)) {
			rvolumes.push(rv.to_string());
		}
	}

	if rvolumes.is_empty() {
		eprintln!("rebalance: {} is missing", req.key);
		return false;
	}

	if !needs_rebalance(&rvolumes, &req.kvolumes) { return true; }

	let s = match remote_get(&format!("http://{}{}", &rvolumes[0], kp)) {
		Ok(ss) => ss,
		Err(_e) => return false,
	};

	for v in req.kvolumes.iter() {
		let mut needs_write = true;

		for v2 in rvolumes.iter() {
			if v == v2 {
				needs_write = false;
				break;
			}
		}

		if needs_write {
			if let Err(e) = remote_put(&format!("http://{}{}", v, kp), s.len(), &s) {
				eprintln!("put error: {}", e);
				return false;
			}
		}
	}

	that.put_record(&req.key, Record {
		rvolumes: req.kvolumes.clone(),
		deleted: Deleted::No,
		hash: String::new(),			
	});

	for v2 in rvolumes.iter() {
		let mut needs_delete = true;

		for v in req.kvolumes.iter() {
			if v == v2 {
				needs_delete = true;
				break;
			}
		}

		if needs_delete {
			if let Err(e) = remote_delete(format!("http://{}{}", v2, kp)) {
				eprintln!("delete error: {}", e);
				return false;
			}
		}
	}

	true
}

fn parse_volume(vol: String) -> Option<RebuildRequest> {
	for i in get_files(&format!("http://{}/", vol)).0 {
		if valid(&i) {
			for j in get_files(&format!("http://{}/{}", vol, i.name)).0 {
				if valid(&j) {
					let url = format!("http://{}/{}/{}/", vol, i.name, j.name);
					return Some(RebuildRequest { vol, url });
				}
			}
		}
	}

	None
}

fn decode_hex(s: &str) -> Result<Vec<u8>, DecodeHexError> {
	if s.len() % 2 != 0 {
		Err(DecodeHexError::OddLength)
	} else {
		(0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.into())).collect()
	}
}

fn get_files(url: &str) -> FileWrapper {
	let mut res = FileWrapper::new();

	match remote_get(&url.to_string()) {
		Ok(ss) => res.0 = serde_json::from_str(&ss).expect("get_files: Cannot parse"),
		Err(e) => {
			eprintln!("get_files: remote_get error {}", e);
			return res;
		},
	};

	res
}

fn valid(f: &File) -> bool {
	if f.name.len() != 2 || f.file_type != "directory" { return false; }

	let decoded = match decode_hex(&f.name) {
		Ok(dec) => dec,
		Err(_e) => return false,
	};

	if decoded.len() != 1 { return false; }

	true
}