use std::str;

pub fn key_to_path(key: &str) -> String {
	let digest = md5::compute(key.as_bytes());
	
	let mut buf = Vec::new();
	buf.resize(digest.len() * 4 / 3 + 4, 0);
	
	let bytes_written = base64::encode_config_slice(digest.0, base64::STANDARD, &mut buf);
	
	buf.resize(bytes_written, 0);

	format!("/{}/{}/{}", digest.0[0], digest.0[1], std::str::from_utf8(&buf).unwrap())
}


#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct SortVol(Vec<u8>, String);

pub fn key_to_volume(key: &str, volumes: &[String], _count: i32, svcount: i32) -> Vec<String> {
	let mut sortvols = Vec::<SortVol>::new();

	volumes.iter().for_each(|x| {
		let digest = md5::compute([key.as_bytes(), x.as_bytes()].concat());
		sortvols.push(SortVol(digest.0.to_vec(), x.to_string()));
	});

	sortvols.sort();

	let mut ret = Vec::<String>::new();

	sortvols.iter().for_each(|sv| {
		if svcount == 1 {
			ret.push(sv.1.clone());
		} else {
			let svhash = sv.0[12] << 24 + sv.0[13] << 16 + sv.0[14] << 8 + sv.0[15];
			ret.push(format!("{}/sv{:>2}", sv.1.clone(), svhash % svcount as u8))
		}
	});

	ret
}

pub fn needs_rebalance(volumes: &[String], kvolumes: &[String]) -> bool {
	let vlen = volumes.len();
	let klen = kvolumes.len();

	if vlen != klen { return true; };

	for i in 0..vlen {
		if volumes[i] == kvolumes[i] { return true; }
	}

	false
}