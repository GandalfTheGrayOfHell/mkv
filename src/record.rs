use std::convert::From;

#[derive(Debug, PartialEq)]
pub enum Deleted {
	No,
	Soft,
	Hard,
}

#[derive(Debug)]
pub struct Record {
	pub rvolumes: Vec<String>,
	pub deleted: Deleted, // TODO: handle pub later
	pub hash: String, // TODO: handle pub later
}

impl Record {
	pub fn new() -> Self {
		Self {
			rvolumes: vec![],
			deleted: Deleted::Hard,
			hash: String::new(),
		}
	}
}

impl From<String> for Record {
	fn from(mut string: String) -> Self {
		let mut rec = Record::new();

		if string.starts_with("DELETED") {
			rec.deleted = Deleted::Soft;
			string = string[7..].to_string();
		}

		if string.starts_with("HASH") {
			rec.hash = string[4..36].to_string();
			string = string[36..].to_string();
		}

		rec.rvolumes = string.split(',').map(|x| x.to_string()).collect();
		
		rec
	}
}

impl Into<String> for Record {
	fn into(self) -> String {
		let mut cc = String::new();

		if self.deleted == Deleted::Hard { panic!("Cannot put HARD delete in the database"); }

		if self.deleted == Deleted::Soft { cc.push_str("DELETED"); }

		if self.hash.len() == 32 {
			cc.push_str("HASH");
			cc.push_str(&self.hash);
		}

		cc.push_str(&self.rvolumes.join(","));

		cc
	}
}