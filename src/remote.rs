use std::fmt;
use std::error;

use reqwest::StatusCode;
use reqwest::blocking::{Client, Body};

#[derive(Debug)]
pub enum Error { WrongStatusCode }

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Wrong status code")
	}
}

impl error::Error for Error {
	fn source(&self) -> Option<&(dyn error::Error + 'static)> {
		None
	}
}

pub fn remote_delete(remote: String) -> Result<(), Box<dyn error::Error>> {
	let resp = Client::new().delete(&remote).body(Body::from("")).send()?;

	if resp.status() != StatusCode::NO_CONTENT { // 204
		return Err(Box::new(Error::WrongStatusCode));
	}

	return Ok(());
}

pub fn remote_put(remote: &String, _length: usize, body: &String) -> Result<(), Box<dyn std::error::Error>> {		
	let resp = Client::new().put(remote).body::<String>(body.into()).send()?;

	if resp.status() != StatusCode::CREATED && resp.status() != StatusCode::NO_CONTENT { // 201 && 204
		return Err(Box::new(Error::WrongStatusCode));
	}
	return Ok(());
}

pub fn remote_get(remote: &String) -> Result<String, Box<dyn std::error::Error>> {
	let mut resp = Client::new().get(remote).body(Body::from("")).send()?;

	if resp.status() != StatusCode::OK {
		return Err(Box::new(Error::WrongStatusCode));
	}

	let mut buffer = Vec::<u8>::new();
	resp.copy_to(&mut buffer)?;

	return Ok(String::from_utf8(buffer)?);
}

pub fn remote_head(remote: &String) -> bool {
	let resp = Client::new().head(remote).body(Body::from("")).send().expect("remote_head: error while sending request");
	return resp.status() == 200;
}