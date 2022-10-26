use clap::{ App, Arg };
use rdkafka::config::ClientConfig;
use std::boxed::Box;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;

pub fn get_config() -> Result<( String, ClientConfig ), Box< dyn std::error::Error > >
{
  let matches = App::new( "rust client example" )
  .version( option_env!( "CARGO_PKG_VERSION" ).unwrap_or( "" ) )
  .arg
  (
    Arg::with_name( "config" )
    .help( "path to confluent cloud config file" )
    .long("config")
    .takes_value( true )
    .required( true ),
  )
  .arg
  (
    Arg::with_name( "topic")
    .help( "test topic to use" )
    .long( "topic" )
    .takes_value( true )
    .required( false ),
  )
  .get_matches();

  let mut kafka_config = ClientConfig::new();

  let file = File::open( matches.value_of( "config" ).ok_or( "error parsing config" )? )?;
  for line in BufReader::new(&file).lines()
  {
    let cur_line: String = line?.trim().to_string();
    if cur_line.starts_with( '#' ) || cur_line.len() < 1
    {
      continue;
    }
    let key_value: Vec< &str > = cur_line.split( "=" ).collect();
    kafka_config.set
    (
      key_value.get( 0 ).ok_or( "malformed key" )?,
      key_value.get( 1 ).ok_or( "malformed value" )?,
    );
  }

  Ok(
  (
    matches
    .value_of( "topic" )
    .unwrap_or_default()
    .to_string(),
    kafka_config,
  ))
}

pub fn to_ast_value_map( map : HashMap< String, serde_json::Value > ) -> HashMap< String, sqlparser::ast::Value >
{
  HashMap::from_iter
  (
    map.iter()
    .map
    ( |( k, v )|
      match v
      {
        serde_json::Value::Null => ( k.to_owned(), sqlparser::ast::Value::Null ),
        serde_json::Value::Bool( val ) => ( k.to_owned(), sqlparser::ast::Value::Boolean( *val ) ),
        serde_json::Value::Number( val ) => ( k.to_owned(), sqlparser::ast::Value::Number( val.to_string(), false ) ),
        serde_json::Value::String( val ) => ( k.to_owned(), sqlparser::ast::Value::SingleQuotedString( val.to_string() ) ),
        _ => unimplemented!()
      }
    )
  )
}
