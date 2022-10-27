use futures::stream::Stream;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::Message;
use sqlparser::{ dialect::GenericDialect, parser::Parser };
use std::{ boxed::Box, collections::HashMap };
use tokio::runtime::current_thread::Runtime;
use sqlparser::ast;
use ast::Select as RealSelect;
use ast::SetExpr::Select;
use std::io::Write;

use crate::expression::exec_expr;

mod expression;
mod utils;

fn input( query : impl Into< String > ) -> std::io::Result< String >
{
  print!( "{query}", query = query.into() );

  let _ = std::io::stdout().flush();
  let mut input = String::new();
  std::io::stdin().read_line( &mut input )?;

  Ok( input.trim_end_matches( "\n" ).to_owned() )
}

fn main() -> Result< (), Box< dyn std::error::Error > >
{
  pretty_env_logger::init();

  log::info!( "Application start" );
  let ( _, mut config ) = utils::get_config()?;
  let consumer: StreamConsumer = config.set( "group.id", "rust_example_group_1" ).create()?;

  let sql = input( "sql> " ).unwrap();
  let dialect = GenericDialect {};
  let ast = Parser::parse_sql( &dialect, &sql ).unwrap();

  let statement = &ast[ 0 ];
  let mut to_show = Vec::new();
  let mut take_from = String::new();
  let mut to_filter = None;
  log::info!( "Start init" );
  if let sqlparser::ast::Statement::Query( query ) = statement
  {
    match *query.body.clone()
    {
      Select( select_query ) => match *select_query
      {
        RealSelect{ projection, from, selection, .. } =>
        {
          projection.iter()
          .for_each( | val |
          {
            match val
            {
              ast::SelectItem::UnnamedExpr( expr ) =>
              {
                to_show.push(( expr.to_string(), exec_expr( expr.to_owned() ) ) );
              },
              ast::SelectItem::Wildcard =>
              {
              	// можно добавить экспрешн с плейсхолдером *
                to_show.push(( "*".to_owned(), Box::new( | _ | ast::Value::Placeholder( "*".to_owned() ) ) ) );
              },
              _ => unimplemented!()
            }
          });
          take_from = from[ 0 ].relation.to_string().replace( "'", "" );
          if let Some( filter ) = selection
          {
            to_filter = Some( exec_expr( filter ) );
          }
        },
      },
      _ => {}
    }
  }
  log::info!( "End init" );
  log::info!( "Take from: {take_from}" );
  // here must be known from which topic we take values
  consumer.subscribe( &vec![ take_from.as_ref() ] )?;

  let processor = consumer
  .start()
  .filter_map( | result | match result
  {
    Ok( message ) =>
    {
      match message.payload_view::< str >()
      {
        Some( Ok( data ) )
        =>
        {
          log::info!( "{data}" );
          let json_map = serde_json::from_str::< HashMap< String, serde_json::Value> >( data ).unwrap();
          Some
          (
            utils::to_ast_value_map( json_map )
          )
        },
        _ => None
      }
    },
    Err( err ) =>
    {
      eprintln!( "error consuming from message stream: {}", err );
      None
    }
  })
  // here must be filtering values by expr(where clause) from sql query
  .filter
  ( | msg |
    if let Some( filter ) = to_filter.as_ref()
    {
      if let ast::Value::Boolean( true ) = filter( msg )
      { true }
      else
      { false }
    }
    else // if no filter - do not filter
    { true }
  )
  // here will show all messages, after filter, somehow
  .for_each( | msg |
  {
    println!( "Message:" );
    // Если плейсхолдер * - выводить все значения из мапы
    to_show.iter().for_each
    (
      |( key, show )|
      {
        match show( &msg )
        {
          ast::Value::Placeholder( ph ) if &ph == "*" =>
          {
            msg.iter().for_each( |( k, v )| println!( "{k}: {: <15}", v.to_string() ) )
          },
          msg => println!( "{key}: {: <15}", msg.to_string() )
        }
      }
    );
    println!();
    Ok( () )
  });

  Runtime::new()?
  .block_on( processor )
  .map_err( | _ | eprintln!( "error running consumer on current thread" ) )
  .ok();

  Ok( () )
}
