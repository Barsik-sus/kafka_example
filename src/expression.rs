use std::collections::HashMap;

use sqlparser::ast;

fn value_as_f64( val : ast::Value ) -> Result< f64, String >
{
  if let ast::Value::Number( string_number, _ ) = val
  {
    string_number.parse().or( Err( "Can not take a number".to_owned() ) )
  }
  else
  {
    Err( "Value not a number".to_owned() )
  }
}

pub fn exec_expr( expr : ast::Expr ) -> Box< dyn Fn( &HashMap< String, ast::Value > ) -> ast::Value >
{
  match expr
  {
    ast::Expr::Value( val ) => Box::new( move | _ | val.clone() ),
    ast::Expr::Identifier( ident ) => Box::new( move | values : &HashMap< String, ast::Value > | values[ &ident.value ].to_owned() ),
    ast::Expr::BinaryOp { left, op, right } =>
    {
      let left = exec_expr( *left );
      let right = exec_expr( *right );

      match op
      {
        sqlparser::ast::BinaryOperator::Plus =>
        {
          Box::new( move | values : &HashMap< String, ast::Value > |
          {
            let left = value_as_f64( left( values ) ).unwrap();
            let right = value_as_f64( right( values ) ).unwrap();
            log::info!( "{l} + {r}", l = left, r = right );
            ast::Value::Number( format!( "{}", left + right ), false )
          })
        },
        sqlparser::ast::BinaryOperator::Minus =>
        {
          Box::new( move | values : &HashMap< String, ast::Value > |
          {
            let left = value_as_f64( left( values ) ).unwrap();
            let right = value_as_f64( right( values ) ).unwrap();
            log::info!( "{l} - {r}", l = left, r = right );
            ast::Value::Number( format!( "{}", left - right ), false )
          })

        },
        sqlparser::ast::BinaryOperator::Multiply =>
        {
          Box::new( move | values : &HashMap< String, ast::Value > |
          {
            let left = value_as_f64( left( values ) ).unwrap();
            let right = value_as_f64( right( values ) ).unwrap();
            log::info!( "{l} * {r}", l = left, r = right );
            ast::Value::Number( format!( "{}", left * right ), false )
          })
        },
        sqlparser::ast::BinaryOperator::Divide =>
        {
          Box::new( move | values : &HashMap< String, ast::Value > |
          {
            let left = value_as_f64( left( values ) ).unwrap();
            let right = value_as_f64( right( values ) ).unwrap();
            log::info!( "{l} / {r}", l = left, r = right );
            ast::Value::Number( format!( "{}", left / right ), false )
          })

        },
        sqlparser::ast::BinaryOperator::Gt =>
        {
          Box::new( move | values : &HashMap< String, ast::Value > |
          {
            let left = value_as_f64( left( values ) ).unwrap();
            let right = value_as_f64( right( values ) ).unwrap();
            log::info!( "{l} > {r}", l = left, r = right );
            ast::Value::Boolean( left > right )
          })
        }
        sqlparser::ast::BinaryOperator::Lt =>
        {
          Box::new( move | values : &HashMap< String, ast::Value > |
          {
            let left = value_as_f64( left( values ) ).unwrap();
            let right = value_as_f64( right( values ) ).unwrap();
            log::info!( "{l} < {r}", l = left, r = right );
            ast::Value::Boolean( left < right )
          })
        },
        // sqlparser::ast::BinaryOperator::GtEq => format!( "{left} >= {right}" ),
        // sqlparser::ast::BinaryOperator::LtEq => format!( "{left} <= {right}" ),
        sqlparser::ast::BinaryOperator::Eq =>
        {
          Box::new( move | values : &HashMap< String, ast::Value > |
          {
            let left = left( values );
            let right = right( values );
            log::info!( "{l} == {r}", l = left, r = right );
            ast::Value::Boolean( left == right )
          })
        },
        sqlparser::ast::BinaryOperator::NotEq =>
        {
          Box::new( move | values : &HashMap< String, ast::Value > |
          {
            let left = left( values );
            let right = right( values );
            log::info!( "{l} != {r}", l = left, r = right );
            ast::Value::Boolean( left != right )
          })
        },
        sqlparser::ast::BinaryOperator::And =>
        {
          Box::new( move | values : &HashMap< String, ast::Value > |
          {
            let left = left( values );
            let right = right( values );
            log::info!( "{l} && {r}", l = left, r = right );
            if let ( ast::Value::Boolean( left ), ast::Value::Boolean( right ) ) = ( left, right )
            {
              ast::Value::Boolean( left && right )
            }
            else
            {
              ast::Value::Boolean( false )
            }
          })
        },
        sqlparser::ast::BinaryOperator::Or =>
        {
          Box::new( move | values : &HashMap< String, ast::Value > |
          {
            let left = left( values );
            let right = right( values );
            log::info!( "{l} || {r}", l = left, r = right );
            if let ( ast::Value::Boolean( left ), ast::Value::Boolean( right ) ) = ( left, right )
            {
              ast::Value::Boolean( left || right )
            }
            else
            {
              ast::Value::Boolean( false )
            }
          })
        },
        _ => { unimplemented!() },
      }
    },
    ast::Expr::Nested( expr ) => exec_expr( *expr ),
    _ => Box::new( | _ | ast::Value::Null )
  }
}
