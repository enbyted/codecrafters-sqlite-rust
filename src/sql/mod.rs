pub mod data;

use self::data::*;
use std::borrow::Cow;

peg::parser! {
    pub grammar parser() for str {
        rule dotcmd() -> Query<'input>
            = "." cmd:$(['a'..='z']+) { Query::DotCmd(cmd.into()) }

        rule _()
            = quiet!{[' ' | '\n' | '\r' | '\t']+}

        rule comma_separator()
            = _? "," _?

        rule farg_star() -> FunctionArguments<'input>
            = "*" { FunctionArguments::Star }

        rule farg_list() -> FunctionArguments<'input>
            = args:expr() ** comma_separator() { FunctionArguments::List(args) }

        rule function_arguments() -> FunctionArguments<'input>
            = farg_star() / farg_list()

        rule lit_string() -> Cow<'input, str>
            = ("'" val:$([^'\'']*) "'" {val.into()}) / ("\"" val:$([^'"']*) "\"" {val.into()})

        rule lit_integer() -> Literal<'input>
            = val:$(['-']?['0'..='9']+) {? Ok(Literal::Integer(val.parse().map_err(|_| "expected integer literal")?)) }

        rule literal() -> Literal<'input>
            = (v:lit_string() {Literal::String(v.into())}) / lit_integer()

        rule binary_operator() -> BinaryOperator
            = "=" {BinaryOperator::Equals}

        rule expr_literal() -> Expression<'input>
            = value:literal() { Expression::Literal(value) }

        rule expr_function() -> Expression<'input>
            = name:ident_unquoted() "(" _? arguments:function_arguments() _? ")" { Expression::Function { name, arguments } }

        rule ident_with_dot() -> Cow<'input,str>
            = value:ident() "." { value }

        rule expr_col_ref() -> Expression<'input>
            = schema:ident_with_dot()? table:ident_with_dot()? column:ident(){ Expression::ColRef { schema: schema, table, column } }

        rule expr_base() -> Expression<'input>
            = expr_literal() / expr_function() / expr_col_ref()

        rule expr_binary_op() -> Expression<'input>
            = left:expr_base() _? operator:binary_operator() _? right:expr_base() { Expression::BinaryOp { left: Box::new(left), right: Box::new(right), operator }}

        rule expr() -> Expression<'input>
            = expr_binary_op() / expr_base()

        rule ident() -> Cow<'input, str>
            = ident_quoted() / ident_unquoted()

        rule ident_unquoted() -> Cow<'input, str>
            = val:$(['a'..='z' | 'A'..='Z']['a'..='z' | 'A'..='Z' | '0'..='9' | '_']*) { val.into() }

        rule ident_quoted() -> Cow<'input, str>
            = "`" val:$([^'`']*) "`" { val.into() }

        rule as_ident() -> Cow<'input, str>
            = _ k("AS")  _ val:ident() { val }

        rule result_column() -> ResultColumn<'input>
            = _? value:expr() as_name:as_ident()? { ResultColumn { value, as_name } }

        rule k(kw: &'static str)
            = input:$([_]*<{kw.len()}>) {? if input.eq_ignore_ascii_case(kw) { Ok(()) } else { Err(kw) }}

        rule where_clause() -> Vec<Expression<'input>>
            = k("WHERE") _? exprs:expr() ++ comma_separator() { exprs }

        rule stmt_select() -> Query<'input>
            = k("SELECT") _ cols:result_column() ++ comma_separator() _ k("FROM") _ table:ident() _? conditions:where_clause()?{ Query::Select(StmtSelect { cols, table, conditions: conditions.unwrap_or_default() }) }

        rule type_name() -> TypeName
            = (k("INTEGER") {TypeName::Integer}) /
              (k("INT") {TypeName::Int}) /
              ((k("CHARACTER") / k("VARCHAR") / k("TEXT")) {TypeName::String}) /
              (k("BLOB") {TypeName::Blob})

        rule signed_number() -> i64
            = val:$(['+' | '-']?['0'..='9']+) {? val.parse().map_err(|e| "Expected signed number") }

        rule column_type() -> TypeName
            = type_name:type_name() (_? "(" _? signed_number()**<1,2> comma_separator() _? ")"  )? { type_name }

        rule column_constraint() -> ColumnConstraint
            = (k("PRIMARY") _ k("KEY") {ColumnConstraint::PrimaryKey}) /
              (k("AUTOINCREMENT") {ColumnConstraint::AutoIncrement}) /
              (k("NOT") _ k("NULL") {ColumnConstraint::NotNull})

        rule column_def() -> ColumnDefinition<'input>
            = name:(ident() / lit_string()) type_name:(_ t:column_type() {t})? _? constraints:column_constraint() ** _ { ColumnDefinition { name, type_name: type_name.unwrap_or(TypeName::Blob), constraints } }

        pub rule stmt_create_table() -> StmtCreateTable<'input>
            = k("CREATE") _ k("TABLE") _ name:(ident() / lit_string()) _? "(" _? columns:column_def() ++ comma_separator() _? ")" { StmtCreateTable { name, columns } }

        pub rule stmt_create_index() -> StmtCreateIndex<'input>
            = k("CREATE") _ k("INDEX") _ name:(ident() / lit_string()) _ k("ON") _ table:(ident() / lit_string()) _? "(" _? column:(ident() / lit_string()) _? ")" { StmtCreateIndex { name, table, column } }

        pub rule query() -> Query<'input>
            = dotcmd() / stmt_select()
    }
}
