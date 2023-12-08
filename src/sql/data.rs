#[derive(Debug, Eq, PartialEq)]
pub enum TypeName {
    Integer,
    Int,
    String,
    Blob,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ColumnConstraint {
    PrimaryKey,
    AutoIncrement,
    NotNull,
}

#[derive(Debug)]
pub struct ColumnDefinition<'a> {
    pub name: &'a str,
    pub type_name: TypeName,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug)]
pub enum FunctionArguments<'a> {
    Star,
    List(Vec<Expression<'a>>),
}

#[derive(Debug, Clone)]
pub enum BinaryOperator {
    Equals,
}

#[derive(Debug, Clone)]
pub enum Literal<'a> {
    String(&'a str),
    Integer(i64),
}

#[derive(Debug)]
pub enum Expression<'a> {
    ColRef {
        schema: Option<&'a str>,
        table: Option<&'a str>,
        column: &'a str,
    },
    Function {
        name: &'a str,
        arguments: FunctionArguments<'a>,
    },
    BinaryOp {
        left: Box<Expression<'a>>,
        right: Box<Expression<'a>>,
        operator: BinaryOperator,
    },
    Literal(Literal<'a>),
}

#[derive(Debug)]
pub struct ResultColumn<'a> {
    pub value: Expression<'a>,
    pub as_name: Option<&'a str>,
}

#[derive(Debug)]
pub struct StmtSelect<'a> {
    pub cols: Vec<ResultColumn<'a>>,
    pub table: &'a str,
    pub conditions: Vec<Expression<'a>>,
}

#[derive(Debug)]
pub struct StmtCreateTable<'a> {
    pub name: &'a str,
    pub columns: Vec<ColumnDefinition<'a>>,
}

#[derive(Debug)]
pub enum Query<'a> {
    DotCmd(&'a str),
    Select(StmtSelect<'a>),
    CreateTable(StmtCreateTable<'a>),
}
