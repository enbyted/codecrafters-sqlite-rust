use std::borrow::Cow;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TypeName {
    Integer,
    Int,
    String,
    Blob,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ColumnConstraint {
    PrimaryKey,
    AutoIncrement,
    NotNull,
}

#[derive(Debug, Clone)]
pub struct ColumnDefinition<'a> {
    pub name: Cow<'a, str>,
    pub type_name: TypeName,
    pub constraints: Vec<ColumnConstraint>,
}

impl<'a> ColumnDefinition<'a> {
    pub fn is_rowid(&self) -> bool {
        self.type_name == TypeName::Integer
            && self
                .constraints
                .iter()
                .find(|cn| **cn == ColumnConstraint::PrimaryKey)
                .is_some()
    }

    pub fn to_owned(&self) -> ColumnDefinition<'static> {
        ColumnDefinition {
            name: self.name.to_string().into(),
            type_name: self.type_name.clone(),
            constraints: self.constraints.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum FunctionArguments<'a> {
    Star,
    List(Vec<Expression<'a>>),
}

impl<'a> FunctionArguments<'a> {
    pub fn to_owned(&self) -> FunctionArguments<'static> {
        match self {
            FunctionArguments::Star => FunctionArguments::Star,
            FunctionArguments::List(v) => {
                FunctionArguments::List(v.iter().map(|i| i.to_owned()).collect())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum BinaryOperator {
    Equals,
}

#[derive(Debug, Clone)]
pub enum Literal<'a> {
    String(Cow<'a, str>),
    Integer(i64),
}

impl<'a> Literal<'a> {
    pub fn to_owned(&self) -> Literal<'static> {
        match self {
            Literal::String(v) => Literal::String(v.to_string().into()),
            Literal::Integer(v) => Literal::Integer(*v),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Expression<'a> {
    ColRef {
        schema: Option<Cow<'a, str>>,
        table: Option<Cow<'a, str>>,
        column: Cow<'a, str>,
    },
    Function {
        name: Cow<'a, str>,
        arguments: FunctionArguments<'a>,
    },
    BinaryOp {
        left: Box<Expression<'a>>,
        right: Box<Expression<'a>>,
        operator: BinaryOperator,
    },
    Literal(Literal<'a>),
}

fn option_cow_str_to_owned<'a>(v: &Option<Cow<'a, str>>) -> Option<Cow<'static, str>> {
    match v {
        Some(v) => Some(v.to_string().into()),
        None => None,
    }
}
impl<'a> Expression<'a> {
    pub fn to_owned(&self) -> Expression<'static> {
        match self {
            Expression::ColRef {
                schema,
                table,
                column,
            } => Expression::ColRef {
                schema: option_cow_str_to_owned(schema),
                table: option_cow_str_to_owned(table),
                column: column.to_string().into(),
            },
            Expression::Function { name, arguments } => Expression::Function {
                name: name.to_string().into(),
                arguments: arguments.to_owned(),
            },
            Expression::BinaryOp {
                left,
                right,
                operator,
            } => Expression::BinaryOp {
                left: Box::new(left.as_ref().to_owned()),
                right: Box::new(right.as_ref().to_owned()),
                operator: operator.clone(),
            },
            Expression::Literal(l) => Expression::Literal(l.to_owned()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResultColumn<'a> {
    pub value: Expression<'a>,
    pub as_name: Option<Cow<'a, str>>,
}

impl ResultColumn<'_> {
    pub fn to_owned(&self) -> ResultColumn<'static> {
        ResultColumn {
            value: self.value.to_owned(),
            as_name: option_cow_str_to_owned(&self.as_name),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StmtSelect<'a> {
    pub cols: Vec<ResultColumn<'a>>,
    pub table: Cow<'a, str>,
    pub conditions: Vec<Expression<'a>>,
}

impl StmtSelect<'_> {
    pub fn to_owned(&self) -> StmtSelect<'static> {
        StmtSelect {
            cols: self.cols.iter().map(|c| c.to_owned()).collect(),
            table: self.table.to_string().into(),
            conditions: self.conditions.iter().map(|c| c.to_owned()).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StmtCreateTable<'a> {
    pub name: Cow<'a, str>,
    pub columns: Vec<ColumnDefinition<'a>>,
}

impl StmtCreateTable<'_> {
    pub fn to_owned(&self) -> StmtCreateTable<'static> {
        StmtCreateTable {
            name: self.name.to_string().into(),
            columns: self.columns.iter().map(|c| c.to_owned()).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StmtCreateIndex<'a> {
    pub name: Cow<'a, str>,
    pub table: Cow<'a, str>,
    pub column: Cow<'a, str>,
}

impl StmtCreateIndex<'_> {
    pub fn to_owned(&self) -> StmtCreateIndex<'static> {
        StmtCreateIndex {
            name: self.name.to_string().into(),
            table: self.table.to_string().into(),
            column: self.column.to_string().into(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Query<'a> {
    DotCmd(Cow<'a, str>),
    Select(StmtSelect<'a>),
    CreateTable(StmtCreateTable<'a>),
    CreateIndex(StmtCreateIndex<'a>),
}

impl Query<'_> {
    pub fn to_owned(&self) -> Query<'static> {
        match self {
            Query::DotCmd(v) => Query::DotCmd(v.to_string().into()),
            Query::Select(v) => Query::Select(v.to_owned()),
            Query::CreateTable(v) => Query::CreateTable(v.to_owned()),
            Query::CreateIndex(v) => Query::CreateIndex(v.to_owned()),
        }
    }
}
