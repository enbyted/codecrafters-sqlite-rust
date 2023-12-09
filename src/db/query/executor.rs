use std::collections::HashMap;

use crate::{
    db::table::{TableRow, TableRowCell},
    error::{DbError, Result},
    sql::data::{BinaryOperator, Expression, FunctionArguments, Literal},
};

pub trait ExpressionExecutor {
    fn is_aggregate(&self) -> bool {
        false
    }
    fn begin_aggregate_group(&mut self) {}
    fn on_row(&mut self, row: &TableRow);
    fn value(&self) -> TableRowCell;
}

#[derive(Debug, Clone, Copy)]
pub enum ColumnRef {
    ColumnIndex(usize),
    Rowid,
}

type FunctionCreator<'a> =
    fn(&ExecutorFactory<'a>, &FunctionArguments) -> Result<'static, Box<dyn ExpressionExecutor>>;

#[derive(Debug)]
pub struct ExecutorFactory<'a> {
    column_map: &'a HashMap<&'a str, ColumnRef>,
    function_creators: HashMap<&'static str, FunctionCreator<'a>>,
}

impl<'a> ExecutorFactory<'a> {
    pub fn new(column_map: &'a HashMap<&'a str, ColumnRef>) -> ExecutorFactory<'a> {
        let mut function_creators: HashMap<_, FunctionCreator<'a>> = HashMap::new();

        function_creators.insert("count", CountExecutor::from_arguments);

        ExecutorFactory {
            column_map,
            function_creators,
        }
    }

    pub fn create_executor_for_expr(
        &self,
        expr: &Expression,
    ) -> Result<'static, Box<dyn ExpressionExecutor>> {
        match expr {
            Expression::ColRef {
                schema: None,
                table: None,
                column,
            } => match self.column_map.get(column) {
                Some(ColumnRef::ColumnIndex(column_index)) => {
                    Ok(Box::new(ColumnExtractionExecutor::new(*column_index)))
                }
                Some(ColumnRef::Rowid) => Ok(Box::new(RowIdExtractor::new())),
                None => {
                    if column.eq_ignore_ascii_case("rowid") || column.eq_ignore_ascii_case("oid") {
                        Ok(Box::new(RowIdExtractor::new()))
                    } else {
                        Err(DbError::ColumnNotFound(column.to_string()))
                    }
                }
            },
            Expression::ColRef { .. } => todo!("reading rowid columns is not supported yet"),
            Expression::Function { name, arguments } => {
                if let Some(creator) = self
                    .function_creators
                    .get(name.to_ascii_lowercase().as_str())
                {
                    creator(self, arguments)
                } else {
                    Err(DbError::UnknownFunction(name.to_string()))
                }
            }
            Expression::BinaryOp {
                left,
                right,
                operator,
            } => {
                let left = self.create_executor_for_expr(left)?;
                let right = self.create_executor_for_expr(right)?;

                Ok(Box::new(BinaryOpExecutor::new(
                    left,
                    right,
                    operator.clone(),
                )))
            }
            Expression::Literal(lit) => Ok(Box::new(LiteralExecutor::new(lit.clone()))),
        }
    }
}

struct CountExecutor {
    count: i64,
    source_expression: Option<Box<dyn ExpressionExecutor>>,
}

impl CountExecutor {
    pub fn new(source_expression: Option<Box<dyn ExpressionExecutor>>) -> CountExecutor {
        CountExecutor {
            count: 0,
            source_expression,
        }
    }

    fn from_arguments(
        factory: &ExecutorFactory<'_>,
        arguments: &FunctionArguments,
    ) -> Result<'static, Box<dyn ExpressionExecutor>> {
        match arguments {
            FunctionArguments::Star => Ok(Box::new(Self::new(None))),
            FunctionArguments::List(args) => {
                if args.len() == 0 {
                    Err(DbError::NotEnoughArguments(0, 1))
                } else if args.len() == 1 {
                    Ok(Box::new(Self::new(Some(
                        factory.create_executor_for_expr(&args[0])?,
                    ))))
                } else {
                    Err(DbError::TooManyArguments(args.len(), 1))
                }
            }
        }
    }
}

impl ExpressionExecutor for CountExecutor {
    fn is_aggregate(&self) -> bool {
        true
    }

    fn begin_aggregate_group(&mut self) {
        self.count = 0;
    }

    fn on_row(&mut self, row: &TableRow) {
        if let Some(expr) = &mut self.source_expression {
            expr.on_row(row);
            if let TableRowCell::Null = expr.value() {
                // Nulls are not counted
            } else {
                self.count += 1;
            }
        } else {
            self.count += 1;
        }
    }

    fn value(&self) -> TableRowCell {
        TableRowCell::Integer(self.count)
    }
}

struct RowIdExtractor(TableRowCell);

impl RowIdExtractor {
    pub fn new() -> RowIdExtractor {
        RowIdExtractor(TableRowCell::Null)
    }
}

impl ExpressionExecutor for RowIdExtractor {
    fn on_row(&mut self, row: &TableRow) {
        self.0 = TableRowCell::Integer(row.rowid());
    }

    fn value(&self) -> TableRowCell {
        self.0.clone()
    }
}

struct ColumnExtractionExecutor(usize, TableRowCell);

impl ColumnExtractionExecutor {
    pub fn new(column_index: usize) -> ColumnExtractionExecutor {
        ColumnExtractionExecutor(column_index, TableRowCell::Null)
    }
}

impl ExpressionExecutor for ColumnExtractionExecutor {
    fn on_row(&mut self, row: &TableRow) {
        if let Some(col) = row.cells().skip(self.0).next() {
            self.1 = col.clone();
        } else {
            self.1 = TableRowCell::Null;
        }
    }

    fn value(&self) -> TableRowCell {
        self.1.clone()
    }
}

struct BinaryOpExecutor {
    left: Box<dyn ExpressionExecutor>,
    right: Box<dyn ExpressionExecutor>,
    op: BinaryOperator,
}

impl BinaryOpExecutor {
    pub fn new(
        left: Box<dyn ExpressionExecutor>,
        right: Box<dyn ExpressionExecutor>,
        op: BinaryOperator,
    ) -> BinaryOpExecutor {
        BinaryOpExecutor { left, right, op }
    }
}

impl ExpressionExecutor for BinaryOpExecutor {
    fn is_aggregate(&self) -> bool {
        self.left.is_aggregate() || self.right.is_aggregate()
    }

    fn begin_aggregate_group(&mut self) {
        self.left.begin_aggregate_group();
        self.right.begin_aggregate_group();
    }

    fn on_row(&mut self, row: &TableRow) {
        self.left.on_row(row);
        self.right.on_row(row);
    }

    fn value(&self) -> TableRowCell {
        match self.op {
            BinaryOperator::Equals => {
                let left = self.left.value();
                let right = self.right.value();
                if left == right {
                    TableRowCell::Integer(1)
                } else {
                    TableRowCell::Integer(0)
                }
            }
        }
    }
}

struct LiteralExecutor(TableRowCell);

impl LiteralExecutor {
    pub fn new(literal: Literal) -> LiteralExecutor {
        match literal {
            Literal::String(str) => LiteralExecutor(TableRowCell::String(str.to_owned())),
            Literal::Integer(val) => LiteralExecutor(TableRowCell::Integer(val)),
        }
    }
}

impl ExpressionExecutor for LiteralExecutor {
    fn is_aggregate(&self) -> bool {
        false
    }

    fn begin_aggregate_group(&mut self) {}

    fn on_row(&mut self, _row: &TableRow) {}

    fn value(&self) -> TableRowCell {
        self.0.clone()
    }
}
