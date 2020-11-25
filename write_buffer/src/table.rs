use generated_types::wal as wb;
use query::exec::{make_schema_pivot, GroupedSeriesSetPlan, SeriesSetPlan};
use tracing::debug;

use std::{collections::BTreeSet, collections::HashMap, sync::Arc};

use crate::{
    column,
    column::Column,
    dictionary::{Dictionary, Error as DictionaryError},
    partition::PartitionIdSet,
    partition::{Partition, PartitionPredicate},
};
use data_types::TIME_COLUMN_NAME;
use snafu::{OptionExt, ResultExt, Snafu};

use arrow_deps::{
    arrow,
    arrow::{
        array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder},
        datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
        record_batch::RecordBatch,
    },
    datafusion::{
        self,
        logical_plan::{Expr, LogicalPlan, LogicalPlanBuilder},
        prelude::*,
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Table {} not found", table))]
    TableNotFound { table: String },

    #[snafu(display(
        "Column {} said it was type {} but extracting a value of that type failed",
        column,
        expected
    ))]
    WalValueTypeMismatch { column: String, expected: String },

    #[snafu(display(
        "Tag value ID {} not found in dictionary of partition {}",
        value,
        partition
    ))]
    TagValueIdNotFoundInDictionary {
        value: u32,
        partition: String,
        source: DictionaryError,
    },

    #[snafu(display(
        "Column type mismatch for column {}: can't insert {} into column with type {}",
        column,
        inserted_value_type,
        existing_column_type
    ))]
    ColumnTypeMismatch {
        column: String,
        existing_column_type: String,
        inserted_value_type: String,
    },

    #[snafu(display("Column error on column {}: {}", column, source))]
    ColumnError {
        column: String,
        source: column::Error,
    },

    #[snafu(display(
        "Internal error: Expected column {} to be type {} but was {}",
        column_id,
        expected_column_type,
        actual_column_type
    ))]
    InternalColumnTypeMismatch {
        column_id: u32,
        expected_column_type: String,
        actual_column_type: String,
    },

    #[snafu(display(
        "Column name '{}' not found in dictionary of partition {}",
        column_name,
        partition
    ))]
    ColumnNameNotFoundInDictionary {
        column_name: String,
        partition: String,
        source: DictionaryError,
    },

    #[snafu(display(
        "Internal: Column id '{}' not found in dictionary of partition {}",
        column_id,
        partition
    ))]
    ColumnIdNotFoundInDictionary {
        column_id: u32,
        partition: String,
        source: DictionaryError,
    },

    #[snafu(display(
        "Schema mismatch: for column {}: can't insert {} into column with type {}",
        column,
        inserted_value_type,
        existing_column_type
    ))]
    SchemaMismatch {
        column: u32,
        existing_column_type: String,
        inserted_value_type: String,
    },

    #[snafu(display("Error building plan: {}", source))]
    BuildingPlan {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(display("Schema mismatch: for column {}: {}", column, source))]
    InternalSchemaMismatch {
        column: u32,
        source: crate::column::Error,
    },

    #[snafu(display(
        "No index entry found for column {} with id {}",
        column_name,
        column_id
    ))]
    InternalNoColumnInIndex { column_name: String, column_id: u32 },

    #[snafu(display("Error creating column from wal for column {}: {}", column, source))]
    CreatingFromWal {
        column: u32,
        source: crate::column::Error,
    },

    #[snafu(display("Error evaluating column predicate for column {}: {}", column, source))]
    ColumnPredicateEvaluation {
        column: u32,
        source: crate::column::Error,
    },

    #[snafu(display("Row insert to table {} missing column name", table))]
    ColumnNameNotInRow { table: u32 },

    #[snafu(display(
        "Group column '{}' not found in tag columns: {}",
        column_name,
        all_tag_column_names
    ))]
    GroupColumnNotFound {
        column_name: String,
        all_tag_column_names: String,
    },

    #[snafu(display("Duplicate group column '{}'", column_name))]
    DuplicateGroupColumn { column_name: String },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Table {
    /// Name of the table as a u32 in the partition dictionary
    pub id: u32,

    /// Maps column name (as a u32 in the partition dictionary) to an index in self.columns
    pub column_id_to_index: HashMap<u32, usize>,

    /// Actual column storage
    pub columns: Vec<Column>,
}

type ArcStringVec = Vec<Arc<String>>;

impl Table {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            column_id_to_index: HashMap::new(),
            columns: Vec::new(),
        }
    }

    fn append_row(
        &mut self,
        dictionary: &mut Dictionary,
        values: &flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<wb::Value<'_>>>,
    ) -> Result<()> {
        let row_count = self.row_count();

        // insert new columns and validate existing ones
        for value in values {
            let column_name = value
                .column()
                .context(ColumnNameNotInRow { table: self.id })?;
            let column_id = dictionary.lookup_value_or_insert(column_name);

            let column = match self.column_id_to_index.get(&column_id) {
                Some(idx) => &mut self.columns[*idx],
                None => {
                    // Add the column and make all values for existing rows None
                    let idx = self.columns.len();
                    self.column_id_to_index.insert(column_id, idx);
                    self.columns.push(
                        Column::with_value(dictionary, row_count, value)
                            .context(CreatingFromWal { column: column_id })?,
                    );

                    continue;
                }
            };

            column.push(dictionary, &value).context(ColumnError {
                column: column_name,
            })?;
        }

        // make sure all the columns are of the same length
        for col in &mut self.columns {
            col.push_none_if_len_equal(row_count);
        }

        Ok(())
    }

    pub fn row_count(&self) -> usize {
        self.columns.first().map_or(0, |v| v.len())
    }

    /// Returns a reference to the specified column
    fn column(&self, column_id: u32) -> Result<&Column> {
        Ok(self
            .column_id_to_index
            .get(&column_id)
            .map(|&column_index| &self.columns[column_index])
            .expect("invalid column id"))
    }

    /// Returns a reference to the specified column as a slice of
    /// i64s. Errors if the type is not i64
    pub fn column_i64(&self, column_id: u32) -> Result<&[Option<i64>]> {
        let column = self.column(column_id)?;
        match column {
            Column::I64(vals, _) => Ok(vals),
            _ => InternalColumnTypeMismatch {
                column_id,
                expected_column_type: "i64",
                actual_column_type: column.type_description(),
            }
            .fail(),
        }
    }

    pub fn append_rows(
        &mut self,
        dictionary: &mut Dictionary,
        rows: &flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<wb::Row<'_>>>,
    ) -> Result<()> {
        for row in rows {
            if let Some(values) = row.values() {
                self.append_row(dictionary, &values)?;
            }
        }

        Ok(())
    }

    /// Creates and adds a datafuson filtering expression, if any out of the
    /// combination of predicate and timestamp. Returns the builder
    fn add_datafusion_predicate(
        plan_builder: LogicalPlanBuilder,
        partition_predicate: &PartitionPredicate,
    ) -> Result<LogicalPlanBuilder> {
        match partition_predicate.filter_expr() {
            Some(df_predicate) => plan_builder.filter(df_predicate).context(BuildingPlan),
            None => Ok(plan_builder),
        }
    }

    /// Creates a DataFusion LogicalPlan that returns column *names* as a
    /// single column of Strings
    ///
    /// The created plan looks like:
    ///
    ///  Extension(PivotSchema)
    ///    (Optional Projection to get rid of time)
    ///        Filter(predicate)
    ///          InMemoryScan
    pub fn tag_column_names_plan(
        &self,
        partition_predicate: &PartitionPredicate,
        partition: &Partition,
    ) -> Result<LogicalPlan> {
        let need_time_column = partition_predicate.range.is_some();

        let time_column_id = partition_predicate.time_column_id;

        // figure out the tag columns
        let requested_columns_with_index = self
            .column_id_to_index
            .iter()
            .filter_map(|(&column_id, &column_index)| {
                // keep tag columns and the timestamp column, if needed to evaluate a timestamp predicate
                let need_column = if let Column::Tag(_, _) = self.columns[column_index] {
                    true
                } else {
                    need_time_column && column_id == time_column_id
                };

                if need_column {
                    // the id came out of our map, so it should always be valid
                    let column_name = partition.dictionary.lookup_id(column_id).unwrap();
                    Some((column_name, column_index))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // TODO avoid materializing here
        let data = self.to_arrow_impl(partition, &requested_columns_with_index)?;

        let schema = data.schema();

        let projection = None;
        let projected_schema = schema.clone();

        let plan_builder = LogicalPlanBuilder::from(&LogicalPlan::InMemoryScan {
            data: vec![vec![data]],
            schema,
            projection,
            projected_schema,
        });

        // Shouldn't have field selections here (as we are getting the tags...)
        assert!(!partition_predicate.has_field_restriction());

        let plan_builder = Self::add_datafusion_predicate(plan_builder, partition_predicate)?;

        // add optional selection to remove time column
        let plan_builder = if !need_time_column {
            plan_builder
        } else {
            // Create expressions for all columns except time
            let select_exprs = requested_columns_with_index
                .iter()
                .filter_map(|&(column_name, _)| {
                    if column_name != TIME_COLUMN_NAME {
                        Some(col(column_name))
                    } else {
                        None
                    }
                })
                .collect();

            plan_builder.project(select_exprs).context(BuildingPlan)?
        };

        let plan = plan_builder.build().context(BuildingPlan)?;

        // And finally pivot the plan
        let plan = make_schema_pivot(plan);

        debug!(
            "Created column_name plan for table '{}':\n{}",
            partition.dictionary.lookup_id(self.id).unwrap(),
            plan.display_indent_schema()
        );

        Ok(plan)
    }

    /// Creates a DataFusion LogicalPlan that returns column *values* as a
    /// single column of Strings
    ///
    /// The created plan looks like:
    ///
    ///    Projection
    ///        Filter(predicate)
    ///          InMemoryScan
    pub fn tag_values_plan(
        &self,
        column_name: &str,
        partition_predicate: &PartitionPredicate,
        partition: &Partition,
    ) -> Result<LogicalPlan> {
        // TODO avoid materializing all the columns here (ideally
        // DataFusion can prune them out)
        let data = self.all_to_arrow(partition)?;

        let schema = data.schema();

        let projection = None;
        let projected_schema = schema.clone();
        let select_exprs = vec![col(column_name)];

        // And build the plan!
        let plan_builder = LogicalPlanBuilder::from(&LogicalPlan::InMemoryScan {
            data: vec![vec![data]],
            schema,
            projection,
            projected_schema,
        });

        // shouldn't have columns selection (as this is getting tag values...)
        assert!(!partition_predicate.has_field_restriction());

        let plan_builder = Self::add_datafusion_predicate(plan_builder, partition_predicate)?;

        plan_builder
            .project(select_exprs)
            .context(BuildingPlan)?
            .build()
            .context(BuildingPlan)
    }

    /// Creates a SeriesSet plan that produces an output table with rows that match the predicate
    ///
    /// The output looks like:
    /// (tag_col1, tag_col2, ... field1, field2, ... timestamp)
    ///
    /// The order of the tag_columns is orderd by name.
    ///
    /// The data is sorted on tag_col1, tag_col2, ...) so that all
    /// rows for a particular series (groups where all tags are the
    /// same) occur together in the plan
    pub fn series_set_plan(
        &self,
        partition_predicate: &PartitionPredicate,
        partition: &Partition,
    ) -> Result<SeriesSetPlan> {
        self.series_set_plan_impl(partition_predicate, None, partition)
    }

    /// Creates the plans for computing series set, pulling prefix_columns, if any, as a prefix of the ordering
    /// The created plan looks like:
    ///
    ///    Projection (select the columns columns needed)
    ///      Order by (tag_columns, timestamp_column)
    ///        Filter(predicate)
    ///          InMemoryScan
    pub fn series_set_plan_impl(
        &self,
        partition_predicate: &PartitionPredicate,
        prefix_columns: Option<&[String]>,
        partition: &Partition,
    ) -> Result<SeriesSetPlan> {
        // I wonder if all this string creation will be too slow?
        let table_name = partition
            .dictionary
            .lookup_id(self.id)
            .expect("looking up table name in dictionary")
            .to_string();

        let table_name = Arc::new(table_name);
        let (mut tag_columns, field_columns) =
            self.tag_and_field_column_names(partition_predicate, partition)?;

        // reorder tag_columns to have the prefix columns, if requested
        if let Some(prefix_columns) = prefix_columns {
            tag_columns = reorder_prefix(prefix_columns, tag_columns)?;
        }

        // TODO avoid materializing all the columns here (ideally
        // DataFusion can prune them out)
        let data = self.all_to_arrow(partition)?;

        let schema = data.schema();

        let projection = None;
        let projected_schema = schema.clone();

        // And build the plan from the bottom up
        let plan_builder = LogicalPlanBuilder::from(&LogicalPlan::InMemoryScan {
            data: vec![vec![data]],
            schema,
            projection,
            projected_schema,
        });

        // Filtering
        let plan_builder = Self::add_datafusion_predicate(plan_builder, partition_predicate)?;

        let mut sort_exprs = Vec::new();
        sort_exprs.extend(tag_columns.iter().map(|c| c.into_sort_expr()));
        sort_exprs.push(TIME_COLUMN_NAME.into_sort_expr());

        // Order by
        let plan_builder = plan_builder.sort(sort_exprs).context(BuildingPlan)?;

        // Selection
        let mut select_exprs = Vec::new();
        select_exprs.extend(tag_columns.iter().map(|c| c.into_expr()));
        select_exprs.extend(field_columns.iter().map(|c| c.into_expr()));
        select_exprs.push(TIME_COLUMN_NAME.into_expr());

        let plan_builder = plan_builder.project(select_exprs).context(BuildingPlan)?;

        // and finally create the plan
        let plan = plan_builder.build().context(BuildingPlan)?;

        Ok(SeriesSetPlan {
            table_name,
            plan,
            tag_columns,
            field_columns,
        })
    }

    /// Creates a GroupedSeriesSet plan that produces an output table with rows that match the predicate
    ///
    /// The output looks like:
    /// (group_tag_column1, group_tag_column2, ... tag_col1, tag_col2, ... field1, field2, ... timestamp)
    ///
    /// The order of the tag_columns is ordered by name.
    ///
    /// The data is sorted on tag_col1, tag_col2, ...) so that all
    /// rows for a particular series (groups where all tags are the
    /// same) occur together in the plan
    ///
    /// The created plan looks like:
    ///
    ///    Projection (select the columns columns needed)
    ///      Order by (tag_columns, timestamp_column)
    ///        Filter(predicate)
    ///          InMemoryScan
    pub fn grouped_series_set_plan(
        &self,
        partition_predicate: &PartitionPredicate,
        group_columns: &[String],
        partition: &Partition,
    ) -> Result<GroupedSeriesSetPlan> {
        let series_set_plan =
            self.series_set_plan_impl(partition_predicate, Some(&group_columns), partition)?;
        let num_prefix_tag_group_columns = group_columns.len();

        Ok(GroupedSeriesSetPlan {
            series_set_plan,
            num_prefix_tag_group_columns,
        })
    }

    /// Creates a plan that produces an output table with rows that
    /// match the predicate for all fields in the table.
    ///
    /// The output looks like (field0, field1, ..., time)
    ///
    /// The data is not sorted in any particular order
    ///
    /// The created plan looks like:
    ///
    ///    Projection (select the field columns needed)
    ///        Filter(predicate) [optional]
    ///          InMemoryScan
    pub fn field_names_plan(
        &self,
        partition_predicate: &PartitionPredicate,
        partition: &Partition,
    ) -> Result<LogicalPlan> {
        // TODO avoid materializing all the columns here (ideally
        // DataFusion can prune them out)
        let data = self.all_to_arrow(partition)?;

        let schema = data.schema();

        let projection = None;
        let projected_schema = schema.clone();

        // And build the plan from the bottom up
        let plan_builder = LogicalPlanBuilder::from(&LogicalPlan::InMemoryScan {
            data: vec![vec![data]],
            schema,
            projection,
            projected_schema,
        });

        // Filtering
        let plan_builder = Self::add_datafusion_predicate(plan_builder, partition_predicate)?;

        // Selection
        let select_exprs = self
            .field_and_time_column_names(partition_predicate, partition)
            .into_iter()
            .map(|c| c.into_expr())
            .collect::<Vec<_>>();

        let plan_builder = plan_builder.project(select_exprs).context(BuildingPlan)?;

        // and finally create the plan
        plan_builder.build().context(BuildingPlan)
    }

    // Returns (tag_columns, field_columns) vectors with the names of
    // all tag and field columns, respectively. The vectors are sorted
    // by name.
    fn tag_and_field_column_names(
        &self,
        partition_predicate: &PartitionPredicate,
        partition: &Partition,
    ) -> Result<(ArcStringVec, ArcStringVec)> {
        let mut tag_columns = Vec::with_capacity(self.column_id_to_index.len());
        let mut field_columns = Vec::with_capacity(self.column_id_to_index.len());

        for (&column_id, &column_index) in &self.column_id_to_index {
            let column_name = partition
                .dictionary
                .lookup_id(column_id)
                .expect("Find column name in dictionary");

            if column_name != TIME_COLUMN_NAME {
                let column_name = Arc::new(column_name.to_string());

                match self.columns[column_index] {
                    Column::Tag(_, _) => tag_columns.push(column_name),
                    _ => {
                        if partition_predicate.should_include_field(column_id) {
                            field_columns.push(column_name)
                        }
                    }
                }
            }
        }

        // tag columns are always sorted by name (aka sorted by tag
        // key) in the output schema, so ensure the columns are sorted
        // (the select exprs)
        tag_columns.sort();

        // Sort the field columns too so that the output always comes
        // out in a predictable order
        field_columns.sort();

        Ok((tag_columns, field_columns))
    }

    // Returns (field_columns and time) in sorted order
    fn field_and_time_column_names(
        &self,
        partition_predicate: &PartitionPredicate,
        partition: &Partition,
    ) -> ArcStringVec {
        let mut field_columns = self
            .column_id_to_index
            .iter()
            .filter_map(|(&column_id, &column_index)| {
                match self.columns[column_index] {
                    Column::Tag(_, _) => None, // skip tags
                    _ => {
                        if partition_predicate.should_include_field(column_id)
                            || partition_predicate.is_time_column(column_id)
                        {
                            let column_name = partition
                                .dictionary
                                .lookup_id(column_id)
                                .expect("Find column name in dictionary");
                            Some(Arc::new(column_name.to_string()))
                        } else {
                            None
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        // Sort the field columns too so that the output always comes
        // out in a predictable order
        field_columns.sort();

        field_columns
    }

    /// Converts this table to an arrow record batch.
    pub fn to_arrow(
        &self,
        partition: &Partition,
        requested_columns: &[&str],
    ) -> Result<RecordBatch> {
        // if requested columns is empty, retrieve all columns in the table
        if requested_columns.is_empty() {
            self.all_to_arrow(partition)
        } else {
            let columns_with_index = self.column_names_with_index(partition, requested_columns)?;

            self.to_arrow_impl(partition, &columns_with_index)
        }
    }

    fn column_names_with_index<'a>(
        &self,
        partition: &Partition,
        columns: &[&'a str],
    ) -> Result<Vec<(&'a str, usize)>> {
        columns
            .iter()
            .map(|&column_name| {
                let column_id = partition.dictionary.lookup_value(column_name).context(
                    ColumnNameNotFoundInDictionary {
                        column_name,
                        partition: &partition.key,
                    },
                )?;

                let column_index =
                    *self
                        .column_id_to_index
                        .get(&column_id)
                        .context(InternalNoColumnInIndex {
                            column_name,
                            column_id,
                        })?;

                Ok((column_name, column_index))
            })
            .collect()
    }

    /// Convert all columns to an arrow record batch
    pub fn all_to_arrow(&self, partition: &Partition) -> Result<RecordBatch> {
        let mut requested_columns_with_index = self
            .column_id_to_index
            .iter()
            .map(|(&column_id, &column_index)| {
                let column_name = partition.dictionary.lookup_id(column_id).context(
                    ColumnIdNotFoundInDictionary {
                        column_id,
                        partition: &partition.key,
                    },
                )?;
                Ok((column_name, column_index))
            })
            .collect::<Result<Vec<_>>>()?;

        requested_columns_with_index.sort_by(|(a, _), (b, _)| a.cmp(b));

        self.to_arrow_impl(partition, &requested_columns_with_index)
    }

    /// Converts this table to an arrow record batch,
    ///
    /// requested columns with index are tuples of column_name, column_index
    pub fn to_arrow_impl(
        &self,
        partition: &Partition,
        requested_columns_with_index: &[(&str, usize)],
    ) -> Result<RecordBatch> {
        let mut fields = Vec::with_capacity(requested_columns_with_index.len());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(requested_columns_with_index.len());

        for &(column_name, column_index) in requested_columns_with_index.iter() {
            let arrow_col: ArrayRef = match &self.columns[column_index] {
                Column::String(vals, _) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Utf8, true));
                    let mut builder = StringBuilder::with_capacity(vals.len(), vals.len() * 10);

                    for v in vals {
                        match v {
                            None => builder.append_null(),
                            Some(s) => builder.append_value(s),
                        }
                        .context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::Tag(vals, _) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Utf8, true));
                    let mut builder = StringBuilder::with_capacity(vals.len(), vals.len() * 10);

                    for v in vals {
                        match v {
                            None => builder.append_null(),
                            Some(value_id) => {
                                let tag_value = partition.dictionary.lookup_id(*value_id).context(
                                    TagValueIdNotFoundInDictionary {
                                        value: *value_id,
                                        partition: &partition.key,
                                    },
                                )?;
                                builder.append_value(tag_value)
                            }
                        }
                        .context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::F64(vals, _) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Float64, true));
                    let mut builder = Float64Builder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::I64(vals, _) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Int64, true));
                    let mut builder = Int64Builder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::Bool(vals, _) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Boolean, true));
                    let mut builder = BooleanBuilder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
            };

            columns.push(arrow_col);
        }

        let schema = ArrowSchema::new(fields);

        RecordBatch::try_new(Arc::new(schema), columns).context(ArrowError {})
    }

    /// returns true if any row in this table could possible match the
    /// predicate. true does not mean any rows will *actually* match,
    /// just that the entire table can not be ruled out.
    ///
    /// false means that no rows in this table could possibly match
    pub fn could_match_predicate(&self, partition_predicate: &PartitionPredicate) -> Result<bool> {
        Ok(
            self.matches_column_selection(partition_predicate.field_restriction.as_ref())
                && self.matches_table_name_predicate(
                    partition_predicate.table_name_predicate.as_ref(),
                )
                && self.matches_timestamp_predicate(partition_predicate)?
                && self.has_columns(partition_predicate.required_columns.as_ref()),
        )
    }

    /// Returns true if the table contains at least one of the fields
    /// requested or there are no specific fields requested.
    fn matches_column_selection(&self, column_selection: Option<&BTreeSet<u32>>) -> bool {
        match column_selection {
            Some(column_selection) => {
                // figure out if any of the columns exists
                self.column_id_to_index
                    .keys()
                    .any(|column_id| column_selection.contains(column_id))
            }
            None => true, // no specific selection
        }
    }

    fn matches_table_name_predicate(&self, table_name_predicate: Option<&BTreeSet<u32>>) -> bool {
        match table_name_predicate {
            Some(table_name_predicate) => table_name_predicate.contains(&self.id),
            None => true, // no table predicate
        }
    }

    /// returns true if there are any timestamps in this table that
    /// fall within the timestamp range
    fn matches_timestamp_predicate(
        &self,
        partition_predicate: &PartitionPredicate,
    ) -> Result<bool> {
        match &partition_predicate.range {
            None => Ok(true),
            Some(range) => {
                let time_column_id = partition_predicate.time_column_id;
                let time_column = self.column(time_column_id)?;
                time_column.has_i64_range(range.start, range.end).context(
                    ColumnPredicateEvaluation {
                        column: time_column_id,
                    },
                )
            }
        }
    }

    /// returns true if no columns are specified, or the table has all
    /// columns specified
    fn has_columns(&self, columns: Option<&PartitionIdSet>) -> bool {
        if let Some(columns) = columns {
            match columns {
                PartitionIdSet::AtLeastOneMissing => return false,
                PartitionIdSet::Present(symbols) => {
                    for symbol in symbols {
                        if !self.column_id_to_index.contains_key(symbol) {
                            return false;
                        }
                    }
                }
            }
        }
        true
    }

    /// returns true if there are any rows in column that are non-null
    /// and within the timestamp range specified by pred
    pub fn column_matches_predicate<T>(
        &self,
        column: &[Option<T>],
        partition_predicate: &PartitionPredicate,
    ) -> Result<bool> {
        match partition_predicate.range {
            None => Ok(true),
            Some(range) => {
                let time_column_id = partition_predicate.time_column_id;
                let time_column = self.column(time_column_id)?;
                time_column
                    .has_non_null_i64_range(column, range.start, range.end)
                    .context(ColumnPredicateEvaluation {
                        column: time_column_id,
                    })
            }
        }
    }
}

/// Reorders tag_columns so that its prefix matches exactly
/// prefix_columns. Returns an error if there are duplicates, or other
/// untoward inputs
fn reorder_prefix(
    prefix_columns: &[String],
    tag_columns: Vec<Arc<String>>,
) -> Result<Vec<Arc<String>>> {
    // tag_used_set[i[ is true if we have used the value in tag_columns[i]
    let mut tag_used_set = vec![false; tag_columns.len()];

    // Note that this is an O(N^2) algorithm. We are assuming the
    // number of tag columns is reasonably small

    // map from prefix_column[idx] -> index in tag_columns
    let prefix_map = prefix_columns
        .iter()
        .map(|pc| {
            let found_location = tag_columns
                .iter()
                .enumerate()
                .find(|(_, c)| pc == c.as_ref());

            if let Some((index, _)) = found_location {
                if tag_used_set[index] {
                    DuplicateGroupColumn { column_name: pc }.fail()
                } else {
                    tag_used_set[index] = true;
                    Ok(index)
                }
            } else {
                GroupColumnNotFound {
                    column_name: pc,
                    all_tag_column_names: tag_columns
                        .iter()
                        .map(|s| s.as_ref() as &str)
                        .collect::<Vec<_>>()
                        .as_slice()
                        .join(", "),
                }
                .fail()
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let mut new_tag_columns = prefix_map
        .iter()
        .map(|&i| tag_columns[i].clone())
        .collect::<Vec<_>>();

    new_tag_columns.extend(tag_columns.into_iter().enumerate().filter_map(|(i, c)| {
        // already used in prefix
        if tag_used_set[i] {
            None
        } else {
            Some(c)
        }
    }));

    Ok(new_tag_columns)
}

/// Traits to help creating DataFuson expressions from strings
trait IntoExpr {
    /// Creates a DataFuson expr
    fn into_expr(&self) -> Expr;

    /// creates a DataFusion SortExpr
    fn into_sort_expr(&self) -> Expr {
        Expr::Sort {
            expr: Box::new(self.into_expr()),
            asc: true, // Sort ASCENDING
            nulls_first: true,
        }
    }
}

impl IntoExpr for Arc<String> {
    fn into_expr(&self) -> Expr {
        col(self.as_ref())
    }
}

impl IntoExpr for str {
    fn into_expr(&self) -> Expr {
        col(self)
    }
}

#[cfg(test)]
mod tests {
    use arrow::util::pretty::pretty_format_batches;
    use data_types::data::split_lines_into_write_entry_partitions;
    use influxdb_line_protocol::{parse_lines, ParsedLine};
    use query::{exec::Executor, predicate::PredicateBuilder};
    use test_helpers::str_vec_to_arc_vec;

    use super::*;

    #[test]
    fn test_has_columns() {
        // setup a test table
        let mut partition = Partition::new("dummy_partition_key");
        let dictionary = &mut partition.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let state_symbol = dictionary.id("state").unwrap();
        let new_symbol = dictionary.lookup_value_or_insert("not_a_columns");

        assert!(table.has_columns(None));

        let pred = PartitionIdSet::AtLeastOneMissing;
        assert!(!table.has_columns(Some(&pred)));

        let set = BTreeSet::<u32>::new();
        let pred = PartitionIdSet::Present(set);
        assert!(table.has_columns(Some(&pred)));

        let mut set = BTreeSet::new();
        set.insert(state_symbol);
        let pred = PartitionIdSet::Present(set);
        assert!(table.has_columns(Some(&pred)));

        let mut set = BTreeSet::new();
        set.insert(new_symbol);
        let pred = PartitionIdSet::Present(set);
        assert!(!table.has_columns(Some(&pred)));

        let mut set = BTreeSet::new();
        set.insert(state_symbol);
        set.insert(new_symbol);
        let pred = PartitionIdSet::Present(set);
        assert!(!table.has_columns(Some(&pred)));
    }

    #[test]
    fn test_matches_table_name_predicate() {
        // setup a test table
        let mut partition = Partition::new("dummy_partition_key");
        let dictionary = &mut partition.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("h2o"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];
        write_lines_to_table(&mut table, dictionary, lp_lines);

        let h2o_symbol = dictionary.id("h2o").unwrap();

        assert!(table.matches_table_name_predicate(None));

        let set = BTreeSet::new();
        assert!(!table.matches_table_name_predicate(Some(&set)));

        let mut set = BTreeSet::new();
        set.insert(h2o_symbol);
        assert!(table.matches_table_name_predicate(Some(&set)));

        // Some symbol that is not the same as h2o_symbol
        assert_ne!(37377, h2o_symbol);
        let mut set = BTreeSet::new();
        set.insert(37377);
        assert!(!table.matches_table_name_predicate(Some(&set)));
    }

    #[tokio::test]
    async fn test_series_set_plan() {
        // setup a test table
        let mut partition = Partition::new("dummy_partition_key");
        let dictionary = &mut partition.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
            "h2o,state=CA,city=LA temp=90.0 200",
            "h2o,state=CA,city=LA temp=90.0 350",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let predicate = PredicateBuilder::default().build();
        let partition_predicate = partition.compile_predicate(&predicate).unwrap();
        let series_set_plan = table
            .series_set_plan(&partition_predicate, &partition)
            .expect("creating the series set plan");

        assert_eq!(series_set_plan.table_name.as_ref(), "table_name");
        assert_eq!(
            series_set_plan.tag_columns,
            *str_vec_to_arc_vec(&["city", "state"])
        );
        assert_eq!(
            series_set_plan.field_columns,
            *str_vec_to_arc_vec(&["temp"])
        );

        // run the created plan, ensuring the output is as expected
        let results = run_plan(series_set_plan.plan).await;

        let expected = vec![
            "+--------+-------+------+------+",
            "| city   | state | temp | time |",
            "+--------+-------+------+------+",
            "| Boston | MA    | 70.4 | 100  |",
            "| Boston | MA    | 72.4 | 250  |",
            "| LA     | CA    | 90   | 200  |",
            "| LA     | CA    | 90   | 350  |",
            "+--------+-------+------+------+",
        ];
        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_series_set_plan_order() {
        // test that the columns and rows come out in the right order (tags then timestamp)

        // setup a test table
        let mut partition = Partition::new("dummy_partition_key");
        let dictionary = &mut partition.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,zz_tag=A,state=MA,city=Kingston temp=70.1 800",
            "h2o,state=MA,city=Kingston,zz_tag=B temp=70.2 100",
            "h2o,state=CA,city=Boston temp=70.3 250",
            "h2o,state=MA,city=Boston,zz_tag=A temp=70.4 1000",
            "h2o,state=MA,city=Boston temp=70.5,other=5.0 250",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let predicate = PredicateBuilder::default().build();
        let partition_predicate = partition.compile_predicate(&predicate).unwrap();
        let series_set_plan = table
            .series_set_plan(&partition_predicate, &partition)
            .expect("creating the series set plan");

        assert_eq!(series_set_plan.table_name.as_ref(), "table_name");
        assert_eq!(
            series_set_plan.tag_columns,
            *str_vec_to_arc_vec(&["city", "state", "zz_tag"])
        );
        assert_eq!(
            series_set_plan.field_columns,
            *str_vec_to_arc_vec(&["other", "temp"])
        );

        // run the created plan, ensuring the output is as expected
        let results = run_plan(series_set_plan.plan).await;

        let expected = vec![
            "+----------+-------+--------+-------+------+------+",
            "| city     | state | zz_tag | other | temp | time |",
            "+----------+-------+--------+-------+------+------+",
            "| Boston   | CA    |        |       | 70.3 | 250  |",
            "| Boston   | MA    |        | 5     | 70.5 | 250  |",
            "| Boston   | MA    | A      |       | 70.4 | 1000 |",
            "| Kingston | MA    | A      |       | 70.1 | 800  |",
            "| Kingston | MA    | B      |       | 70.2 | 100  |",
            "+----------+-------+--------+-------+------+------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_series_set_plan_filter() {
        // test that filters are applied reasonably

        // setup a test table
        let mut partition = Partition::new("dummy_partition_key");
        let dictionary = &mut partition.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
            "h2o,state=CA,city=LA temp=90.0 200",
            "h2o,state=CA,city=LA temp=90.0 350",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let predicate = PredicateBuilder::default()
            .add_expr(col("city").eq(lit("LA")))
            .timestamp_range(190, 210)
            .build();

        let partition_predicate = partition.compile_predicate(&predicate).unwrap();

        let series_set_plan = table
            .series_set_plan(&partition_predicate, &partition)
            .expect("creating the series set plan");

        assert_eq!(series_set_plan.table_name.as_ref(), "table_name");
        assert_eq!(
            series_set_plan.tag_columns,
            *str_vec_to_arc_vec(&["city", "state"])
        );
        assert_eq!(
            series_set_plan.field_columns,
            *str_vec_to_arc_vec(&["temp"])
        );

        // run the created plan, ensuring the output is as expected
        let results = run_plan(series_set_plan.plan).await;

        let expected = vec![
            "+------+-------+------+------+",
            "| city | state | temp | time |",
            "+------+-------+------+------+",
            "| LA   | CA    | 90   | 200  |",
            "+------+-------+------+------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_grouped_series_set_plan() {
        // test that filters are applied reasonably

        // setup a test table
        let mut partition = Partition::new("dummy_partition_key");
        let dictionary = &mut partition.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
            "h2o,state=CA,city=LA temp=90.0 200",
            "h2o,state=CA,city=LA temp=90.0 350",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let predicate = PredicateBuilder::default()
            .add_expr(col("city").eq(lit("LA")))
            .timestamp_range(190, 210)
            .build();
        let partition_predicate = partition.compile_predicate(&predicate).unwrap();

        let group_columns = vec![String::from("state")];

        let grouped_series_set_plan = table
            .grouped_series_set_plan(&partition_predicate, &group_columns, &partition)
            .expect("creating the grouped_series set plan");

        assert_eq!(grouped_series_set_plan.num_prefix_tag_group_columns, 1);

        // run the created plan, ensuring the output is as expected
        let results = run_plan(grouped_series_set_plan.series_set_plan.plan).await;

        let expected = vec![
            "+-------+------+------+------+",
            "| state | city | temp | time |",
            "+-------+------+------+------+",
            "| CA    | LA   | 90   | 200  |",
            "+-------+------+------+------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_field_name_plan() {
        // setup a test table
        let mut partition = Partition::new("dummy_partition_key");
        let dictionary = &mut partition.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            // Order this so field3 comes before field2
            // (and thus the columns need to get reordered)
            "h2o,tag1=foo,tag2=bar field1=70.6,field3=2 100",
            "h2o,tag1=foo,tag2=bar field1=70.4,field2=\"ss\" 100",
            "h2o,tag1=foo,tag2=bar field1=70.5,field2=\"ss\" 100",
            "h2o,tag1=foo,tag2=bar field1=70.6,field4=true 1000",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let predicate = PredicateBuilder::default().timestamp_range(0, 200).build();

        let partition_predicate = partition.compile_predicate(&predicate).unwrap();

        let field_names_set_plan = table
            .field_names_plan(&partition_predicate, &partition)
            .expect("creating the field_name plan");

        // run the created plan, ensuring the output is as expected
        let results = run_plan(field_names_set_plan).await;

        let expected = vec![
            "+--------+--------+--------+--------+------+",
            "| field1 | field2 | field3 | field4 | time |",
            "+--------+--------+--------+--------+------+",
            "| 70.6   |        | 2      |        | 100  |",
            "| 70.4   | ss     |        |        | 100  |",
            "| 70.5   | ss     |        |        | 100  |",
            "+--------+--------+--------+--------+------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[test]
    fn test_reorder_prefix() {
        assert_eq!(reorder_prefix_ok(&[], &[]), &[] as &[&str]);

        assert_eq!(reorder_prefix_ok(&[], &["one"]), &["one"]);
        assert_eq!(reorder_prefix_ok(&["one"], &["one"]), &["one"]);

        assert_eq!(reorder_prefix_ok(&[], &["one", "two"]), &["one", "two"]);
        assert_eq!(
            reorder_prefix_ok(&["one"], &["one", "two"]),
            &["one", "two"]
        );
        assert_eq!(
            reorder_prefix_ok(&["two"], &["one", "two"]),
            &["two", "one"]
        );
        assert_eq!(
            reorder_prefix_ok(&["two", "one"], &["one", "two"]),
            &["two", "one"]
        );

        assert_eq!(
            reorder_prefix_ok(&[], &["one", "two", "three"]),
            &["one", "two", "three"]
        );
        assert_eq!(
            reorder_prefix_ok(&["one"], &["one", "two", "three"]),
            &["one", "two", "three"]
        );
        assert_eq!(
            reorder_prefix_ok(&["two"], &["one", "two", "three"]),
            &["two", "one", "three"]
        );
        assert_eq!(
            reorder_prefix_ok(&["three", "one"], &["one", "two", "three"]),
            &["three", "one", "two"]
        );

        // errors
        assert_eq!(
            reorder_prefix_err(&["one"], &[]),
            "Group column \'one\' not found in tag columns: "
        );
        assert_eq!(
            reorder_prefix_err(&["one"], &["two", "three"]),
            "Group column \'one\' not found in tag columns: two, three"
        );
        assert_eq!(
            reorder_prefix_err(&["two", "one", "two"], &["one", "two"]),
            "Duplicate group column \'two\'"
        );
    }

    fn reorder_prefix_ok(prefix: &[&str], table_columns: &[&str]) -> Vec<String> {
        let prefix = prefix.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let table_columns =
            Arc::try_unwrap(str_vec_to_arc_vec(table_columns)).expect("unwrap the arc");

        let res = reorder_prefix(&prefix, table_columns);
        let message = format!("Expected OK, got {:?}", res);
        let res = res.expect(&message);

        res.into_iter()
            .map(|a| Arc::try_unwrap(a).expect("unwrapping arc"))
            .collect()
    }

    // returns the error string or panics if `reorder_prefix` doesn't return an error
    fn reorder_prefix_err(prefix: &[&str], table_columns: &[&str]) -> String {
        let prefix = prefix.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let table_columns =
            Arc::try_unwrap(str_vec_to_arc_vec(table_columns)).expect("unwrap the arc");

        let res = reorder_prefix(&prefix, table_columns);

        match res {
            Ok(r) => {
                panic!(
                    "Expected error result from reorder_prefix_err, but was OK: '{:?}'",
                    r
                );
            }
            Err(e) => format!("{}", e),
        }
    }

    /// Runs `plan` and returns the output as petty-formatted array of strings
    async fn run_plan(plan: LogicalPlan) -> Vec<String> {
        // run the created plan, ensuring the output is as expected
        let batches = Executor::new()
            .run_logical_plan(plan)
            .await
            .expect("ok running plan");

        pretty_format_batches(&batches)
            .expect("formatting results")
            .trim()
            .split('\n')
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    }

    ///  Insert the line protocol lines in `lp_lines` into this table
    fn write_lines_to_table(table: &mut Table, dictionary: &mut Dictionary, lp_lines: Vec<&str>) {
        let lp_data = lp_lines.join("\n");

        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();

        let data = split_lines_into_write_entry_partitions(partition_key_func, &lines);

        let batch = flatbuffers::get_root::<wb::WriteBufferBatch<'_>>(&data);
        let entries = batch.entries().expect("at least one entry");

        for entry in entries {
            let table_batches = entry.table_batches().expect("there were table batches");
            for batch in table_batches {
                let rows = batch.rows().expect("Had rows in the batch");
                table
                    .append_rows(dictionary, &rows)
                    .expect("Appended the row");
            }
        }
    }

    fn partition_key_func(_: &ParsedLine<'_>) -> String {
        String::from("the_partition_key")
    }
}
