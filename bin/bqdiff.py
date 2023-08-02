#!/usr/bin/env python3

description = """
bqdiff

Compare two BigQuery tables L[eft] and R[ight] and create a "difference report" table.
L and R are differenced using a user-provided keyset to join them, and the final
report table will contain the following row entries:
    - 1 row per row in either L or R that is missing from R or L, respectively
    - 2 rows per row in both L or R that are different
"""

import os
import logging
import sys
import argparse
import time

logging.basicConfig()

import pandas as pd
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 2000)


from google.cloud.bigquery import Client
from google.api_core import exceptions


ARGS_CONFIG = {
    "--join": {
        "required": True,
        "nargs": "+",
        "help": "The join keys to use to compare the tables",
    },
    "--left": {
        "type": str,
        "help": "The left table to compare",
    },
    "--output": {
        "required": True,
        "help": "The output table to write to."
    },
    "--right": {
        "type": str,
        "help": "The right table to compare",
    },
    "--nocheck-types": {
        "required": False,
        "default": False,
        "action": "store_true",
        "help": "Do not compare the type of the columns before comparing the values",
    },
    "--exclude": {
        "required": False,
        "nargs": "+",
        "help": "A list of columns to exclude from the comparison",
    },
    "--include": {
        "required": False,
        "nargs": "+",
        "help": "A list of columns to exclude from the comparison",
    },
    "--where": {
        "required": False,
        "type": str,
        "help": "An optional WHERE clause (without the WHERE) to filter both inputs",
    },
    "--verbosity": {
        "required": False,
        "default": "info",
        "choices": ["debug", "info", "warning", "error", "critical"],
        "help": "The logging level to use",
    }
}

def create_parser(config=ARGS_CONFIG, **kwargs):
    parser = argparse.ArgumentParser(**kwargs)
    for arg, conf in config.items():
        parser.add_argument(arg, **conf)
    return parser

def _merge_sets(*sets):
    merged = set()
    for s in sets:
        if s is not None:
            merged.update(s)
    return merged 

def _flatten_csv_args(*args):
    result = []
    for arg in args:
        result += arg.split(",")
    return result


class DiffQuery:

    default_preprocessor = lambda x: x

    def __init__(self, left, right, join_keys, output=None, where=None, includes=None, excludes=None, check_types=True, logger=logging):
        self.logger = logger
        self.logger.info(f"Comparing schemas for '{left.full_table_id}' and '{right.full_table_id}'...")
        self.assert_schemas_match(
            left.schema, right.schema, includes=[includes, join_keys], excludes=[excludes], check_types=check_types
        )
        self.logger.info(f"🍏 Schemas match.")
        self.left = left
        self.right = right
        self.output = output
        self.join_keys = set(join_keys)
        self.where = f'where {where}' if where else ''
        self.fields = self._get_fields(left, includes=includes, excludes=excludes) - self.join_keys
        self.field_types = self._get_field_types(left, includes=self.fields)

    @staticmethod
    def _surrogate_key(*keys, delimiter='::', default=''):
        coalesce = f", '{delimiter}',\n".join([
            f"            coalesce(cast({key} as string), '{default}')"
            for key in keys
        ])
        return f"farm_fingerprint(concat(\n{coalesce} ))"

    @classmethod
    def _null_aware_equality(cls, left, right, schema_field):
        fn = cls._get_column_preprocessor(schema_field)
        return f"( ({left} is null and {right} is null) or ({fn(left)} = {fn(right)}) )"


    @classmethod
    def _get_fields(cls, table, includes=None, excludes=None):
        excludes = set(excludes or [])
        keys = set([field.name for field in table.schema])
        if includes:
            includes = set(includes) - excludes
            if len(includes - keys):
                raise ValueError(
                    f"Table {table.full_table_id} does not contain columns: {includes - keys}. Available columns:\n"
                    + "\n".join(sorted(keys))
                )
        else:
            includes = keys - excludes
        return includes

    @classmethod
    def _get_field_types(cls, table, includes=None, excludes=None):
        fields = cls._get_fields(table, includes=includes, excludes=excludes)
        return {field.name: field for field in table.schema if field.name in fields}

    @classmethod
    def _get_column_preprocessor(cls, field):
        if field.mode == "REPEATED":
            return lambda s: f"to_json_string({s})"
        if field.field_type == "STRUCT":
            return lambda s: f"to_json_string({s})"
        return cls.default_preprocessor

    def _build_diff_query(self):
        field_comparisons = [
            "if(" + self._null_aware_equality(f"a.{field}", f"b.{field}", field_def) + f", [], ['{field}'])"
            for field, field_def in self.field_types.items()
        ]
        if len(field_comparisons) == 0:
            field_comparisons = ['[]']
        return f"""
with 
a as (
    select
        {self._surrogate_key(*self.join_keys)} as _diff_key,
        {','.join(self.join_keys)},
        {','.join(self.fields)}
    from 
        `{self.left.full_table_id.replace(':', '.')}`
    {self.where}
)
, b as (
    select
        {self._surrogate_key(*self.join_keys)} as _diff_key,
        {','.join(self.join_keys)},
        {','.join(self.fields)}
    from 
        `{self.right.full_table_id.replace(':', '.')}`
    {self.where}
)
, diffs as (
    select
        coalesce(a._diff_key, b._diff_key) as _diff_key,
        (a._diff_key is not null) as _in_left,
        (b._diff_key is not null) as _in_right,
        array_concat(
            {', '.join(field_comparisons)}
        ) as differing_fields
    from a
    full outer join b
        on a._diff_key = b._diff_key
)
, resolved_diffs as (
    select
        diffs.* replace (
            -- Only report a difference if the row is in both tables
            if(_in_left and _in_right, differing_fields, []) as differing_fields
        ),
        'left' as _diff_side,
        a.* except(_diff_key)
    from diffs
    inner join a
        on diffs._diff_key = a._diff_key

    union all

    select
        diffs.* replace (
            -- Only report a difference if the row is in both tables
            if(_in_left and _in_right, differing_fields, []) as differing_fields
        ),
        'right' as _diff_side,
        b.* except(_diff_key)
    from diffs
    inner join b
        on diffs._diff_key = b._diff_key
)
select *
from resolved_diffs
        """

    def _build_output_query(self):
        assert self.output is not None
        return f"""
create or replace table {self.output} as (
    {self._build_diff_query()}
)
        """

    def _build_summary_query(self):
        assert self.output is not None
        return f"""
with
output as (
    select * from {self.output}
)
, count_summary as (
    select
        0 as index,
        '{self.left}' as left_table,
        '{self.right}' as right_table,
        'intersect_count' as key,
        sum(cast((_in_left and _in_right) as integer)) / 2.0 as num_records
    from output

    union all

    select
        1 as index,
        '{self.left}' as left_table,
        '{self.right}' as right_table,
        'left_only_count' as key,
        sum(cast((_in_left and not _in_right) as integer)) * 1.0 as num_records
    from output

    union all

    select
        2 as index,
        '{self.left}' as left_table,
        '{self.right}' as right_table,
        'right_only_count' as key,
        sum(cast((_in_right and not _in_left) as integer)) * 1.0 as num_records
    from output

    union all

    select
        row_number() over (partition by 1 order by num_records desc) + 2 as index,
        '{self.left}' as left_table,
        '{self.right}' as right_table,
        key,
        1.0 * num_records as num_records
    from (
        select
            cast(field as string) as key,
            count(*) / 2 as num_records
        from output
        join unnest(differing_fields) as field
        group by field
    )
)
select * from count_summary
order by index
        """


    def run(self, client, **kwargs):
        query = self._build_output_query() if self.output else self._build_diff_query()
        job = client.query(query, **kwargs)
        return job

    def summarize(self, client, **kwargs):
        query = self._build_summary_query()
        job = client.query(query, **kwargs)
        self.logger.debug(f"Running summary query:\n{query}")
        rows = []
        for row in job:
            rows.append(dict(row.items()))
        result = pd.DataFrame(rows)

        total_records = result.head(3)["num_records"].sum()
        result["perc_records"] = (100 * result["num_records"].astype(float) / total_records)
        result["total_records"] = total_records
        return result

    def assert_schemas_match(self, left, right, includes=None, excludes=None, check_types=True):
                    
        left_keys = set([field.name for field in left])
        right_keys = set([field.name for field in right])

        includes = _merge_sets(*includes)
        excludes = _merge_sets(*excludes)

        if len(includes) == 0:
            includes.update(left_keys)
            includes.update(right_keys)

        includes = includes - excludes
        left = [field for field in left if field.name in includes]
        right = [field for field in right if field.name in includes]

        checks = [ 
            (
                (includes - left_keys), "Left table missing requested fields: {missing}"
            ),
            (
                (includes - right_keys), "Right table missing requested fields: {missing}"
            ),
            (
                set([field.name for field in left]) - set([field.name for field in right]), 
                "Schemas do not match; right table is missing fields: {missing}"
            ),
            (
                set([field.name for field in right]) - set([field.name for field in left]),
                "Schemas do not match; left table is missing fields: {missing}"
            ),
        ]
                    
        for missing, msg in checks:
            if len(missing) > 0:
                raise ValueError(msg.format(missing=missing))

        if not check_types:
            return

        # If no error has be raised, the schema names match but the types differ 
        unmatched = set([
            f1.name 
            for (f1, f2) in zip(left, right)
            if f1 != f2
        ])
        if len(unmatched) > 0:
            raise ValueError(
                "Schemas types do not match for fields: {unmatched}".format(
                    missing='\n'.join([repr(m) for m in unmatched]),
                )
            )
        

def main(client, args, logger=logging):
    try:
        logger.info(f"Comparing: (left) '{args.left}' to (right) '{args.right}'")
        tables = [
            client.get_table(args.left),
            client.get_table(args.right),
        ]

        if args.include:
            includes = set(_flatten_csv_args(*args.include))
            logger.info(f"Checking columns subset: {includes}")
        else:
            includes = None

        if args.exclude:
            excludes = set(_flatten_csv_args(*args.exclude))
            logger.info(f"Excluding columns: {excludes}")
        else:
            excludes = None

        differ = DiffQuery(
            *tables, 
            join_keys=_flatten_csv_args(*args.join),
            output=args.output,
            where=args.where,
            includes=includes,
            excludes=args.exclude,
            check_types=not args.nocheck_types,
            logger=logger,
        )
        logger.debug("Created diff query:")
        logger.debug(differ._build_output_query())

        job = differ.run(client, job_id_prefix="diff_report_" + os.getenv("USER", "") + "_")
        logger.info(f"Running differ job: {job.job_id}")
        job.result()
        time.sleep(1)
        result = differ.summarize(client, job_id_prefix="diff_report_summary_" + os.getenv("USER", "") + "_")

        exit_code = 0
        if result.iloc[1:3]["num_records"].sum():
            logger.error(f"💥💥💥 FAILURE: {args.left} and {args.right} have disjoint records.💥💥💥")
            logger.info(f"""To inspect the missing records, please see:
            select *
            from {args.output}
            where not _in_left or not _in_right

            """)    
            exit_code = 1
            logger.error(f"Summary:\n{result}")

        if len(result) > 3:
            logger.error(f"💥💥💥 FAILURE: {args.left} and {args.right} have differing records.💥💥💥")
            logger.info(f"""To inspect the differing records, please see:
            select *
            from {args.output}
            where array_length(differing_fields) > 0
            order by array_length(differing_fields) desc, _diff_key, _diff_side

            """)    
            logger.error(f"Summary:\n{result}")
            exit_code = 1

        if exit_code == 0:
            logger.info(f"🍏 SUCCESS. {args.left} and {args.right} are the same.")
            logger.info(f"Summary:\n{result}")

        return exit_code
    except Exception as e:
        logger.error(f"💥💥💥 FAILURE: {e}")

if __name__ == "__main__":
    parser = create_parser(prog=sys.argv[0], description=description)
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    args = parser.parse_args(sys.argv[1:])
    try:
        import coloredlogs
        coloredlogs.install(level=args.verbosity.upper())
    except:
        pass
    logger = logging.getLogger(os.path.basename(__file__))
    logger.setLevel(getattr(logging, args.verbosity.upper()))
    client = Client()
    sys.exit(main(client, args, logger=logger))
