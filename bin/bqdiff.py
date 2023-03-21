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

try:
    import coloredlogs
except:
    pass

from google.cloud.bigquery import Client
from google.api_core import exceptions

logger = logging.getLogger(os.path.basename(__file__))

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


class DiffQuery:

    default_preprocessor = lambda x: x

    column_preprocessors = {
        None: default_preprocessor,
        "RECORD": lambda s: "to_json_string({s})",
    }

    def __init__(self, left, right, join_keys, output=None, where=None, includes=None, excludes=None, check_types=True):
        self.assert_schemas_match(
            left.schema, right.schema, includes=includes, excludes=excludes, check_types=check_types
        )
        fields = self._get_fields(left, includes=includes, excludes=excludes)

        if set(join_keys) - fields:
            raise ValueError(
                f"Tables [{left.full_table_id}, {right.full_table_id}] missing join key(s): {join_keys}"
            )
        self.left = left
        self.right = right
        self.output = output
        self.join_keys = set(join_keys)
        self.where = f'where {where}' if where else ''
        self.fields = fields - self.join_keys
        self.select_keys = ','.join(self.join_keys)

    @staticmethod
    def _surrogate_key(*keys, delimiter='::', default=''):
        coalesce = f", '{delimiter}',\n".join([
            f"coalesce(cast({key} as string), '{default}')"
            for key in keys
        ])
        return f"farm_fingerprint(concat(\n{coalesce}\n))"

    @classmethod
    def _null_aware_equality(cls, left, right, column_type=None):
        fn = cls.column_preprocessors.get(column_type, cls.default_preprocessor)
        return f"( ({left} is null and {right} is null) or ({fn(left)} = {fn(right)}) )"


    @staticmethod
    def _get_fields(table, includes=None, excludes=None):
        excludes = set(excludes or [])
        keys = set([field.name for field in table.schema])
        if includes:
            includes = set(includes) - excludes
            if len(includes - keys):
                raise ValueError(f"Table {table.full_table_id} does not contain columns: {includes - keys}")
        else:
            includes = keys - excludes
        return includes

    def _build_diff_query(self):
        field_comparisons = [
            "if(" + _null_aware_equality(f"a.{field}", f"b.f{field}") + f", [], ['{field}'])"
            for field in self.fields
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
                    if(a._diff_key is not null and b._diff_key is not null, 0.5, 0) _both_count,
                    array_concat(
                        {', '.join(field_comparisons)}
                    ) as differing_fields
                from a
                full outer join b
                    on a._diff_key = b._diff_key
            )
            , resolved_diffs as (
                select
                    diffs.*,
                    'left' as _diff_side,
                    1 as _left_count,
                    0 as _right_count,
                    if(array_length(diffs.differing_fields) > 0, 0.5, 0) as _diff_count,
                    a.* except(_diff_key)
                from diffs
                inner join a
                    on diffs._diff_key = a._diff_key

                union all

                select
                    diffs.*,
                    'right' as _diff_side,
                    0 as _left_count,
                    1 as _right_count,
                    if(array_length(diffs.differing_fields) > 0, 0.5, 0) as _diff_count,
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

    def run(self, client, **kwargs):
        query = self._build_output_query() if self.output else self._build_diff_query()
        job = client.query(query, **kwargs)
        return job

    @staticmethod
    def assert_schemas_match(left, right, includes=None, excludes=None, check_types=True):
        left_keys = set([field.name for field in left])
        right_keys = set([field.name for field in right])
        excludes = set(excludes or [])
        if includes is None:
            includes = set()
            includes.update(left_keys)
            includes.update(right_keys)
        includes = set(includes) - excludes
        left = [field for field in left if field.name in includes]
        right = [field for field in right if field.name in includes]

        if left == right:
            logger.debug(f"Schemas match for fields: {includes}.")
            return

        # Check for missing fields between the 2 schemas
        missing = set([field.name for field in left]) - set([field.name for field in right])
        if len(missing):                        
            raise ValueError(f"Schemas do not match; right table is missing fields: {missing}")

        missing = set([field.name for field in right]) - set([field.name for field in left])
        if len(missing):                        
            raise ValueError(f"Schemas do not match; left table is missing fields: {missing}")

        # If no error has be raised, the schema names match but the types differ 
        unmatched = set([
            f1.name 
            for (f1, f2) in zip(left, right)
            if f1 != f2
        ])
        if not check_types:
            logger.warning(f"Schema types do not match for fields: {unmatched}")
            return

        raise ValueError(
            "Schemas types do not match for fields: {unmatched}".format(
                missing='\n'.join([repr(m) for m in missing]),
            )
        )
        

def main(client, args):
    # try:
    logger.info(f"Comparing: (left) '{args.left}' to (right) '{args.right}'")
    if args.include:
        logger.info(f"Checking only columns: {args.include}")
    elif args.exclude:
        logger.info(f"Excluding columns: {args.exclude}")

    tables = [
        client.get_table(args.left),
        client.get_table(args.right),
    ]
    includes = set(args.join)
    if args.include:
        includes.update(set(args.include))

    differ = DiffQuery(
        *tables, 
        join_keys=args.join,
        output=args.output,
        where=args.where,
        includes=includes,
        excludes=args.exclude,
        check_types=not args.nocheck_types,
    )
    logger.debug("Created diff query:")

    job = differ.run(client, job_id_prefix="diff_report_" + os.getenv("USER", "") + "_")
    logger.info(f"Running differ job: {job.job_id}")
    logger.debug(differ._build_output_query())
    return 0

if __name__ == "__main__":
    parser = create_parser(prog=sys.argv[0], description=description)
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    args = parser.parse_args(sys.argv[1:])
    logging.basicConfig(level=args.verbosity.upper())
    client = Client()
    sys.exit(main(client, args))
