# type: ignore
"""
Simple linear migration tool for PynamoDB models.

Usage (argparse-based CLI):
    uv run migrate create <table_name> [hash_key_name] [hash_key_type]
    uv run migrate add <table_name> <attr_name>:<AttrType>
    uv run migrate upgrade [target_revision]
    uv run migrate downgrade [target_revision]

Notes:
- Optional positionals are kept for backward compatibility.
- You can also use: python -m tools.migrate SUBCOMMAND ...
"""

import argparse
import datetime
import importlib.util
import os
import sys
import traceback
from typing import Any, Dict, List, Optional

import boto3
from mypy_boto3_dynamodb import DynamoDBClient, DynamoDBServiceResource
from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute
from pynamodb.models import Model

from ._generator import create_migration_file

# --- Config ---
MIGRATIONS_DIR = os.path.join(os.getcwd(), "migrations")
if not os.path.isdir(MIGRATIONS_DIR):
    os.makedirs(MIGRATIONS_DIR)

# DynamoDB region default (models may override)
DEFAULT_REGION = "ap-northeast-1"


# --- MigrationHistory model ---
class MigrationHistory(Model):
    class Meta:
        table_name = "MigrationHistory"
        region = DEFAULT_REGION

    revision = UnicodeAttribute(hash_key=True)
    applied_at = UTCDateTimeAttribute(null=False)


# --- Migration history helpers ---
def ensure_migration_history_table():
    if not MigrationHistory.exists():
        MigrationHistory.create_table(
            read_capacity_units=1, write_capacity_units=1, wait=True
        )


# --- Helpers ---
def now_revision_str() -> str:
    # Revision in spec: YYYYMMDD_HHMMSS
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


def list_migration_files() -> List[str]:
    files = [f for f in os.listdir(MIGRATIONS_DIR) if f.endswith(".py")]
    # sort by revision (filename starts with timestamp)
    files.sort()
    return files


def parse_revision_from_filename(fname: str) -> str:
    # assumes filename starts with revision
    parts = os.path.splitext(fname)[0].split("_", 2)
    return parts[0] + "_" + parts[1]


def import_migration_module(filepath: str):
    spec = importlib.util.spec_from_file_location("migration_module", filepath)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {filepath}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    return mod


def get_applied_revisions() -> List[str]:
    ensure_migration_history_table()
    # Scan it (small table)
    return [item.revision for item in MigrationHistory.scan()]


def record_revision_applied(revision: str):
    ensure_migration_history_table()
    MigrationHistory(revision=revision, applied_at=datetime.datetime.utcnow()).save()


def remove_revision_record(revision: str):
    ensure_migration_history_table()
    try:
        item = MigrationHistory.get(revision)
        item.delete()
    except Exception:
        # not exists
        pass


# --- DynamoDB copy helper (optional) ---
def copy_table(
    src_table_name: str,
    dst_table_name: str,
    region_name: str = DEFAULT_REGION,
    read_capacity: int = 5,
    write_capacity: int = 5,
    wait: bool = True,
):
    """
    Create destination table with same key schema & attributes based on describe_table,
    then copy all items via scan+batch_writer.
    NOTE: This is a best-effort helper; secondary indexes & complex settings may require manual handling.
    """
    dynamodb: DynamoDBServiceResource = boto3.resource(
        "dynamodb", region_name=region_name
    )
    client: DynamoDBClient = boto3.client("dynamodb", region_name=region_name)

    # describe source
    src_desc: Dict[str, Any] = client.describe_table(TableName=src_table_name)["Table"]

    # Build create_table kwargs for destination
    attribute_definitions: Dict[str, Any] = src_desc.get("AttributeDefinitions", [])
    key_schema: Dict[str, Any] = src_desc.get("KeySchema", [])

    print(
        f"Creating table {dst_table_name} with same key schema as {src_table_name} (no GSIs/LSIs)."
    )
    create_kwargs = {
        "TableName": dst_table_name,
        "KeySchema": key_schema,
        "AttributeDefinitions": attribute_definitions,
        "ProvisionedThroughput": {
            "ReadCapacityUnits": read_capacity,
            "WriteCapacityUnits": write_capacity,
        },
    }
    client.create_table(**create_kwargs)
    if wait:
        waiter = client.get_waiter("table_exists")
        waiter.wait(TableName=dst_table_name)

    # copy items
    src_table = dynamodb.Table(src_table_name)
    dst_table = dynamodb.Table(dst_table_name)

    # scan and write in batches
    print("Starting items copy (scan -> batch_writer).")
    response = src_table.scan()
    items = response.get("Items", [])
    with dst_table.batch_writer() as bw:
        for it in items:
            bw.put_item(Item=it)

    # handle pagination
    while "LastEvaluatedKey" in response:
        response = src_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items = response.get("Items", [])
        with dst_table.batch_writer() as bw:
            for it in items:
                bw.put_item(Item=it)

    print("Table copy finished.")
    return True


# --- Runner logic ---
def upgrade(target_revision: Optional[str] = None):
    files = list_migration_files()
    applied = set(get_applied_revisions())
    # construct ordered list of (revision, filepath)
    rev_file = []
    for fname in files:
        rev = parse_revision_from_filename(fname)
        rev_file.append((rev, os.path.join(MIGRATIONS_DIR, fname)))
    rev_file.sort()

    to_apply = []
    for rev, path in rev_file:
        if rev in applied:
            continue
        to_apply.append((rev, path))
        if target_revision and rev == target_revision:
            break

    if not to_apply:
        print("No migrations to apply.")
        return

    for rev, path in to_apply:
        print(f"Applying {rev} -> {path}")
        try:
            mod = import_migration_module(path)
            if not hasattr(mod, "upgrade"):
                raise RuntimeError("Migration file has no upgrade()")
            mod.upgrade()
            record_revision_applied(rev)
            print(f"Applied {rev}")
        except Exception as e:
            print(f"Error applying {rev}: {e}")
            traceback.print_exc()
            sys.exit(1)


def downgrade(target_revision: Optional[str] = None):
    files = list_migration_files()
    # build ordered list
    rev_file = []
    for fname in files:
        rev = parse_revision_from_filename(fname)
        rev_file.append((rev, os.path.join(MIGRATIONS_DIR, fname)))
    rev_file.sort()
    applied = get_applied_revisions()
    # We will reverse-apply downgrades for applied revisions newest-first
    applied_set = set(applied)
    applied_ordered = [r for r, p in rev_file if r in applied_set]
    if not applied_ordered:
        print("No applied migrations.")
        return

    # downto target (exclusive): if target is None, revert latest one only
    to_revert = []
    if target_revision:
        # revert until we reach target_revision (do NOT revert target_revision itself)
        while applied_ordered:
            last = applied_ordered.pop()  # newest
            if last == target_revision:
                break
            # find path
            match_path = next((p for r, p in rev_file if r == last), None)
            if match_path:
                to_revert.append((last, match_path))
    else:
        # revert only latest
        last = applied_ordered.pop()
        match_path = next((p for r, p in rev_file if r == last), None)
        if match_path:
            to_revert.append((last, match_path))

    if not to_revert:
        print("Nothing to downgrade (target may equal current).")
        return

    for rev, path in to_revert:
        print(f"Downgrading {rev} -> {path}")
        try:
            mod = import_migration_module(path)
            if not hasattr(mod, "downgrade"):
                raise RuntimeError("Migration file has no downgrade()")
            mod.downgrade()
            remove_revision_record(rev)
            print(f"Reverted {rev}")
        except (ImportError, AttributeError, RuntimeError) as e:
            print(f"Error reverting {rev}: {e}")
            traceback.print_exc()
            sys.exit(1)
        except Exception as e:
            # Unexpected exception, print and exit
            print(f"Unexpected error reverting {rev}: {e}")
            traceback.print_exc()
            sys.exit(1)


# --- CLI ---
def _cmd_create(args: argparse.Namespace) -> None:
    table_name: str = args.table_name
    hash_key_name: str = args.hash_key_name or "id"
    hash_key_type: str = args.hash_key_type or "UnicodeAttribute"
    attr_imports = (
        "UnicodeAttribute, NumberAttribute, BooleanAttribute, UTCDateTimeAttribute"
    )

    files = list_migration_files()
    last_rev = None
    if files:
        last_fname = files[-1]
        last_rev = (
            os.path.splitext(last_fname)[0].split("_", 2)[0]
            + "_"
            + os.path.splitext(last_fname)[0].split("_", 2)[1]
        )

    path = create_migration_file(
        "create",
        table_name,
        down_rev=last_rev,
        hash_key_name=hash_key_name,
        hash_key_type=hash_key_type,
        attr_imports=attr_imports,
    )
    print(path)


def _cmd_add(args: argparse.Namespace) -> None:
    table_name: str = args.table_name
    extra: str = args.attribute

    files = list_migration_files()
    last_rev = None
    if files:
        last_fname = files[-1]
        last_rev = (
            os.path.splitext(last_fname)[0].split("_", 2)[0]
            + "_"
            + os.path.splitext(last_fname)[0].split("_", 2)[1]
        )

    path = create_migration_file("add", table_name, extra=extra, down_rev=last_rev)
    print(path)


def _cmd_upgrade(args: argparse.Namespace) -> None:
    target: Optional[str] = args.target_revision
    upgrade(target)


def _cmd_downgrade(args: argparse.Namespace) -> None:
    target: Optional[str] = args.target_revision
    downgrade(target)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="PynamoDB migration tool",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # create
    p_create = subparsers.add_parser(
        "create",
        help="Create a migration to create a new table",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p_create.add_argument("table_name", help="Table name to create")
    # Optional positionals for backward compatibility
    p_create.add_argument(
        "hash_key_name",
        nargs="?",
        default=None,
        help="Hash key attribute name (default: id)",
    )
    p_create.add_argument(
        "hash_key_type",
        nargs="?",
        default=None,
        help="Hash key attribute type (default: UnicodeAttribute)",
    )
    p_create.set_defaults(func=_cmd_create)

    # add attribute
    p_add = subparsers.add_parser(
        "add",
        help="Create a migration to add an attribute (by copy)",
    )
    p_add.add_argument("table_name", help="Target table name")
    p_add.add_argument(
        "attribute",
        help="Attribute spec as <name>:<AttrType> (e.g., email:UnicodeAttribute)",
    )
    p_add.set_defaults(func=_cmd_add)

    # upgrade
    p_up = subparsers.add_parser(
        "upgrade", help="Apply migrations up to target (inclusive)"
    )
    p_up.add_argument(
        "target_revision",
        nargs="?",
        default=None,
        help="Target revision (YYYYMMDD_HHMMSS). If omitted, apply all pending.",
    )
    p_up.set_defaults(func=_cmd_upgrade)

    # downgrade
    p_down = subparsers.add_parser(
        "downgrade",
        help="Revert migrations down to just above target. If omitted, revert latest.",
    )
    p_down.add_argument(
        "target_revision",
        nargs="?",
        default=None,
        help="Target revision (exclusive). If omitted, revert latest only.",
    )
    p_down.set_defaults(func=_cmd_downgrade)

    return parser


def main(argv: Optional[List[str]] = None):
    parser = build_parser()
    args = parser.parse_args(argv)
    # Dispatch
    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        return
    func(args)


if __name__ == "__main__":
    main()
