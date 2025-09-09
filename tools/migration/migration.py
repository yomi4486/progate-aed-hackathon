# type: ignore
"""
Simple linear migration tool for PynamoDB models.

Usage:
  python migration.py create <table_name>
  python migration.py add <table_name> <attr_name>:<AttrType>
  python migration.py upgrade [<target_revision>]
  python migration.py downgrade [<target_revision>]
"""

import datetime
import importlib.util
import os
import sys
import textwrap
import traceback
from typing import List, Optional, Dict, Any

import boto3
from mypy_boto3_dynamodb import DynamoDBClient, DynamoDBServiceResource
from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute
from pynamodb.models import Model

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
    if spec is None:
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


# --- DynamoDB copy helper (for ALTER-like ops) ---
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
    dynamodb: DynamoDBServiceResource = boto3.resource("dynamodb", region_name=region_name)
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


# --- File templates ---
CREATE_TEMPLATE = """\
from pynamodb.models import Model
from pynamodb.attributes import {attr_imports}

revision = "{revision}"
down_revision = {down_revision_repr}


class {class_name}(Model):
    class Meta:
        table_name = "{table_name}"
        region = "{region}"

    {hash_key_name} = {hash_key_type}(hash_key=True)  # 主キー属性

    # 他の属性は add コマンドで追加してください


def upgrade():
    \"\"\"Apply: create table if not exists\"\"\"
    if not {class_name}.exists():
        {class_name}.create_table(read_capacity_units=5, write_capacity_units=5, wait=True)


def downgrade():
    \"\"\"Rollback: drop table (careful!)\"\"\"
    if {class_name}.exists():
        {class_name}.delete_table()
"""

ADD_ATTR_TEMPLATE = """
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute, BooleanAttribute

revision = "{revision}"
down_revision = "{down_revision}"


def upgrade():
    \"\"\"Add attribute '{attr_name}' by creating a new table and copying data automatically.\"\"\"
    # 旧テーブル
    class Old{class_name}(Model):
        class Meta:
            table_name = "{table_name}"
            region = "{region}"
        # Existing attributes
{old_attributes}

    # 新テーブル
    class New{class_name}(Model):
        class Meta:
            table_name = "{table_name}"
            region = "{region}"
        # Existing attributes plus new attribute
{new_attributes}
    # 新テーブル作成
    if not New{class_name}.exists():
        New{class_name}.create_table(billing_mode='PAY_PER_REQUEST', wait=True)

    # 旧テーブルから新テーブルへデータコピー
    for user in Old{class_name}.scan():
        New{class_name}(id=user.id, email=user.email, {attr_name}=getattr(user, '{attr_name}', None)).save()

    # 旧テーブル削除
    if Old{class_name}.exists():
        Old{class_name}.delete_table()

    print("新テーブル {table_name}_new へデータコピー完了。旧テーブルは自動削除されました。")


def downgrade():
    \"\"\"Remove attribute '{attr_name}' by copying data to a table without the attribute.\"\"\"
    # 旧テーブル
    class Old{class_name}(Model):
        class Meta:
            table_name = "{table_name}"
            region = "{region}"
        id = UnicodeAttribute(hash_key=True)
        email = UnicodeAttribute()
        {attr_name} = {attr_type}(null=True)

    # 新テーブル（属性なし）
    class New{class_name}(Model):
        class Meta:
            table_name = "{table_name}"
            region = "{region}"
        id = UnicodeAttribute(hash_key=True)
        email = UnicodeAttribute()

    # 新テーブル作成
    if not New{class_name}.exists():
        New{class_name}.create_table(billing_mode='PAY_PER_REQUEST', wait=True)

    # データコピー
    for user in Old{class_name}.scan():
        New{class_name}(id=user.id, email=user.email).save()

    # 旧テーブル削除
    if Old{class_name}.exists():
        Old{class_name}.delete_table()

    print("元のテーブル {table_name} へデータコピー完了。newテーブルは自動削除されました。")
"""


# --- Generator functions ---
def create_migration_file(
    action: str,
    table_name: str,
    extra: Optional[str] = None,
    down_rev: Optional[str] = None,
    hash_key_name: str = "id",
    hash_key_type: str = "UnicodeAttribute",
    attr_imports: str = "UnicodeAttribute, NumberAttribute"
):
    revision = now_revision_str()
    fname = f"{revision}_{action}_{table_name}.py"
    path = os.path.join(MIGRATIONS_DIR, fname)

    down_revision_repr = f'"{down_rev}"' if down_rev else "None"
    class_name = table_name.capitalize()
    region = DEFAULT_REGION

    if action == "create":
        content = CREATE_TEMPLATE.format(
            revision=revision,
            down_revision_repr=down_revision_repr,
            class_name=class_name,
            table_name=table_name,
            region=region,
            hash_key_name=hash_key_name,
            hash_key_type=hash_key_type,
            attr_imports=attr_imports,
        )
    elif action in ("add", "remove", "update"):
        # extra expected: "age:Number"
        if not extra:
            raise ValueError("extra attribute specification required for add/remove")
        attr_name, attr_type = extra.split(":", 1)
        content = ADD_ATTR_TEMPLATE.format(
            revision=revision,
            down_revision=down_rev or "",
            class_name=class_name,
            table_name=table_name,
            region=region,
            attr_name=attr_name,
            attr_type=attr_type,
        )
    else:
        # generic template
        content = textwrap.dedent(
            f"""\
        revision = "{revision}"
        down_revision = {down_revision_repr}


        def upgrade():
            pass


        def downgrade():
            pass
        """
        )
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"Generated migration: {path}")
    return path


# --- Runner logic ---
def upgrade(target_revision: Optional[str] = None):
    files = list_migration_files()
    applied = set(get_applied_revisions())
    # construct ordered list of (revision, filepath)
    rev_file = []
    for fname in files:
        rev = (
            os.path.splitext(fname)[0].split("_", 2)[0] + "_" + os.path.splitext(fname)[0].split("_", 2)[1]
        )
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
        rev = (
            os.path.splitext(fname)[0].split("_", 2)[0] + "_" + os.path.splitext(fname)[0].split("_", 2)[1]
        )
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
        except Exception as e:
            print(f"Error reverting {rev}: {e}")
            traceback.print_exc()
            sys.exit(1)


# --- CLI ---
def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return

    cmd = argv[1]
    if cmd == "create":
        if len(argv) < 3:
            print("Usage: create <table_name> [hash_key_name] [hash_key_type]")
            return
        table_name = argv[2]
        hash_key_name = argv[3] if len(argv) >= 4 else "id"
        hash_key_type = argv[4] if len(argv) >= 5 else "UnicodeAttribute"
        attr_imports = "UnicodeAttribute, NumberAttribute, BooleanAttribute, UTCDateTimeAttribute"
        # determine last revision
        files = list_migration_files()
        last_rev = None
        if files:
            last_fname = files[-1]
            last_rev = (
                os.path.splitext(last_fname)[0].split("_", 2)[0] + "_" + os.path.splitext(last_fname)[0].split("_", 2)[1]
            )
        path = create_migration_file(
            "create", table_name, down_rev=last_rev,
            hash_key_name=hash_key_name, hash_key_type=hash_key_type, attr_imports=attr_imports
        )
        print(path)
    elif cmd == "add":
        if len(argv) < 4:
            print("Usage: add <table_name> <attr_name>:<AttrType>")
            return
        table_name = argv[2]
        extra = argv[3]
        files = list_migration_files()
        last_rev = None
        if files:
            last_fname = files[-1]
            last_rev = (
                os.path.splitext(last_fname)[0].split("_", 2)[0] + "_" + os.path.splitext(last_fname)[0].split("_", 2)[1]
            )
        path = create_migration_file("add", table_name, extra=extra, down_rev=last_rev)
        print(path)
    elif cmd == "upgrade":
        target = argv[2] if len(argv) >= 3 else None
        upgrade(target)
    elif cmd == "downgrade":
        target = argv[2] if len(argv) >= 3 else None
        downgrade(target)
    else:
        print("Unknown command:", cmd)
        print(__doc__)


if __name__ == "__main__":
    main(sys.argv)
