"""
Code generation utilities for the PynamoDB migration tool.

This module encapsulates:
- Migration file content rendering (templates)
- Attribute formatting helpers
- Syntax validation and file writing

Public API:
- create_migration_file(...): str -> path to generated file
"""

from __future__ import annotations

import datetime
import os
import sys
import textwrap
from typing import Dict, List, Optional

# --- Defaults ---
DEFAULT_REGION = "ap-northeast-1"


# --- Internal helpers ---
def _now_revision_str() -> str:
    """Return revision string like YYYYMMDD_HHMMSS."""
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


def _format_attribute_args(attributes: List[Dict[str, str]]) -> str:
    """Build kwargs for model constructor during copy.

    Example: id=user.id, email=user.email
    """
    return ", ".join([f"{attr['name']}=user.{attr['name']}" for attr in attributes])


def _format_attributes(
    attributes: List[Dict[str, str]],
    hash_key_name: str,
    indent: int = 4,
) -> str:
    pad = " " * indent
    lines: List[str] = []
    for attr in attributes:
        if attr["name"] == hash_key_name:
            lines.append(f"{pad}{attr['name']} = {attr['type']}(hash_key=True)")
        else:
            lines.append(f"{pad}{attr['name']} = {attr['type']}()")
    return "\n".join(lines)


# --- Templates ---
_CREATE_TEMPLATE = '''\
# mypy: ignore-errors
from pynamodb.models import Model
from pynamodb.attributes import {attr_imports}  # type: ignore # noqa: F401

revision = "{revision}"
down_revision = {down_revision_repr}


class {class_name}(Model):
    class Meta:  # type: ignore
        table_name = "{table_name}"
        region = "{region}"

    {hash_key_name} = {hash_key_type}(hash_key=True)  # 主キー属性

    # 他の属性は add コマンドで追加してください


def upgrade():
    """Apply: create table if not exists"""
    if not {class_name}.exists():
        {class_name}.create_table(read_capacity_units=5, write_capacity_units=5, wait=True)


def downgrade():
    """Rollback: drop table (careful!)"""
    if {class_name}.exists():
        {class_name}.delete_table()
'''


_ADD_ATTR_TEMPLATE = '''
# mypy: ignore-errors
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute, BooleanAttribute  # type: ignore # noqa: F401

revision = "{revision}"
down_revision = "{down_revision}"


def upgrade():
    """Add attribute '{attr_name}' by creating a new table and copying data automatically."""

    # 旧テーブル
    class Old{class_name}(Model):
        class Meta:  # type: ignore
            table_name = "{table_name}"
            region = "{region}"

        # Existing attributes
{old_attributes}

    # 新テーブル
    class New{class_name}(Model):
        class Meta:  # type: ignore
            table_name = "{table_name}"
            region = "{region}"

        # Existing attributes plus new attribute
{new_attributes}

    # 新テーブル作成
    if not New{class_name}.exists():
        New{class_name}.create_table(billing_mode="PAY_PER_REQUEST", wait=True)

    # 旧テーブルから新テーブルへデータコピー
    for user in Old{class_name}.scan():
        New{class_name}({attribute_args}, {attr_name}=getattr(user, "{attr_name}", None)).save()

    # 旧テーブル削除
    if Old{class_name}.exists():
        Old{class_name}.delete_table()

    print("Data copy to new table '{table_name}' completed. Old table has been automatically deleted.")


def downgrade():
    """Remove attribute '{attr_name}' by copying data to a table without the attribute."""

    # 旧テーブル
    class Old{class_name}(Model):
        class Meta:  # type: ignore
            table_name = "{table_name}"
            region = "{region}"

        # Existing attributes (including the attribute to be removed)
{new_attributes}

    # 新テーブル（属性なし）
    class New{class_name}(Model):
        class Meta:  # type: ignore
            table_name = "{table_name}"
            region = "{region}"

        # Existing attributes (without the attribute to be removed)
{old_attributes}

    # 新テーブル作成
    if not New{class_name}.exists():
        New{class_name}.create_table(billing_mode="PAY_PER_REQUEST", wait=True)

    # データコピー
    for user in Old{class_name}.scan():
        New{class_name}({attribute_args}).save()

    # 旧テーブル削除
    if Old{class_name}.exists():
        Old{class_name}.delete_table()

    print("Data copied to the original table '{table_name}'. The temporary table has been deleted automatically.")
'''


# --- Rendering ---
def _render_create(
    *,
    revision: str,
    class_name: str,
    table_name: str,
    region: str,
    hash_key_name: str,
    hash_key_type: str,
    down_rev: Optional[str],
    attr_imports: str,
) -> str:
    down_revision_repr = f'"{down_rev}"' if down_rev else "None"
    return _CREATE_TEMPLATE.format(
        revision=revision,
        down_revision_repr=down_revision_repr,
        class_name=class_name,
        table_name=table_name,
        region=region,
        hash_key_name=hash_key_name,
        hash_key_type=hash_key_type,
        attr_imports=attr_imports,
    )


def _render_add_attr(
    *,
    revision: str,
    class_name: str,
    table_name: str,
    region: str,
    hash_key_name: str,
    attributes: List[Dict[str, str]],
    extra: str,
    down_rev: Optional[str],
) -> str:
    if ":" not in extra:
        raise ValueError("extra attribute specification required as <name>:<AttrType>")
    attr_name, attr_type = extra.split(":", 1)

    old_attributes = _format_attributes(attributes, hash_key_name, indent=8)
    new_attributes = _format_attributes(attributes + [{"name": attr_name, "type": attr_type}], hash_key_name, indent=8)
    attribute_args = _format_attribute_args(attributes)

    return _ADD_ATTR_TEMPLATE.format(
        revision=revision,
        down_revision=down_rev or "",
        class_name=class_name,
        table_name=table_name,
        region=region,
        attr_name=attr_name,
        attr_type=attr_type,
        old_attributes=old_attributes,
        new_attributes=new_attributes,
        attribute_args=attribute_args,
    )


def _render_generic(revision: str, down_rev: Optional[str]) -> str:
    down_revision_repr = f'"{down_rev}"' if down_rev else "None"
    return textwrap.dedent(
        f"""\
        revision = "{revision}"
        down_revision = {down_revision_repr}


        def upgrade():
            pass


        def downgrade():
            pass
        """
    )


def _validate_python(filename_for_error: str, content: str) -> None:
    """Validate the generated Python code; raise SystemExit on error to mimic CLI behavior."""
    try:
        compile(content, filename_for_error, "exec")
    except SyntaxError as e:
        err_line = e.text.strip() if e.text else ""
        print(
            "SyntaxError in generated migration:\n"
            f"  file: {filename_for_error}\n"
            f"  line: {e.lineno}, column: {e.offset}\n"
            f"  msg: {e.msg}\n"
            f"  code: {err_line}",
            file=sys.stderr,
        )
        raise SystemExit(1)


def create_migration_file(
    action: str,
    table_name: str,
    extra: Optional[str] = None,
    down_rev: Optional[str] = None,
    hash_key_name: str = "id",
    hash_key_type: str = "UnicodeAttribute",
    attr_imports: str = "UnicodeAttribute, NumberAttribute, BooleanAttribute, UTCDateTimeAttribute",
    attributes: Optional[List[Dict[str, str]]] = None,
    migrations_dir: Optional[str] = None,
) -> str:
    """Generate a migration file and write it under migrations_dir.

    Returns the path to the generated file.
    """
    # Resolve output directory
    out_dir = migrations_dir or os.path.join(os.getcwd(), "migrations")
    os.makedirs(out_dir, exist_ok=True)

    revision = _now_revision_str()
    fname = f"{revision}_{action}_{table_name}.py"
    path = os.path.join(out_dir, fname)

    class_name = table_name.capitalize()
    region = DEFAULT_REGION

    if attributes is None:
        attributes = [{"name": hash_key_name, "type": hash_key_type}]

    if action == "create":
        content = _render_create(
            revision=revision,
            class_name=class_name,
            table_name=table_name,
            region=region,
            hash_key_name=hash_key_name,
            hash_key_type=hash_key_type,
            down_rev=down_rev,
            attr_imports=attr_imports,
        )
    elif action in ("add", "remove", "update"):
        content = _render_add_attr(
            revision=revision,
            class_name=class_name,
            table_name=table_name,
            region=region,
            hash_key_name=hash_key_name,
            attributes=attributes,
            extra=extra or "",
            down_rev=down_rev,
        )
    else:
        content = _render_generic(revision, down_rev)

    # Syntax validation
    _validate_python(fname, content)

    # Write file
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"Generated migration: {path}")
    return path
