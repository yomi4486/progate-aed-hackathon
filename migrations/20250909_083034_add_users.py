
# mypy: ignore-errors
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute, BooleanAttribute  # type: ignore # noqa: F401

revision = "20250909_083034"
down_revision = ""


def upgrade():
    """Add attribute 'email' by creating a new table and copying data automatically."""

    # 旧テーブル
    class OldUsers(Model):
        class Meta:  # type: ignore
            table_name = "users"
            region = "ap-northeast-1"

    # Existing attributes
        id = UnicodeAttribute(hash_key=True)

    # 新テーブル
    class NewUsers(Model):
        class Meta:  # type: ignore
            table_name = "users"
            region = "ap-northeast-1"

    # Existing attributes plus new attribute
        id = UnicodeAttribute(hash_key=True)
        email = UnicodeAttribute()
    # 新テーブル作成
    if not NewUsers.exists():
        NewUsers.create_table(billing_mode="PAY_PER_REQUEST", wait=True)

    # 旧テーブルから新テーブルへデータコピー
    for user in OldUsers.scan():
        NewUsers(id=user.id, email=getattr(user, "email", None)).save()

    # 旧テーブル削除
    if OldUsers.exists():
        OldUsers.delete_table()

    print("Data copy to new table 'users' completed. Old table has been automatically deleted.")


def downgrade():
    """Remove attribute 'email' by copying data to a table without the attribute."""

    # 旧テーブル
    class OldUsers(Model):
        class Meta:  # type: ignore
            table_name = "users"
            region = "ap-northeast-1"

        # Existing attributes (including the attribute to be removed)
        id = UnicodeAttribute(hash_key=True)
        email = UnicodeAttribute()

    # 新テーブル（属性なし）
    class NewUsers(Model):
        class Meta:  # type: ignore
            table_name = "users"
            region = "ap-northeast-1"

        # Existing attributes (without the attribute to be removed)
        id = UnicodeAttribute(hash_key=True)

    # 新テーブル作成
    if not NewUsers.exists():
        NewUsers.create_table(billing_mode="PAY_PER_REQUEST", wait=True)

    # データコピー
    for user in OldUsers.scan():
        NewUsers(id=user.id).save()

    # 旧テーブル削除
    if OldUsers.exists():
        OldUsers.delete_table()

    print("Data copied to the original table 'users'. The temporary table has been deleted automatically.")
