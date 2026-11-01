"""Create the pgdispatch schema.

The tables are defined once, in pgdispatch's own SQL migrations; this Django
migration simply runs them, so ``migrate`` and ``pgdispatch migrate`` always
produce the same schema. Future versions add a Django migration per SQL one.
"""

from __future__ import annotations

from typing import Any

from django.db import migrations


def forwards(apps: Any, schema_editor: Any) -> None:
    from pgdispatch.django.client import migrate_schema

    migrate_schema(schema_editor.connection, target=1)


def backwards(apps: Any, schema_editor: Any) -> None:
    from pgdispatch.django.client import drop_schema

    drop_schema(schema_editor.connection)


class Migration(migrations.Migration):
    initial = True
    dependencies = []
    operations = [migrations.RunPython(forwards, backwards, elidable=False)]
