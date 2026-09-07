"""Recover the completed MySQL 0003 DDL after its version marker overflowed."""
import importlib.util
import os
from pathlib import Path
from sqlalchemy import Boolean, create_engine, inspect, text
from sqlalchemy.dialects.mysql import TINYINT

engine = create_engine(os.environ['DATABASE_URL'].replace('mysql+aiomysql:', 'mysql+pymysql:').replace('mysql:', 'mysql+pymysql:'))
with engine.begin() as conn:
    current = conn.execute(text('SELECT version_num FROM alembic_version')).scalar_one()
    assert current == '0002_audit_orchestration_history', current
    inspector = inspect(conn)
    class VerifyOperations:
        def f(self, name): return name
        def create_table(self, name, *columns):
            actual = {c['name']: c for c in inspector.get_columns(name)}
            assert set(actual) == {c.name for c in columns}, name
            for c in columns:
                a = actual[c.name]
                assert a['nullable'] == c.nullable, (name, c.name)
                mysql_bool = isinstance(c.type, Boolean) and isinstance(a['type'], TINYINT) and a['type'].display_width == 1
                assert mysql_bool or a['type']._type_affinity == c.type._type_affinity, (name, c.name)
                assert getattr(a['type'], 'length', None) == getattr(c.type, 'length', None), (name, c.name)
            assert inspector.get_pk_constraint(name)['constrained_columns'] == ['id']
            print(name, 'columns verified')
        def create_index(self, name, table, columns):
            indexes = {i['name']: i for i in inspector.get_indexes(table)}
            assert indexes[name]['column_names'] == columns, name
    path = Path('/app/alembic/versions/0003_orchestration_step_peer_history.py')
    spec = importlib.util.spec_from_file_location('migration', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = VerifyOperations()
    module.upgrade()
    conn.execute(text('ALTER TABLE alembic_version MODIFY version_num VARCHAR(128) NOT NULL'))
    conn.execute(text('UPDATE alembic_version SET version_num=:revision'), {'revision': module.revision})
    print('Verified all 0003 columns and indexes; recovered revision marker.')
