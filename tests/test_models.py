from sqlalchemy import create_engine, inspect

from reliable_pipeline.models import Base


def test_sqlalchemy_schema_creates_expected_tables() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    inspector = inspect(engine)
    assert set(inspector.get_table_names()) == {"import_jobs", "publications"}
    import_constraints = inspector.get_unique_constraints("import_jobs")
    assert any(item["column_names"] == ["idempotency_key"] for item in import_constraints)
