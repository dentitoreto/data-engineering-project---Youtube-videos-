def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"


def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name"


def test_dags_integrity(dagbag):
    # first subtest
    assert dagbag.import_errors == {}, f"Import errors found: {dagbag.import_errors}"
    print("=============")
    print(dagbag.import_errors)

    # second subtest
    expected_dag_ids = ["produce_json", "update_database", "run_data_quality"]
    loaded_dag_ids = list(dagbag.dags.keys())
    print("=============")
    print(dagbag.dags.keys())

    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, f"DAG {dag_id} is missing"

    # third subtest
    assert dagbag.size() == 3
    print("=============")
    print(dagbag.size())

    # fourth subtest
    expected_task_count = {
        "produce_json": 5,
        "update_database": 3,
        "run_data_quality": 2,
    }
    print("=============")
    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_count[dag_id]
        actual_count = len(dag.tasks)
        assert expected_count == actual_count, (
            f"DAG {dag_id} is having {actual_count} tasks and expected number of is {expected_count}"
        )
