"""DAG integrity tests: verify DAG IDs and that all three DAGs loaded without import errors."""

import migration_dags_combined as m


class TestMaprToS3DagIntegrity:

    def test_dag_loads_with_correct_id(self):
        assert m.dag_mapr_to_s3.dag_id == 'mapr_to_s3_migration'

    def test_excel_param_defined(self):
        assert 'excel_file_path' in m.dag_mapr_to_s3.params


class TestIcebergDagIntegrity:

    def test_dag_loads_with_correct_id(self):
        assert m.dag_iceberg.dag_id == 'iceberg_migration'

    def test_excel_param_defined(self):
        assert 'excel_file_path' in m.dag_iceberg.params


class TestFolderCopyDagIntegrity:

    def test_dag_loads_with_correct_id(self):
        assert m.dag_folder_copy.dag_id == 'folder_only_data_copy'

    def test_excel_param_defined(self):
        assert 'excel_file_path' in m.dag_folder_copy.params

class TestS3MetadataDagIntegrity:

    def test_dag_loads_with_correct_id(self):
        assert m.dag_s3_metadata.dag_id == 's3_to_s3_metadata_migration'

    def test_excel_param_defined(self):
        assert 'excel_file_path' in m.dag_s3_metadata.params
