"""DAG integrity tests: verify DAG IDs and that all DAGs loaded without import errors."""

import migration_dag_folder_copy as m3
import migration_dag_iceberg as m2
import migration_dag_mapr_to_s3 as m1
import migration_dag_metadata as m4


class TestMaprToS3DagIntegrity:

    def test_dag_loads_with_correct_id(self):
        assert m1.dag_mapr_to_s3.dag_id == 'source_to_s3_migration'

    def test_excel_param_defined(self):
        assert 'excel_file_path' in m1.dag_mapr_to_s3.params


class TestIcebergDagIntegrity:

    def test_dag_loads_with_correct_id(self):
        assert m2.dag_iceberg.dag_id == 'iceberg_migration'

    def test_excel_param_defined(self):
        assert 'excel_file_path' in m2.dag_iceberg.params


class TestFolderCopyDagIntegrity:

    def test_dag_loads_with_correct_id(self):
        assert m3.dag_folder_copy.dag_id == 'folder_only_data_copy'

    def test_excel_param_defined(self):
        assert 'excel_file_path' in m3.dag_folder_copy.params

class TestS3MetadataDagIntegrity:

    def test_dag_loads_with_correct_id(self):
        assert m4.dag_s3_metadata.dag_id == 's3_to_s3_metadata_migration'

    def test_excel_param_defined(self):
        assert 'excel_file_path' in m4.dag_s3_metadata.params
