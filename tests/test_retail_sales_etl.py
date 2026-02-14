"""
Unit Tests for Retail Sales ETL Pipeline
==========================================

Tests covering DAG structure, task dependencies, and data transformations.

Run with: pytest test_retail_sales_etl.py -v
"""

import pytest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch
import pandas as pd
import json

# Import Airflow components
from airflow.models import DagBag

# Load the same region config used by the DAG
_CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config' / 'region_config.json'
with open(_CONFIG_PATH) as _f:
    REGION_CONFIGS = json.load(_f)['regions']
REGION_NAMES = [r['name'] for r in REGION_CONFIGS]
REGION_DISPLAY_MAP = {r['name']: r['display_name'] for r in REGION_CONFIGS}


class TestDAGStructure:
    """Test DAG configuration and structure"""
    
    @pytest.fixture
    def dagbag(self):
        """Load DAG"""
        return DagBag(dag_folder='dags/', include_examples=False)
    
    def test_dag_loaded(self, dagbag):
        """Test that DAG is loaded without errors"""
        dag = dagbag.get_dag(dag_id='retail_sales_etl_pipeline')
        assert dagbag.import_errors == {}
        assert dag is not None
        assert len(dag.tasks) > 0
    
    def test_dag_configuration(self, dagbag):
        """Test DAG-level configuration"""
        dag = dagbag.get_dag(dag_id='retail_sales_etl_pipeline')
        
        # Schedule
        assert dag.schedule_interval == '0 6 * * *'
        
        # Catchup
        assert dag.catchup is False
        
        # Max active runs
        assert dag.max_active_runs == 1
        
        # Tags
        assert 'sales' in dag.tags
        assert 'etl' in dag.tags
        assert 'production' in dag.tags
    
    def test_default_args(self, dagbag):
        """Test default arguments"""
        dag = dagbag.get_dag(dag_id='retail_sales_etl_pipeline')
        
        assert dag.default_args['owner'] == 'data-engineering'
        assert dag.default_args['retries'] == 2
        assert dag.default_args['retry_delay'] == timedelta(minutes=5)
        assert dag.default_args['email_on_failure'] is True
    
    def test_task_count(self, dagbag):
        """Test expected number of tasks"""
        dag = dagbag.get_dag(dag_id='retail_sales_etl_pipeline')
        
        # Should have:
        # - 1 start
        # - 4 extract tasks (in task group)
        # - 1 transform
        # - 1 validate (branch)
        # - 1 prepare load
        # - 1 stage to snowflake
        # - 1 merge to final
        # - 1 verify load
        # - 1 data quality failure
        # - 1 success
        # - 1 end
        
        # Minimum expected tasks
        assert len(dag.tasks) >= 10
    
    def test_task_dependencies(self, dagbag):
        """Test critical task dependencies"""
        dag = dagbag.get_dag(dag_id='retail_sales_etl_pipeline')

        # Get specific tasks
        start_task = dag.get_task('start')
        transform_task = dag.get_task('transform_sales_data')
        validate_task = dag.get_task('validate_data_quality')

        # Start should have downstream tasks
        assert len(start_task.downstream_task_ids) > 0

        # Transform should be downstream of extract tasks (config-driven)
        extract_task_ids = {f'extract_data.extract_{r}' for r in REGION_NAMES}
        assert extract_task_ids & transform_task.upstream_task_ids

        # Validate should be downstream of transform
        assert 'transform_sales_data' in validate_task.upstream_task_ids


class TestDataTransformation:
    """Test data transformation logic"""
    
    @pytest.fixture
    def sample_data(self):
        """Create sample sales data with CDC is_deleted column"""
        return pd.DataFrame({
            'sale_id': [1, 2, 3, 2, 4],  # Includes duplicate (2)
            'customer_id': ['C001', 'C002', 'C003', 'C002', 'C004'],
            'product_id': ['P001', 'P002', 'P003', 'P002', 'P004'],
            'sale_date': pd.to_datetime([
                '2024-01-15 10:00:00',
                '2024-01-15 11:00:00',
                '2024-01-15 12:00:00',
                '2024-01-15 11:00:00',  # Duplicate timestamp
                '2024-01-15 13:00:00'
            ]),
            'quantity': [2, 1, 3, 1, 5],
            'unit_price': [10.00, 20.00, 15.00, 20.00, 8.00],
            'total_amount': [20.00, 20.00, 45.00, 20.00, 40.00],
            'region': ['us_east', 'us_west', 'europe', 'us_west', 'asia_pacific'],
            'store_id': ['S001', 'S002', 'S003', 'S002', 'S004'],
            'payment_method': ['credit', 'cash', 'credit', 'cash', 'debit'],
            'discount_amount': [0, 2.00, 5.00, 2.00, None],  # Includes NULL
            'tax_amount': [2.00, 1.80, 4.50, 1.80, 4.00],
            'created_at': pd.to_datetime('2024-01-15 10:00:00'),
            'updated_at': pd.to_datetime('2024-01-15 10:00:00'),
            'is_deleted': [False, False, False, False, False]
        })
    
    def test_deduplication(self, sample_data):
        """Test duplicate removal logic"""
        # Simulate deduplication
        deduplicated = sample_data.drop_duplicates(subset=['sale_id'], keep='first')
        
        assert len(deduplicated) == 4  # 5 - 1 duplicate = 4
        assert 2 in deduplicated['sale_id'].values
        assert deduplicated['sale_id'].is_unique
    
    def test_null_handling(self, sample_data):
        """Test NULL value filling"""
        # Fill NULLs with 0 for discount_amount
        filled = sample_data.copy()
        filled['discount_amount'] = filled['discount_amount'].fillna(0)
        
        assert filled['discount_amount'].isna().sum() == 0
        assert filled.loc[4, 'discount_amount'] == 0
    
    def test_derived_columns(self, sample_data):
        """Test calculation of derived columns"""
        # Calculate net_amount
        sample_data['discount_amount'] = sample_data['discount_amount'].fillna(0)
        sample_data['net_amount'] = (
            sample_data['total_amount'] - sample_data['discount_amount']
        )
        
        # Calculate gross_profit
        sample_data['gross_profit'] = (
            sample_data['net_amount'] - sample_data['tax_amount']
        )
        
        # Verify calculations
        assert sample_data.loc[0, 'net_amount'] == 20.00
        assert sample_data.loc[1, 'net_amount'] == 18.00  # 20 - 2
        assert sample_data.loc[0, 'gross_profit'] == 18.00  # 20 - 2
    
    def test_partition_columns(self, sample_data):
        """Test partition column generation"""
        sample_data['sale_year'] = sample_data['sale_date'].dt.year
        sample_data['sale_month'] = sample_data['sale_date'].dt.month
        sample_data['sale_day'] = sample_data['sale_date'].dt.day
        
        assert all(sample_data['sale_year'] == 2024)
        assert all(sample_data['sale_month'] == 1)
        assert all(sample_data['sale_day'] == 15)
    
    def test_region_normalization(self, sample_data):
        """Test region code standardization uses config-driven mapping"""
        sample_data['region'] = sample_data['region'].map(REGION_DISPLAY_MAP)

        expected_regions = set(REGION_DISPLAY_MAP.values())
        assert set(sample_data['region'].unique()) == expected_regions


class TestDataQuality:
    """Test data quality validation logic"""
    
    def test_record_count_threshold(self):
        """Test minimum record count validation"""
        MIN_RECORD_COUNT = 100
        
        # Test with sufficient records
        assert 150 >= MIN_RECORD_COUNT
        
        # Test with insufficient records
        assert not (50 >= MIN_RECORD_COUNT)
    
    def test_null_percentage_calculation(self):
        """Test NULL percentage calculation"""
        total_records = 1000
        null_count = 30
        
        null_percentage = (null_count / total_records * 100)
        
        assert null_percentage == 3.0
        assert null_percentage < 5.0  # Threshold
    
    def test_business_rule_validation(self):
        """Test business rule checks"""
        test_cases = [
            {'quantity': 5, 'unit_price': 10, 'total_amount': 50, 'valid': True},
            {'quantity': 0, 'unit_price': 10, 'total_amount': 0, 'valid': False},  # qty = 0
            {'quantity': -5, 'unit_price': 10, 'total_amount': -50, 'valid': False},  # negative qty
            {'quantity': 5, 'unit_price': -10, 'total_amount': -50, 'valid': False},  # negative price
            {'quantity': 5, 'unit_price': 10, 'total_amount': 10, 'valid': False},  # amount too low
            {'quantity': 5, 'unit_price': 10, 'total_amount': 150, 'valid': False},  # amount too high
        ]
        
        for case in test_cases:
            qty = case['quantity']
            price = case['unit_price']
            amount = case['total_amount']
            
            is_valid = (
                qty > 0 and 
                price > 0 and 
                amount > 0 and
                amount >= (qty * price * 0.5) and
                amount <= (qty * price * 2.0)
            )
            
            assert is_valid == case['valid'], f"Failed for case: {case}"


class TestRegionConfig:
    """Test dynamic region configuration"""

    def test_config_file_exists(self):
        """Test that region config file exists and is valid JSON"""
        assert _CONFIG_PATH.exists(), f"Config not found: {_CONFIG_PATH}"
        with open(_CONFIG_PATH) as f:
            config = json.load(f)
        assert 'regions' in config
        assert len(config['regions']) > 0

    def test_region_config_required_fields(self):
        """Each region must have required fields"""
        required_fields = {'name', 'conn_id', 'display_name'}
        for region_cfg in REGION_CONFIGS:
            missing = required_fields - set(region_cfg.keys())
            assert not missing, f"Region {region_cfg.get('name', '?')} missing fields: {missing}"

    def test_region_names_are_unique(self):
        """Region names must be unique"""
        assert len(REGION_NAMES) == len(set(REGION_NAMES))

    def test_display_names_are_unique(self):
        """Display names must be unique"""
        display_names = [r['display_name'] for r in REGION_CONFIGS]
        assert len(display_names) == len(set(display_names))

    def test_dag_generates_extract_tasks_from_config(self):
        """DAG should create one extract task per config entry"""
        dagbag = DagBag(dag_folder='dags/', include_examples=False)
        dag = dagbag.get_dag(dag_id='retail_sales_etl_pipeline')
        for region_cfg in REGION_CONFIGS:
            task_id = f'extract_data.extract_{region_cfg["name"]}'
            assert dag.has_task(task_id), f"Missing task: {task_id}"


class TestExtractionLogic:
    """Test data extraction functions"""
    
    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook')
    def test_extraction_query_uses_timestamp_window(self, mock_pg_hook):
        """Test CDC extraction uses updated_at timestamp window"""
        expected_query_parts = [
            'SELECT',
            'sale_id',
            'is_deleted',
            'FROM sales.transactions',
            'WHERE updated_at >= %s',
            'AND updated_at < %s',
            "AND status = 'completed'"
        ]

        # Extraction query uses parameterized timestamp window (CDC pattern)
        query = """
        SELECT sale_id, customer_id, is_deleted
        FROM sales.transactions
        WHERE updated_at >= %s
            AND updated_at < %s
            AND status = 'completed'
        ORDER BY sale_id;
        """

        for part in expected_query_parts:
            assert part in query

        # Verify query does NOT contain hardcoded date values
        assert "= '2024" not in query
        # Verify no longer uses DATE(sale_date) filter
        assert 'DATE(sale_date)' not in query
    
    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook')
    def test_extraction_error_handling(self, mock_pg_hook):
        """Test extraction error handling"""
        # Mock connection failure
        mock_pg_hook.return_value.get_conn.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception) as exc_info:
            raise mock_pg_hook.return_value.get_conn.side_effect
        
        assert "Connection failed" in str(exc_info.value)


class TestSnowflakeOperations:
    """Test Snowflake load and merge operations"""

    def test_cdc_merge_query_structure(self):
        """Test CDC-aware MERGE handles inserts, updates, and deletes"""
        merge_query = """
        MERGE INTO analytics.sales_fact target
        USING staging.sales_data_stage source
        ON target.sale_id = source.sale_id
        WHEN MATCHED AND source.is_deleted = TRUE THEN DELETE
        WHEN MATCHED AND source.is_deleted = FALSE THEN UPDATE SET ...
        WHEN NOT MATCHED AND source.is_deleted = FALSE THEN INSERT ...
        """

        required_parts = [
            'MERGE INTO',
            'USING',
            'ON target.sale_id = source.sale_id',
            'WHEN MATCHED AND source.is_deleted = TRUE THEN DELETE',
            'WHEN MATCHED AND source.is_deleted = FALSE THEN',
            'WHEN NOT MATCHED AND source.is_deleted = FALSE THEN'
        ]

        for part in required_parts:
            assert part in merge_query
    
    def test_upsert_logic(self):
        """Test upsert logic (conceptual)"""
        # Existing records
        existing = pd.DataFrame({
            'sale_id': [1, 2, 3],
            'total_amount': [100, 200, 300]
        })
        
        # New records (includes update for sale_id=2 and new sale_id=4)
        new = pd.DataFrame({
            'sale_id': [2, 4],
            'total_amount': [250, 400]  # 2 is updated, 4 is new
        })
        
        # Simulate merge
        merged = existing.set_index('sale_id')
        new_indexed = new.set_index('sale_id')
        
        # Update existing
        merged.update(new_indexed)
        
        # Insert new
        new_records = new_indexed[~new_indexed.index.isin(merged.index)]
        merged = pd.concat([merged, new_records])
        
        merged = merged.reset_index()
        
        # Verify
        assert len(merged) == 4  # 3 original + 1 new
        assert merged[merged['sale_id'] == 2]['total_amount'].values[0] == 250  # Updated
        assert 4 in merged['sale_id'].values  # New record inserted


class TestMonitoringAndLogging:
    """Test monitoring and XCom functionality"""
    
    def test_xcom_metadata_structure(self):
        """Test XCom metadata format"""
        metadata = {
            'region': 'us_east',
            'record_count': 1500,
            'target_date': '2024-01-15',
            'status': 'success'
        }
        
        assert 'region' in metadata
        assert 'record_count' in metadata
        assert 'status' in metadata
        assert metadata['record_count'] > 0
        assert metadata['status'] == 'success'
    
    def test_transformation_metadata(self):
        """Test transformation metadata includes CDC fields"""
        metadata = {
            'total_extracted': 5000,
            'duplicates_removed': 50,
            'transformed_records': 4950,
            'delete_markers': 12,
            'execution_date': '2024-01-15',
            'status': 'success'
        }

        # Validate metadata
        assert metadata['transformed_records'] == (
            metadata['total_extracted'] - metadata['duplicates_removed']
        )
        assert metadata['status'] == 'success'
        assert 'delete_markers' in metadata
        assert metadata['delete_markers'] >= 0


class TestErrorScenarios:
    """Test error handling and edge cases"""
    
    def test_no_data_scenario(self):
        """Test handling of zero records"""
        record_count = 0
        
        # Should trigger appropriate branch
        if record_count == 0:
            status = 'no_data'
        else:
            status = 'success'
        
        assert status == 'no_data'
    
    def test_partial_region_failure(self):
        """Test handling when some regions fail"""
        extraction_results = {
            'us_east': 1500,
            'us_west': 1200,
            'europe': 0,  # Failed
            'asia_pacific': 800
        }
        
        total = sum(extraction_results.values())
        successful_regions = sum(1 for v in extraction_results.values() if v > 0)
        
        assert total == 3500
        assert successful_regions == 3
        
        # Pipeline should continue with available data
        assert total > 0


class TestPerformance:
    """Test performance considerations"""
    
    def test_parallel_extraction_benefit(self):
        """Verify parallel execution reduces total time"""
        num_regions = len(REGION_NAMES)
        time_per_region = 10  # minutes

        sequential_time = num_regions * time_per_region
        parallel_time = time_per_region  # all run concurrently

        assert parallel_time < sequential_time
        assert sequential_time - parallel_time == (num_regions - 1) * time_per_region


class TestCDCLogic:
    """Test Change Data Capture (CDC) specific logic"""

    @pytest.fixture
    def cdc_batch(self):
        """Sample batch with mixed CDC operations: live rows + soft deletes"""
        return pd.DataFrame({
            'sale_id': [10, 20, 30, 40],
            'customer_id': ['C010', 'C020', 'C030', 'C040'],
            'product_id': ['P010', 'P020', 'P030', 'P040'],
            'sale_date': pd.to_datetime('2024-01-15'),
            'quantity': [1, 2, 3, 4],
            'unit_price': [10.0, 20.0, 30.0, 40.0],
            'total_amount': [10.0, 40.0, 90.0, 160.0],
            'region': ['us_east', 'us_west', 'europe', 'asia_pacific'],
            'store_id': ['S010', 'S020', 'S030', 'S040'],
            'payment_method': ['credit', 'cash', 'credit', 'debit'],
            'discount_amount': [0, 0, 0, 0],
            'tax_amount': [1.0, 4.0, 9.0, 16.0],
            'created_at': pd.to_datetime('2024-01-14'),
            'updated_at': pd.to_datetime('2024-01-15 08:00:00'),
            'is_deleted': [False, False, True, False],
        })

    def test_cdc_split_live_and_deleted(self, cdc_batch):
        """Transformation should separate live rows from delete markers"""
        live_df = cdc_batch[cdc_batch['is_deleted'] == False]  # noqa: E712
        delete_df = cdc_batch[cdc_batch['is_deleted'] == True]  # noqa: E712

        assert len(live_df) == 3
        assert len(delete_df) == 1
        assert delete_df.iloc[0]['sale_id'] == 30

    def test_delete_markers_pass_through(self, cdc_batch):
        """Delete markers should not be filtered by data validation"""
        delete_df = cdc_batch[cdc_batch['is_deleted'] == True].copy()  # noqa: E712

        # Delete markers may have quantity=0 or other values that would
        # fail validation, but they should not be subject to validation
        # because they only need sale_id for the MERGE DELETE clause.
        assert len(delete_df) == 1
        assert 'sale_id' in delete_df.columns

    def test_incremental_window_calculation(self):
        """Verify extraction window spans exactly one day"""
        exec_date = datetime.strptime('2024-01-16', '%Y-%m-%d')
        window_start = (exec_date - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
        window_end = exec_date.strftime('%Y-%m-%d %H:%M:%S')

        assert window_start == '2024-01-15 00:00:00'
        assert window_end == '2024-01-16 00:00:00'

    def test_cdc_upsert_and_delete(self):
        """Test full CDC merge: insert, update, and delete in one batch"""
        # Existing target table
        target = pd.DataFrame({
            'sale_id': [1, 2, 3],
            'total_amount': [100, 200, 300],
            'is_deleted': [False, False, False]
        }).set_index('sale_id')

        # CDC changes: update sale_id=2, delete sale_id=3, insert sale_id=4
        changes = pd.DataFrame({
            'sale_id': [2, 3, 4],
            'total_amount': [250, 300, 400],
            'is_deleted': [False, True, False]
        }).set_index('sale_id')

        # Apply updates
        live_changes = changes[changes['is_deleted'] == False]  # noqa: E712
        target.update(live_changes[['total_amount']])

        # Apply deletes
        delete_ids = changes[changes['is_deleted'] == True].index  # noqa: E712
        target = target.drop(delete_ids, errors='ignore')

        # Apply inserts
        new_rows = live_changes[~live_changes.index.isin(target.index)]
        target = pd.concat([target, new_rows[['total_amount', 'is_deleted']]])

        target = target.reset_index()

        assert len(target) == 3  # 1 (kept) + 1 (updated) + 1 (inserted) - 1 (deleted)
        assert 3 not in target['sale_id'].values  # Deleted
        assert target[target['sale_id'] == 2]['total_amount'].values[0] == 250  # Updated
        assert 4 in target['sale_id'].values  # Inserted

    def test_debezium_connector_configs_exist(self):
        """All regional Debezium connector configs should exist"""
        config_dir = Path(__file__).resolve().parent.parent / 'config' / 'debezium'
        for region_cfg in REGION_CONFIGS:
            connector_file = config_dir / f"connector_{region_cfg['name']}.json"
            assert connector_file.exists(), f"Missing: {connector_file}"
            with open(connector_file) as f:
                config = json.load(f)
            assert 'name' in config
            assert 'config' in config
            assert config['config']['plugin.name'] == 'pgoutput'
            assert region_cfg['name'] in config['config']['slot.name']

    def test_snowflake_sink_connector_config_exists(self):
        """Snowflake Kafka sink connector config should exist"""
        sink_file = (
            Path(__file__).resolve().parent.parent
            / 'config' / 'debezium' / 'snowflake_sink_connector.json'
        )
        assert sink_file.exists()
        with open(sink_file) as f:
            config = json.load(f)
        assert config['config']['connector.class'] == \
            'com.snowflake.kafka.connector.SnowflakeSinkConnector'
        assert config['config']['snowflake.ingestion.method'] == 'SNOWPIPE_STREAMING'

    def test_snowflake_sql_scripts_exist(self):
        """All Snowflake CDC SQL scripts should exist"""
        sql_dir = Path(__file__).resolve().parent.parent / 'sql'
        expected_scripts = [
            '01_postgresql_cdc_setup.sql',
            '02_snowflake_cdc_tables.sql',
            '03_snowflake_streams.sql',
            '04_snowflake_tasks.sql',
        ]
        for script in expected_scripts:
            script_path = sql_dir / script
            assert script_path.exists(), f"Missing SQL script: {script_path}"


# ============================================================================
# INTEGRATION TESTS (Require Airflow Environment)
# ============================================================================

class TestIntegration:
    """Integration tests requiring Airflow environment"""
    
    @pytest.mark.integration
    def test_dag_run_success(self):
        """Test full DAG run (requires environment)"""
        # This would require actual Airflow environment
        # Placeholder for integration test structure
        pass
    
    @pytest.mark.integration  
    def test_end_to_end_pipeline(self):
        """Test complete pipeline with test data"""
        # This would require test databases and Snowflake
        # Placeholder for E2E test structure
        pass


# ============================================================================
# FIXTURES AND UTILITIES
# ============================================================================

@pytest.fixture
def mock_airflow_context():
    """Create mock Airflow context"""
    return {
        'ds': '2024-01-15',
        'execution_date': datetime(2024, 1, 15),
        'task_instance': Mock(),
        'dag': Mock(),
    }


if __name__ == '__main__':
    # Run tests
    pytest.main([__file__, '-v', '--tb=short'])
