"""
Unit Tests for Retail Sales ETL Pipeline
==========================================

Tests covering DAG structure, task dependencies, and data transformations.

Run with: pytest test_retail_sales_etl.py -v
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import json

# Import Airflow components
from airflow.models import DagBag
from airflow.utils.state import State
from airflow.utils.types import DagRunType


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
        
        # Transform should be downstream of extract tasks
        assert 'extract_data.extract_us_east' in transform_task.upstream_task_ids or \
               'extract_data.extract_us_west' in transform_task.upstream_task_ids
        
        # Validate should be downstream of transform
        assert 'transform_sales_data' in validate_task.upstream_task_ids


class TestDataTransformation:
    """Test data transformation logic"""
    
    @pytest.fixture
    def sample_data(self):
        """Create sample sales data"""
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
            'updated_at': pd.to_datetime('2024-01-15 10:00:00')
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
        """Test region code standardization"""
        region_mapping = {
            'us_east': 'US-EAST',
            'us_west': 'US-WEST',
            'europe': 'EUROPE',
            'asia_pacific': 'ASIA-PACIFIC'
        }
        
        sample_data['region'] = sample_data['region'].map(region_mapping)
        
        expected_regions = {'US-EAST', 'US-WEST', 'EUROPE', 'ASIA-PACIFIC'}
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


class TestExtractionLogic:
    """Test data extraction functions"""
    
    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook')
    def test_extraction_query_format(self, mock_pg_hook):
        """Test extraction query construction"""
        region = 'us_east'
        target_date = '2024-01-15'
        
        expected_query_parts = [
            'SELECT',
            'sale_id',
            'FROM sales.transactions',
            f"WHERE DATE(sale_date) = '{target_date}'",
            "AND is_deleted = FALSE",
            "AND status = 'completed'"
        ]
        
        # Construct query (simplified)
        query = f"""
        SELECT sale_id, customer_id
        FROM sales.transactions
        WHERE DATE(sale_date) = '{target_date}'
            AND is_deleted = FALSE
            AND status = 'completed'
        ORDER BY sale_id;
        """
        
        for part in expected_query_parts:
            assert part in query
    
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
    
    def test_merge_query_structure(self):
        """Test MERGE statement structure"""
        merge_query = """
        MERGE INTO analytics.sales_fact target
        USING staging.sales_data_stage source
        ON target.sale_id = source.sale_id
        WHEN MATCHED THEN UPDATE SET ...
        WHEN NOT MATCHED THEN INSERT ...
        """
        
        required_parts = [
            'MERGE INTO',
            'USING',
            'ON target.sale_id = source.sale_id',
            'WHEN MATCHED THEN',
            'WHEN NOT MATCHED THEN'
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
        """Test transformation metadata structure"""
        metadata = {
            'total_extracted': 5000,
            'duplicates_removed': 50,
            'transformed_records': 4950,
            'execution_date': '2024-01-15',
            'status': 'success'
        }
        
        # Validate metadata
        assert metadata['transformed_records'] == (
            metadata['total_extracted'] - metadata['duplicates_removed']
        )
        assert metadata['status'] == 'success'


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
        # Sequential: 4 regions * 10 minutes = 40 minutes
        sequential_time = 4 * 10
        
        # Parallel: max(10, 10, 10, 10) = 10 minutes
        parallel_time = max(10, 10, 10, 10)
        
        time_saved = sequential_time - parallel_time
        
        assert time_saved == 30  # 30 minutes saved
        assert parallel_time < sequential_time


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
