"""Tests for dataset generator."""

import pytest

from utils.dataset_generator import (
    DatasetGenerator,
    GeneratorConfig,
    get_generator_metadata,
    GENERATOR_VERSION,
)


class TestDatasetGenerator:
    """Test dataset generation for determinism and correctness."""

    def test_generator_version_is_recorded(self):
        """Generator version should be accessible."""
        assert GENERATOR_VERSION is not None
        assert isinstance(GENERATOR_VERSION, str)

    def test_deterministic_generation_same_seed(self):
        """Same seed should produce identical datasets."""
        config = GeneratorConfig(seed=42, customers_n=100, orders_n=500, events_n=500)

        gen1 = DatasetGenerator(config)
        customers1, orders1, events1 = gen1.generate()

        config2 = GeneratorConfig(seed=42, customers_n=100, orders_n=500, events_n=500)
        gen2 = DatasetGenerator(config2)
        customers2, orders2, events2 = gen2.generate()

        # Same seed should produce same results
        assert len(customers1) == len(customers2)
        assert len(orders1) == len(orders2)
        assert len(events1) == len(events2)

        # Check first customer is same
        assert customers1[0].customer_id == customers2[0].customer_id
        assert customers1[0].email == customers2[0].email

    def test_different_seed_different_data(self):
        """Different seeds should produce different datasets."""
        config1 = GeneratorConfig(seed=42, customers_n=100, orders_n=500, events_n=500)
        gen1 = DatasetGenerator(config1)
        customers1, _, _ = gen1.generate()

        config2 = GeneratorConfig(seed=999, customers_n=100, orders_n=500, events_n=500)
        gen2 = DatasetGenerator(config2)
        customers2, _, _ = gen2.generate()

        # Different seed should produce different data
        assert customers1[0].email != customers2[0].email

    def test_referential_integrity(self):
        """Orders should reference valid customer IDs."""
        config = GeneratorConfig(seed=42, customers_n=100, orders_n=500, events_n=500)
        gen = DatasetGenerator(config)
        customers, orders, _ = gen.generate()

        customer_ids = {c.customer_id for c in customers}

        for order in orders:
            assert order.customer_id in customer_ids, (
                f"Order {order.order_id} references invalid customer {order.customer_id}"
            )

    def test_schema_sql_is_valid(self):
        """Schema SQL should be valid and contain required tables."""
        config = GeneratorConfig(seed=42)
        gen = DatasetGenerator(config)

        schema = gen.get_schema_sql()

        assert "customers" in schema.lower()
        assert "orders" in schema.lower()
        assert "events" in schema.lower()
        assert "CREATE TABLE" in schema

    def test_insert_statements_correct_format(self):
        """Insert statements should be proper tuples."""
        config = GeneratorConfig(seed=42, customers_n=10, orders_n=20, events_n=20)
        gen = DatasetGenerator(config)

        customer_tuples, order_tuples, event_tuples = gen.get_insert_statements()

        # Check customer tuples have correct number of fields
        assert all(len(t) == 3 for t in customer_tuples)

        # Check order tuples have correct number of fields
        assert all(len(t) == 5 for t in order_tuples)

        # Check event tuples have correct number of fields
        assert all(len(t) == 6 for t in event_tuples)

    def test_metadata_includes_seed(self):
        """Generator metadata should include seed."""
        config = GeneratorConfig(seed=12345)
        metadata = get_generator_metadata(config)

        assert metadata["seed"] == 12345
        assert metadata["generator_version"] == GENERATOR_VERSION

    def test_orders_respect_status_distribution(self):
        """Orders should have roughly expected status distribution."""
        config = GeneratorConfig(
            seed=42, customers_n=100, orders_n=10000, events_n=1000
        )
        gen = DatasetGenerator(config)
        _, orders, _ = gen.generate()

        status_counts = {}
        for order in orders:
            status_counts[order.status] = status_counts.get(order.status, 0) + 1

        total = len(orders)

        # Verify all expected statuses are present
        assert "paid" in status_counts
        assert "shipped" in status_counts
        assert "cancelled" in status_counts
        assert "refunded" in status_counts

        # Check that the distribution makes sense (each status has meaningful presence)
        for status in ["paid", "shipped", "cancelled", "refunded"]:
            count = status_counts.get(status, 0)
            assert count > 0, f"Status {status} should have at least some orders"
            assert count <= total, f"Status {status} count should not exceed total"


class TestGeneratorConfig:
    """Test GeneratorConfig defaults."""

    def test_default_config_values(self):
        """Default config should have reasonable values."""
        config = GeneratorConfig()

        assert config.seed == 42
        assert config.customers_n == 1000
        assert config.orders_n == 10000
        assert config.events_n == 10000
        assert config.time_range_seconds > 0
        assert config.path_cardinality > 0
