import os
import shutil
from StorageManager import StorageManager

def test_get_stats_single_table():
    print("Test 1: get_stats with single table from data folder")
    
    storage = StorageManager()
    
    tables = storage.schema_manager.list_tables()
    print(f"  Available tables: {tables}")
    
    if len(tables) > 0:
        first_table = tables[0]
        stats = storage.get_stats(first_table)
        
        print(f"\n  Table: {first_table}")
        print(f"  n_r (number of tuples): {stats.n_r}")
        print(f"  b_r (number of blocks): {stats.b_r}")
        print(f"  l_r (tuple size): {stats.l_r} bytes")
        print(f"  f_r (blocking factor): {stats.f_r}")
        print(f"  v_a_r (distinct values): {stats.v_a_r}")
        
        assert stats.l_r > 0, f"Expected l_r > 0, got {stats.l_r}"
        assert stats.f_r > 0, f"Expected f_r > 0, got {stats.f_r}"
        
        print("  ✓ Test passed!")
    else:
        print("  ⚠ No tables found in data folder")

def test_get_stats_all_tables():
    print("\nTest 2: get_stats with all tables from data folder")
    
    storage = StorageManager('data')
    all_stats = storage.get_stats()
    
    print(f"  Found {len(all_stats)} tables")
    for table_name, stats in all_stats.items():
        print(f"\n  Table: {table_name}")
        print(f"    n_r: {stats.n_r}")
        print(f"    b_r: {stats.b_r}")
        print(f"    l_r: {stats.l_r}")
        print(f"    f_r: {stats.f_r}")
        print(f"    v_a_r: {stats.v_a_r}")
    
    assert isinstance(all_stats, dict), "Expected dict result"
    assert len(all_stats) > 0, f"Expected at least 1 table, got {len(all_stats)}"
    
    print("\n  ✓ Test passed!")

def test_get_stats_empty_parameter():
    print("\nTest 3: get_stats with empty string parameter")
    
    storage = StorageManager('data')
    all_stats = storage.get_stats('')
    
    print(f"  Found {len(all_stats)} tables")
    assert isinstance(all_stats, dict), "Expected dict result for empty string parameter"
    
    print("  ✓ Test passed!")

def test_get_stats_nonexistent_table():
    print("\nTest 4: get_stats with nonexistent table")
    
    storage = StorageManager('data')
    stats = storage.get_stats('nonexistent_table_xyz')
    
    print(f"  n_r: {stats.n_r}")
    print(f"  b_r: {stats.b_r}")
    
    assert stats.n_r == 0, f"Expected 0 tuples, got {stats.n_r}"
    assert stats.b_r == 0, f"Expected 0 blocks, got {stats.b_r}"
    
    print("  ✓ Test passed!")

def test_blocking_factor_calculation():
    print("\nTest 5: Verify blocking factor calculation (b_r = ⌈n_r / f_r⌉)")
    
    storage = StorageManager('data')
    tables = storage.schema_manager.list_tables()
    
    if len(tables) > 0:
        table_name = tables[0]
        stats = storage.get_stats(table_name)
        
        import math
        if stats.n_r > 0 and stats.f_r > 0:
            expected_b_r = math.ceil(stats.n_r / stats.f_r)
            
            print(f"  Table: {table_name}")
            print(f"  n_r: {stats.n_r}")
            print(f"  f_r: {stats.f_r}")
            print(f"  b_r: {stats.b_r}")
            print(f"  Expected b_r (⌈{stats.n_r}/{stats.f_r}⌉): {expected_b_r}")
            
            assert stats.b_r == expected_b_r, f"Expected b_r={expected_b_r}, got {stats.b_r}"
            
            print("Test passed!")
        else:
            print(f"Skipping test - table {table_name} is empty")
    else:
        print("No tables found in data folder")

def test_distinct_values():
    print("\nTest 6: Verify distinct values counting")
    
    storage = StorageManager('data')
    tables = storage.schema_manager.list_tables()
    
    if len(tables) > 0:
        table_name = tables[0]
        stats = storage.get_stats(table_name)
        
        print(f"  Table: {table_name}")
        print(f"  Distinct values per attribute:")
        for attr_name, count in stats.v_a_r.items():
            print(f"    {attr_name}: {count} distinct values")
        
        if stats.n_r > 0:
            for attr_name, count in stats.v_a_r.items():
                assert count > 0, f"Expected distinct values > 0 for {attr_name}"
                assert count <= stats.n_r, f"Distinct values ({count}) cannot exceed total rows ({stats.n_r})"
        
        print("  Test passed!")
    else:
        print("  No tables found in data folder")

if __name__ == '__main__':
    print("=" * 60)
    print("Running StorageManager get_stats() Tests (Using data folder)")
    print("=" * 60)
    
    try:
        test_get_stats_single_table()
        test_get_stats_all_tables()
        test_get_stats_empty_parameter()
        test_get_stats_nonexistent_table()
        test_blocking_factor_calculation()
        test_distinct_values()
        
        print("\n" + "=" * 60)
        print("All tests passed successfully!")
        print("=" * 60)
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
    except Exception as e:
        print(f"\n✗ Error occurred: {e}")
        import traceback
        traceback.print_exc()
