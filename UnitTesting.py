import os
import shutil
from StorageManager import StorageManager
from storagemanager_model.data_retrieval import DataRetrieval
from storagemanager_model.data_write import DataWrite
from storagemanager_model.condition import Condition
from storagemanager_model.data_deletion import DataDeletion

def print_section(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)

def test_list_tables(sm: StorageManager):
    print_section("TEST 1: LIST TABLES (schema.dat)")
    tables = sm.schema_manager.list_tables()
    print("Tables found:", tables)

def test_select_all(sm: StorageManager, table):
    print_section(f"TEST 2: SELECT * FROM {table}")
    req = DataRetrieval(
        table=table,
        column="*"
    )
    rows = sm.read_block(req)
    print(f"Found {len(rows)} rows")
    for r in rows:
        print(r)

def test_select_projection(sm: StorageManager, table, cols):
    print_section(f"TEST 3: SELECT {cols} FROM {table}")
    req = DataRetrieval(
        table=table,
        column=cols
    )
    rows = sm.read_block(req)
    print(f"Found {len(rows)} rows")
    for r in rows:
        print(r)

def test_select_where(sm: StorageManager, table, col, op, val):
    print_section(f"TEST 4: SELECT * FROM {table} WHERE {col} {op} {val}")
    cond = Condition(col, op, val)
    req = DataRetrieval(
        table=table,
        column="*",
        conditions=[cond]
    )
    rows = sm.read_block(req)
    print(f"Found {len(rows)} rows")
    for r in rows:
        print(r)

def test_insert_record(sm: StorageManager):
    print_section("TEST 9: INSERT RECORD INTO Student")
    new_student = {
        "StudentID": 999,
        "FullName": "Test Student",
        "GPA": 3.75
    }
    write_req = DataWrite(
        table = "Student",
        column = None,
        conditions = [],
        new_value = new_student
    )
    sm.write_block(write_req)
    print("Inserted new student record:", new_student)

    print_section("VERIFY INSERTION: SELECT * FROM Student WHERE StudentID = 999")
    cond = Condition("StudentID", "=", 999)
    read_req = DataRetrieval(
        table="Student",
        column="*",
        conditions=[cond]
    )
    rows = sm.read_block(read_req)
    print(f"Found {len(rows)} rows")
    for r in rows:
        print(r)

def test_update_record(sm: StorageManager):
    print_section("TEST 10: UPDATE RECORD IN Student")
    write_req = DataWrite(
        table = "Student",
        column = "GPA",
        conditions = [Condition("StudentID", "=", 3)],
        new_value = 3.95
    )
    row_affected = sm.write_block(write_req)
    print(f"Rows affected: {row_affected}")
    print("Updated GPA of StudentID 3 to 3.95")

    print_section("VERIFY UPDATE: SELECT * FROM Student WHERE StudentID = 3")
    cond = Condition("StudentID", "=", 3)
    read_req = DataRetrieval(
        table="Student",
        column="*",
        conditions=[cond]
    )
    rows = sm.read_block(read_req)
    print(f"Found {len(rows)} rows")
    for r in rows:
        print(r)

def test_delete_record(sm: StorageManager):
    print_section("TEST 11: DELETE RECORD FROM Student")
    print_section("CHECK BEFORE DELETION DELETION: SELECT * FROM Student WHERE StudentID = 4")
    cond = Condition("StudentID", "=", 4)
    read_req = DataRetrieval(
        table="Student",
        column="*",
        conditions=[cond]
    )
    rows = sm.read_block(read_req)
    print(f"Found {len(rows)} rows")
    for r in rows:
        print(r)
    delete_req = DataDeletion(
        table = "Student",
        conditions = [Condition("StudentID", "=", 4)]
    )
    row_affected = sm.delete_block(delete_req)
    print(f"Rows affected: {row_affected}")
    print("Deleted record with StudentID 4")

    print_section("VERIFY DELETION: SELECT * FROM Student WHERE StudentID = 4")
    cond = Condition("StudentID", "=", 4)
    read_req = DataRetrieval(
        table="Student",
        column="*",
        conditions=[cond]
    )
    rows = sm.read_block(read_req)
    print(f"Found {len(rows)} rows")
    for r in rows:
        print(r)

def main():
    sm = StorageManager()

    # 1. cek apakah schema.dat berhasil diload
    test_list_tables(sm)

    tables = sm.schema_manager.list_tables()

    # 2. test SELECT * untuk semua tabel
    for t in tables:
        test_select_all(sm, t)

    # 3. test SELECT kolom tertentu
    for t in tables:
        if t == "Student":
            test_select_projection(sm, t, ["StudentID"])
        elif t == "Course":
            test_select_projection(sm, t, ["CourseName", "Year"])

    # 4. test SELECT WHERE
    for t in tables:
        if t == "Student":
            try:
                test_select_where(sm, t, "StudentID", ">", 25)
            except:
                pass

        if t == "Course":
            try:
                test_select_where(sm, t, "CourseName", "=", "Database Systems")
            except:
                pass

    # 5. test error handling: pilih kolom yang tidak ada
    print_section("TEST 5: ERROR HANDLING - INVALID COLUMN")
    try:
        req = DataRetrieval(
            table="Student",
            column=["InvalidColumnExample"]
        )
        sm.read_block(req)
    except ValueError as ve:
        print("Caught expected error:", ve)

    # 6. test error handling: pilih tabel yang tidak ada
    print_section("TEST 6: ERROR HANDLING - INVALID TABLE")
    try:
        req = DataRetrieval(
            table="InvalidTableName",
            column=["StudentID"]
        )
        sm.read_block(req)
    except ValueError as ve:
        print("Caught expected error:", ve)


    # 7. test error handling: WHERE dengan kolom yang tidak ada
    print_section("TEST 7: ERROR HANDLING - INVALID COLUMN IN WHERE")
    try:
        cond = Condition("NonExistentColumn", "=", 10)
        req = DataRetrieval(
            table="Student",
            column="*",
            conditions=[cond]
        )
        sm.read_block(req)
    except ValueError as ve:
        print("Caught expected error:", ve)

    # 8. test error handling: file data tabel tidak ada
    print_section("TEST 8: ERROR HANDLING - MISSING DATA FILE")
    try:
        req = DataRetrieval(
            table="MissingDataFileTable",
            column="*"
        )
        sm.read_block(req)
    except (FileNotFoundError, ValueError) as fe:
        print("Caught expected error:", fe)

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
    choice = input("Run which tests? (1=read_block tests, 2=get_stats tests, 3=both): ").strip()
    
    if choice == "1" or choice == "3":
        print("\n" + "=" * 60)
        print("RUNNING READ_BLOCK TESTS")
        print("=" * 60)
        main()
    
    if choice == "2" or choice == "3":
        print("\n" + "=" * 60)
        print("RUNNING GET_STATS TESTS")
        print("=" * 60)
        
        try:
            test_get_stats_single_table()
            test_get_stats_all_tables()
            test_get_stats_empty_parameter()
            test_get_stats_nonexistent_table()
            test_blocking_factor_calculation()
            test_distinct_values()
            
            print("\n" + "=" * 60)
            print("All get_stats tests passed successfully!")
            print("=" * 60)
            
        except AssertionError as e:
            print(f"\n✗ Test failed: {e}")
        except Exception as e:
            print(f"\n✗ Error occurred: {e}")
            import traceback
            traceback.print_exc()
