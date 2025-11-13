import os
import sys
import random

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helper.schema import Schema
from helper.schema_manager import SchemaManager
from helper.row_serializer import RowSerializer
from helper.slotted_page import SlottedPage  


student = Schema()
student.add_attribute("StudentID", "int", 4)
student.add_attribute("FullName", "varchar", 50)
student.add_attribute("GPA", "float", 4)

attends = Schema()
attends.add_attribute("StudentID", "int", 4)
attends.add_attribute("CourseID", "int", 4)

course = Schema()
course.add_attribute("CourseID", "int", 4)
course.add_attribute("Year", "int", 4)
course.add_attribute("CourseName", "varchar", 50)
course.add_attribute("CourseDescription", "varchar", 255)

manager = SchemaManager()
manager.add_table_schema("Student", student)
manager.add_table_schema("Attends", attends)
manager.add_table_schema("Course", course)
manager.save_schemas()


first_names = ["Alice", "Bob", "Charlie", "David", "Eva", "Frank", "Grace", "Hannah", "Ivan", "Jill"]
last_names = ["Anderson", "Brown", "Clark", "Davis", "Evans", "Garcia", "Hall", "King", "Lee", "Moore"]
course_names = [
    "Database Systems", "Computer Networks", "Operating Systems", "Machine Learning",
    "Software Engineering", "Artificial Intelligence", "Computer Graphics",
    "Information Security", "Data Mining", "Mobile Programming"
]

serializer = RowSerializer()
os.makedirs("data", exist_ok=True)


students = [
    {"StudentID": i, "FullName": f"{random.choice(first_names)} {random.choice(last_names)}", "GPA": round(random.uniform(2.0, 4.0), 2)}
    for i in range(1, 51)
]

courses = [
    {"CourseID": i, "Year": random.choice([2023, 2024, 2025]),
     "CourseName": random.choice(course_names),
     "CourseDescription": f"This course covers advanced topics in {random.choice(course_names).lower()}."}
    for i in range(1, 51)
]

attends_records = [
    {"StudentID": random.randint(1, 50), "CourseID": random.randint(1, 50)}
    for _ in range(50)
]


def write_with_pages(table_name, schema, records):
    file_path = f"data/{table_name}.dat"
    page = SlottedPage()
    pages = []
    
    for i, record in enumerate(records):
        record_bytes = serializer.serialize(schema, record)
        try:
            page.add_record(record_bytes)
        except Exception as e:
            # Page is full, save it and create a new one
            if page.record_count > 0:
                pages.append(page)
                page = SlottedPage()
                try:
                    page.add_record(record_bytes)
                except Exception as e2:
                    print(f"ERROR: Record {i} is too large to fit in a page ({len(record_bytes)} bytes)")
                    print(f"Record: {record}")
                    raise e2
            else:
                print(f"ERROR: Record {i} is too large to fit in an empty page ({len(record_bytes)} bytes)")
                print(f"Record: {record}")
                raise e
    
    # Don't forget the last page
    if page.record_count > 0:
        pages.append(page)

    # Write all pages to file
    with open(file_path, "wb") as f:
        for p in pages:
            f.write(p.serialize())

    print(f"Wrote {len(records)} records into {len(pages)} page(s): {file_path}")


write_with_pages("Student", student, students)
write_with_pages("Course", course, courses)
write_with_pages("Attends", attends, attends_records)


loader = SchemaManager()
loader.load_schemas()

for table_name in loader.list_tables():
    schema = loader.get_table_schema(table_name)
    print(f"\nTable: {table_name}")
    for attr in schema.get_attributes():
        print(f"  - {attr['name']:15} {attr['type']:10} {attr['size']}")
