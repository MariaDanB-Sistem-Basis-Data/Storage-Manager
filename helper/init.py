import os
import random
from schema import Schema
from schema_manager import SchemaManager
from row_serializer import RowSerializer
from slotted_page import SlottedPage  


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
    
    for record in records:
        record_bytes = serializer.serialize(schema, record)
        try:
            page.add_record(record_bytes)
        except Exception:
            pages.append(page)
            page = SlottedPage()
            page.add_record(record_bytes)
    
    
    pages.append(page)

    
    with open(file_path, "wb") as f:
        for p in pages:
            f.write(p.serialize())

    print(f"Wrote {len(records)} records into {len(pages)} page(s): {file_path}")


write_with_pages("student", student, students)
write_with_pages("course", course, courses)
write_with_pages("attends", attends, attends_records)


loader = SchemaManager()
loader.load_schemas()

for table_name in loader.list_tables():
    schema = loader.get_table_schema(table_name)
    print(f"\nTable: {table_name}")
    for attr in schema.get_attributes():
        print(f"  - {attr['name']:15} {attr['type']:10} {attr['size']}")
