# from helper.schema import Schema
# from helper.schema_manager import SchemaManager

# employee = Schema()
# employee.add_attribute("id", "int", 4)
# employee.add_attribute("name", "char", 30)
# employee.add_attribute("salary", "float", 8)

# department = Schema()
# department.add_attribute("dept_id", "int", 4)
# department.add_attribute("dept_name", "char", 20)

# manager = SchemaManager()
# manager.add_table_schema("employee", employee)
# manager.add_table_schema("department", department)

# manager.save_schemas()

# loader = SchemaManager()
# loader.load_schemas()

# print("Loaded table schemas:")
# print(loader.list_tables())      
# print("\nEmployee table schema:")
# print(loader.get_table_schema("employee"))
# print("\nDepartment table schema:")   
# print(loader.get_table_schema("department"))
