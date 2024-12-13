import json
from Query_Processor.classes.query_processor import QueryProcessor
from Query_Processor.classes.execution_result import ExecutionResult
from dataclasses import is_dataclass, asdict

def custom_json_serializer(obj):
    if is_dataclass(obj):
        return asdict(obj)
    
    if hasattr(obj, "to_dict"):
        return obj.to_dict()

    if hasattr(obj, "__dict__"):
        return obj.__dict__  # Perbaikan di sini untuk menangani objek hasil query.

    if hasattr(obj, "isoformat"):
        return obj.isoformat()

    try:
        return str(obj)
    except Exception:
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


# Query execution and result handling
query_processor = QueryProcessor()

def execute_and_print_query(query):
    try:
        result = query_processor.execute_query(query)
        if result is not None:
            # Serialize the result using the custom JSON serializer
            serialized_result = json.dumps(result, default=custom_json_serializer, indent=4)
            print("Query Result:")
            print(serialized_result)
        else:
            print("No results returned.")
    except Exception as e:
        print(f"Error executing query: {e}")

# Example usage
query = "SELECT * FROM students;"
print(f"Executing query: {query}")
execute_and_print_query(query)

query = "SELECT * FROM attends;"
print(f"Executing query: {query}")
execute_and_print_query(query)

query = "SELECT * FROM courses;"
print(f"Executing query: {query}")
execute_and_print_query(query)

query = "SELECT * FROM students, courses;"
print(f"Executing query: {query}")
execute_and_print_query(query)

query = "SELECT * FROM students, attends;"
print(f"Executing query: {query}")
execute_and_print_query(query)

query = "SELECT * FROM courses, attends;"
print(f"Executing query: {query}")
execute_and_print_query(query)

query = "SELECT * FROM students, courses, attends;"
print(f"Executing query: {query}")
execute_and_print_query(query)

# testing untuk AS
query = "select s.fullname from students as s;"
print(f"Executing query: {query}")
execute_and_print_query(query)

# testing untuk JOIN ON
query = "SELECT * FROM students JOIN Attends ON Students.studentid = Attends.studentid;"
print(f"Executing query: {query}")
execute_and_print_query(query)

#testing untuk WHERE
query = "SELECT * FROM students WHERE studentid = 1;"
print(f"Executing query: {query}")
execute_and_print_query(query)

#testing untuk ORDER BY
query = "SELECT * FROM students ORDER BY studentid DESC;"
print(f"Executing query: {query}")
execute_and_print_query(query)