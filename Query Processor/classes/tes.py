from query_processor import QueryProcessor

query_processor = QueryProcessor()

# Example usage
queries = [
    "SELECT * FROM students, lectures;",
    "UPDATE employee SET salary=1.05*salary WHERE salary > 1000;",
    "FROM students, lecturers;",
    "SELECT name FROM student AS s, lecturer AS l WHERE s.lecturer_id=l.id;",
    "SELECT * FROM employees JOIN departments ON employees.dept_id=departments.id;",
    "SELECT name FROM employees WHERE age >= 30;",
    "SELECT * FROM employees ORDER BY salary DESC;",
    "SELECT name FROM students LIMIT 10;",
    "BEGIN TRANSACTION;",
    "COMMIT;",
    "DELETE FROM employee WHERE department=\"RnD\";",
    "INSERT INTO students (id, name, age) VALUES (1, 'John', 22);",
    "CREATE TABLE employee (id INT PRIMARY KEY, name VARCHAR(50));",
    "DR TABLE employee;"
]

for q in queries:
    print(f"Query: {q}\nValid: {query_processor.validateQuery(q)}\n")

# Contoh penggunaan
# query = "SELECT name FROM student JOIN lecturer ON student.lecturer_id=lecturer.id;"
# fixed_query = query_processor.fix_join_query(query)
# print("Query sebelum:", query)
# print("Query setelah:", fixed_query)