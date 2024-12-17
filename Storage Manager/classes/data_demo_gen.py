from serializer import *
import random

schema_student = {
    "table_name" : "Student",
    "columns": [
        {"name": "StudentID", "type": "int"},
        {"name": "Fullname", "type": "varchar", "length": 50},
        {"name": "GPA", "type": "float"},
    ],
}

schema_course = {
    "table_name": "Course",
    "columns": [
        {"name": "CourseID", "type": "int"},
        {"name": "Year", "type": "int"},
        {"name": "CourseName", "type": "varchar", "length": 50},
        {"name": "CourseDescription", "type": "varchar", "length": 255},
    ],
}

schema_attends = {
    "table_name": "Attends",
    "columns": [
        {"name": "StudentID", "type": "int"},
        {"name": "CourseID", "type": "int"},
    ],
}

rows_students = [
    {"StudentID": 13522122, "Fullname": "Maulvi Ziadinda Maulana", "GPA": 4.0},
    {"StudentID": 13522126, "Fullname": "Rizqika Mulia Pratama", "GPA": 4.0},
    {"StudentID": 13522130, "Fullname": "Justin Aditya Putra Prabakti", "GPA": 4.0},
    {"StudentID": 13522158, "Fullname": "Muhammad Rasheed Qais Tandjung", "GPA": 4.0},
    {"StudentID": 13522134, "Fullname": "Shabrina Maharani", "GPA": 4.0},
    {"StudentID": 13522145, "Fullname": "Farrel Natha Saskoro", "GPA": 4.0},
    {"StudentID": 13522155, "Fullname": "Axel Santadi Warih", "GPA": 4.0},
    {"StudentID": 13522136, "Fullname": "Muhammad Zaki", "GPA": 4.0},
    {"StudentID": 13522149, "Fullname": "Muhammad Dzaki Arta", "GPA": 4.0},
    {"StudentID": 13522138, "Fullname": "Andi Marihot Sitorus", "GPA": 4.0},
    {"StudentID": 13522163, "Fullname": "Atqiya Haydar Luqman", "GPA": 4.0},
    {"StudentID": 13522160, "Fullname": "Rayhan Ridhar Rahman", "GPA": 4.0},
    {"StudentID": 13522151, "Fullname": "Samy Muhammad Haikal", "GPA": 4.0},
    {"StudentID": 13522159, "Fullname": "Rafif Ardhinto Ichwantoro", "GPA": 4.0},
    {"StudentID": 13522150, "Fullname": "Albert Ghazaly", "GPA": 4.0},
    {"StudentID": 13522128, "Fullname": "Mohammad Andhika Fadillah", "GPA": 4.0},
    {"StudentID": 13522123, "Fullname": "Jimly Nur Arif", "GPA": 4.0},
    {"StudentID": 13522121, "Fullname": "Jonathan Emmanuel Saragih", "GPA": 4.0},
    {"StudentID": 13522152, "Fullname": "Muhammad Roihan", "GPA": 4.0},
    {"StudentID": 13522156, "Fullname": "Jason Fernando", "GPA": 4.0},
    {"StudentID": 13522146, "Fullname": "Muhammad Zaidan Sa'Dun Robbani", "GPA": 4.0},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Cattleya Arsenia Pratama", "GPA": 2.8},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Daffa Ananda Wibisono", "GPA": 1.6},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Fahira Nadine Santoso", "GPA": 3.7},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Reynaldo Putra Aditya", "GPA": 3.9},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Aisyah Khansa Permadi", "GPA": 0.5},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Hafiz Ramadhan Yusuf", "GPA": 2.2},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Shakira Amara Putri", "GPA": 3.4},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Naufal Alif Kurniawan", "GPA": 1.3},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Azka Faizal Hakim", "GPA": 2.1},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Zahra Annisa Fadilah", "GPA": 0.0},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Rizky Adrian Wibowo", "GPA": 3.8},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Nadira Syifa Kamila", "GPA": 1.6},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Fahmi Ardiansyah", "GPA": 2.7},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Putri Cahaya Kartika", "GPA": 3.4},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Farhan Maulana Akbar", "GPA": 3.9},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Keisha Amalia Rahma", "GPA": 0.3},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Ahmad Rifqi Althaf", "GPA": 1.2},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Syahrul Anwar Permana", "GPA": 3.5},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Fitria Nabila Zahra", "GPA": 2.1},
    {"StudentID": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "Fullname": "Eka Nur Pratami", "GPA": 2.8},
    {"StudentID": 13521007, "Fullname": "Matthew Mahendra", "GPA": 2.01},
    {"StudentID": 13521162, "Fullname": "Antonio Natthan Krishna", "GPA": 2.01},
    {"StudentID": 13521100, "Fullname": "Alexander Jason", "GPA": 2.01},
    {"StudentID": 13521116, "Fullname": "Juan Christopher Santoso", "GPA": 2.01},
    {"StudentID": 13521155, "Fullname": "Kandida Edgina Gunawan", "GPA": 2.01},
    {"StudentID": 13521127, "Fullname": "Marcel Ryan Antony", "GPA": 2.01},
    {"StudentID": 23534042, "Fullname": "Fawwaz Anugrah Wiradhika Dharmasatya", "GPA": 2.01},
    {"StudentID": 23524065, "Fullname": "Christine Hutabarat", "GPA": 2.01},
    {"StudentID": 23524046, "Fullname": "Hana Fathiyah", "GPA": 2.01}
]

rows_course = [
    {"CourseID": 1102, "Year": 2022, "CourseName": "Berpikir Komputasional", "CourseDescription": ""},
    {"CourseID": 1103, "Year": 2022, "CourseName": "Pengantar Prinsip Keberlanjutan", "CourseDescription": ""},
    {"CourseID": 1111, "Year": 2022, "CourseName": "Laboratorium Fisika Dasar", "CourseDescription": ""},
    {"CourseID": 1116, "Year": 2022, "CourseName": "Laboratorium Interaksi Komputer", "CourseDescription": ""},
    {"CourseID": 1112, "Year": 2022, "CourseName": "Matematika I", "CourseDescription": ""},
    {"CourseID": 1113, "Year": 2022, "CourseName": "Fisika Dasar I", "CourseDescription": ""},
    {"CourseID": 1114, "Year": 2022, "CourseName": "Kimia Dasar I", "CourseDescription": ""},
    {"CourseID": 1115, "Year": 2022, "CourseName": "Pancasila", "CourseDescription": ""},
    {"CourseID": 2002, "Year": 2023, "CourseName": "Literasi Data dan Intelegensi Artifisial", "CourseDescription": ""},
    {"CourseID": 1220, "Year": 2023, "CourseName": "Matematika Diskrit", "CourseDescription": ""},
    {"CourseID": 1221, "Year": 2023, "CourseName": "Logika Komputasional", "CourseDescription": ""},
    {"CourseID": 1230, "Year": 2023, "CourseName": "Organisasi dan Arsitektur Komputer", "CourseDescription": ""},
    {"CourseID": 2001, "Year": 2023, "CourseName": "Pengenalan Rekayasa dan Desain", "CourseDescription": ""},
    {"CourseID": 2005, "Year": 2023, "CourseName": "Bahasa Indonesia", "CourseDescription": ""},
    {"CourseID": 1210, "Year": 2023, "CourseName": "Algoritma dan Pemrograman 1", "CourseDescription": ""},
    {"CourseID": 2110, "Year": 2023, "CourseName": "Algoritma dan Pemrograman 2", "CourseDescription": ""},
    {"CourseID": 2120, "Year": 2023, "CourseName": "Probabilitas dan Statistika", "CourseDescription": ""},
    {"CourseID": 2123, "Year": 2023, "CourseName": "Aljabar Linier dan Geometri", "CourseDescription": ""},
    {"CourseID": 2130, "Year": 2023, "CourseName": "Sistem Operasi", "CourseDescription": ""},
    {"CourseID": 2150, "Year": 2023, "CourseName": "Rekayasa Perangkat Lunak", "CourseDescription": ""},
    {"CourseID": 2003, "Year": 2023, "CourseName": "Olah Raga", "CourseDescription": ""},
    {"CourseID": 2180, "Year": 2023, "CourseName": "Sosio-informatika dan Profesionalisme", "CourseDescription": ""},
    {"CourseID": 2010, "Year": 2024, "CourseName": "Pemrograman Berorientasi Objek", "CourseDescription": ""},
    {"CourseID": 2211, "Year": 2024, "CourseName": "Strategi Algoritma", "CourseDescription": ""},
    {"CourseID": 2224, "Year": 2024, "CourseName": "Teori Bahasa Formal dan Otomata", "CourseDescription": ""},
    {"CourseID": 2230, "Year": 2024, "CourseName": "Jaringan Komputer", "CourseDescription": ""},
    {"CourseID": 2240, "Year": 2024, "CourseName": "Basis Data", "CourseDescription": ""},
    {"CourseID": 2022, "Year": 2024, "CourseName": "Manajemen Proyek", "CourseDescription": ""},
    {"CourseID": 3110, "Year": 2024, "CourseName": "Pengembangan Aplikasi Web", "CourseDescription": ""},
    {"CourseID": 3130, "Year": 2024, "CourseName": "Sistem Paralel dan Terdistribusi", "CourseDescription": ""},
    {"CourseID": 3140, "Year": 2024, "CourseName": "Sistem Basis Data", "CourseDescription": ""},
    {"CourseID": 3141, "Year": 2024, "CourseName": "Sistem Informasi", "CourseDescription": ""},
    {"CourseID": 3151, "Year": 2024, "CourseName": "Interaksi Manusia Komputer", "CourseDescription": ""},
    {"CourseID": 3170, "Year": 2024, "CourseName": "Inteligensi Artifisial", "CourseDescription": ""},
    {"CourseID": 3210, "Year": 2025, "CourseName": "Pengembangan Aplikasi Piranti Bergerak", "CourseDescription": ""},
    {"CourseID": 3250, "Year": 2025, "CourseName": "Proyek Perangkat Lunak", "CourseDescription": ""},
    {"CourseID": 3270, "Year": 2025, "CourseName": "Pembelajaran Mesin", "CourseDescription": ""},
    {"CourseID": 2004, "Year": 2025, "CourseName": "Bahasa Inggris", "CourseDescription": ""},
    {"CourseID": 3211, "Year": 2025, "CourseName": "Komputasi Domain Spesifik", "CourseDescription": ""},
    {"CourseID": 2011, "Year": 2025, "CourseName": "Agama dan Etika Islam", "CourseDescription": ""},
    {"CourseID": 2012, "Year": 2025, "CourseName": "Agama dan Etika Protestan", "CourseDescription": ""},
    {"CourseID": 2013, "Year": 2025, "CourseName": "Agama dan Etika Katolik", "CourseDescription": ""},
    {"CourseID": 2014, "Year": 2025, "CourseName": "Agama dan Etika Hindu", "CourseDescription": ""},
    {"CourseID": 2015, "Year": 2025, "CourseName": "Agama dan Etika Budha", "CourseDescription": ""},
    {"CourseID": 2016, "Year": 2025, "CourseName": "Agama dan Etika Khonghucu", "CourseDescription": ""},
    {"CourseID": 2017, "Year": 2025, "CourseName": "Kepercayaan terhadap Tuhan yang Maha Esa", "CourseDescription": ""},
    {"CourseID": 2006, "Year": 2025, "CourseName": "Kewarganegaraan", "CourseDescription": ""},
    {"CourseID": 4090, "Year": 2025, "CourseName": "Kerja Praktik", "CourseDescription": ""},
    {"CourseID": 4091, "Year": 2025, "CourseName": "Penyusunan Proposal", "CourseDescription": ""},
    {"CourseID": 4092, "Year": 2026, "CourseName": "Tugas Akhir", "CourseDescription": ""},
]

student_ids = [student["StudentID"] for student in rows_students]

rows_attends = [
    {"StudentID": 13522122, "CourseID": 3140},
    {"StudentID": 13522126, "CourseID": 3140},
    {"StudentID": 13522130, "CourseID": 3140},
    {"StudentID": 13522158, "CourseID": 3140},
    {"StudentID": 13522134, "CourseID": 3140},
    {"StudentID": 13522145, "CourseID": 3140},
    {"StudentID": 13522155, "CourseID": 3140},
    {"StudentID": 13522136, "CourseID": 3140},
    {"StudentID": 13522149, "CourseID": 3140},
    {"StudentID": 13522138, "CourseID": 3140},
    {"StudentID": 13522163, "CourseID": 3140},
    {"StudentID": 13522160, "CourseID": 3140},
    {"StudentID": 13522151, "CourseID": 3140},
    {"StudentID": 13522159, "CourseID": 3140},
    {"StudentID": 13522150, "CourseID": 3140},
    {"StudentID": 13522128, "CourseID": 3140},
    {"StudentID": 13522123, "CourseID": 3140},
    {"StudentID": 13522121, "CourseID": 3140},
    {"StudentID": 13522152, "CourseID": 3140},
    {"StudentID": 13522156, "CourseID": 3140},
    {"StudentID": 13522146, "CourseID": 3140},
    *[
        {"StudentID": random.choice(student_ids), "CourseID": random.choice([
            1102, 1103, 1111, 1116, 1112, 1113, 1114, 1115, 2002, 1220,
            1221, 1230, 2001, 2005, 1210, 2110, 2120, 2123, 2130, 2150,
            2003, 2180, 2010, 2211, 2224, 2230, 2240, 2022, 3110, 3130,
            3140, 3141, 3151, 3170, 3210, 3250, 3270, 2004, 3211, 2011,
            2012, 2013, 2014, 2015, 2016, 2017, 2006, 4090, 4091, 4092
        ])} for _ in range(49)
    ]
]

serializer = Serializer()

students_schema = serializer.serialize_schema(schema_student)
course_schema = serializer.serialize_schema(schema_course)
attends_schema = serializer.serialize_schema(schema_attends)

students_data = serializer.serialize_with_blocks(rows_students, schema_student)
course_data = serializer.serialize_with_blocks(rows_course, schema_course)
attends_data = serializer.serialize_with_blocks(rows_attends, schema_attends)

with open("Storage_Manager/data_demo/students_schema.dat", "wb") as file:
    file.write(students_schema)
    
with open("Storage_Manager/data_demo/courses_schema.dat", "wb") as file:
    file.write(course_schema)
    
with open("Storage_Manager/data_demo/attends_schema.dat", "wb") as file:
    file.write(attends_schema)
    
with open("Storage_Manager/data_demo/students.dat", "wb") as file:
    file.write(students_data)
    
with open("Storage_Manager/data_demo/courses.dat", "wb") as file:
    file.write(course_data)

with open("Storage_Manager/data_demo/attends.dat", "wb") as file:
    file.write(attends_data)