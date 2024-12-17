from serializer import *
import random

schema_student = {
    "table_name" : "student",
    "columns": [
        {"name": "studentid", "type": "int"},
        {"name": "fullname", "type": "varchar", "length": 50},
        {"name": "gpa", "type": "float"},
    ],
}

schema_course = {
    "table_name": "courses",
    "columns": [
        {"name": "courseid", "type": "int"},
        {"name": "year", "type": "int"},
        {"name": "coursename", "type": "varchar", "length": 50},
        {"name": "coursedescription", "type": "varchar", "length": 255},
    ],
}

schema_attends = {
    "table_name": "attends",
    "columns": [
        {"name": "studentid", "type": "int"},
        {"name": "courseid", "type": "int"},
    ],
}

rows_students = [
    {"studentid": 13522122, "fullname": "Maulvi Ziadinda Maulana", "gpa": 4.0},
    {"studentid": 13522126, "fullname": "Rizqika Mulia Pratama", "gpa": 4.0},
    {"studentid": 13522130, "fullname": "Justin Aditya Putra Prabakti", "gpa": 4.0},
    {"studentid": 13522158, "fullname": "Muhammad Rasheed Qais Tandjung", "gpa": 4.0},
    {"studentid": 13522134, "fullname": "Shabrina Maharani", "gpa": 4.0},
    {"studentid": 13522145, "fullname": "Farrel Natha Saskoro", "gpa": 4.0},
    {"studentid": 13522155, "fullname": "Axel Santadi Warih", "gpa": 4.0},
    {"studentid": 13522136, "fullname": "Muhammad Zaki", "gpa": 4.0},
    {"studentid": 13522149, "fullname": "Muhammad Dzaki Arta", "gpa": 4.0},
    {"studentid": 13522138, "fullname": "Andi Marihot Sitorus", "gpa": 4.0},
    {"studentid": 13522163, "fullname": "Atqiya Haydar Luqman", "gpa": 4.0},
    {"studentid": 13522160, "fullname": "Rayhan Ridhar Rahman", "gpa": 4.0},
    {"studentid": 13522151, "fullname": "Samy Muhammad Haikal", "gpa": 4.0},
    {"studentid": 13522159, "fullname": "Rafif Ardhinto Ichwantoro", "gpa": 4.0},
    {"studentid": 13522150, "fullname": "Albert Ghazaly", "gpa": 4.0},
    {"studentid": 13522128, "fullname": "Mohammad Andhika Fadillah", "gpa": 4.0},
    {"studentid": 13522123, "fullname": "Jimly Nur Arif", "gpa": 4.0},
    {"studentid": 13522121, "fullname": "Jonathan Emmanuel Saragih", "gpa": 4.0},
    {"studentid": 13522152, "fullname": "Muhammad Roihan", "gpa": 4.0},
    {"studentid": 13522156, "fullname": "Jason Fernando", "gpa": 4.0},
    {"studentid": 13522146, "fullname": "Muhammad Zaidan Sa'Dun Robbani", "gpa": 4.0},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Cattleya Arsenia Pratama", "gpa": 2.8},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Daffa Ananda Wibisono", "gpa": 1.6},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Fahira Nadine Santoso", "gpa": 3.7},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Reynaldo Putra Aditya", "gpa": 3.9},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Aisyah Khansa Permadi", "gpa": 0.5},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Hafiz Ramadhan Yusuf", "gpa": 2.2},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Shakira Amara Putri", "gpa": 3.4},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Naufal Alif Kurniawan", "gpa": 1.3},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Azka Faizal Hakim", "gpa": 2.1},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Zahra Annisa Fadilah", "gpa": 0.0},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Rizky Adrian Wibowo", "gpa": 3.8},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Nadira Syifa Kamila", "gpa": 1.6},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Fahmi Ardiansyah", "gpa": 2.7},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Putri Cahaya Kartika", "gpa": 3.4},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Farhan Maulana Akbar", "gpa": 3.9},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Keisha Amalia Rahma", "gpa": 0.3},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Ahmad Rifqi Althaf", "gpa": 1.2},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Syahrul Anwar Permana", "gpa": 3.5},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Fitria Nabila Zahra", "gpa": 2.1},
    {"studentid": random.choice(range(13520001, 13520163)) if random.choice(range(0, 1)) < 0.5 else random.choice(range(18220001, 18220163)), "fullname": "Eka Nur Pratami", "gpa": 2.8},
    {"studentid": 13521007, "fullname": "Matthew Mahendra", "gpa": 2.01},
    {"studentid": 13521162, "fullname": "Antonio Natthan Krishna", "gpa": 2.01},
    {"studentid": 13521100, "fullname": "Alexander Jason", "gpa": 2.01},
    {"studentid": 13521116, "fullname": "Juan Christopher Santoso", "gpa": 2.01},
    {"studentid": 13521155, "fullname": "Kandida Edgina Gunawan", "gpa": 2.01},
    {"studentid": 13521127, "fullname": "Marcel Ryan Antony", "gpa": 2.01},
    {"studentid": 23534042, "fullname": "Fawwaz Anugrah Wiradhika Dharmasatya", "gpa": 2.01},
    {"studentid": 23524065, "fullname": "Christine Hutabarat", "gpa": 2.01},
    {"studentid": 23524046, "fullname": "Hana Fathiyah", "gpa": 2.01}
]

rows_course = [
    {"courseid": 1102, "year": 2022, "coursename": "Berpikir Komputasional", "coursedescription": ""},
    {"courseid": 1103, "year": 2022, "coursename": "Pengantar Prinsip Keberlanjutan", "coursedescription": ""},
    {"courseid": 1111, "year": 2022, "coursename": "Laboratorium Fisika Dasar", "coursedescription": ""},
    {"courseid": 1116, "year": 2022, "coursename": "Laboratorium Interaksi Komputer", "coursedescription": ""},
    {"courseid": 1112, "year": 2022, "coursename": "Matematika I", "coursedescription": ""},
    {"courseid": 1113, "year": 2022, "coursename": "Fisika Dasar I", "coursedescription": ""},
    {"courseid": 1114, "year": 2022, "coursename": "Kimia Dasar I", "coursedescription": ""},
    {"courseid": 1115, "year": 2022, "coursename": "Pancasila", "coursedescription": ""},
    {"courseid": 2002, "year": 2023, "coursename": "Literasi Data dan Intelegensi Artifisial", "coursedescription": ""},
    {"courseid": 1220, "year": 2023, "coursename": "Matematika Diskrit", "coursedescription": ""},
    {"courseid": 1221, "year": 2023, "coursename": "Logika Komputasional", "coursedescription": ""},
    {"courseid": 1230, "year": 2023, "coursename": "Organisasi dan Arsitektur Komputer", "coursedescription": ""},
    {"courseid": 2001, "year": 2023, "coursename": "Pengenalan Rekayasa dan Desain", "coursedescription": ""},
    {"courseid": 2005, "year": 2023, "coursename": "Bahasa Indonesia", "coursedescription": ""},
    {"courseid": 1210, "year": 2023, "coursename": "Algoritma dan Pemrograman 1", "coursedescription": ""},
    {"courseid": 2110, "year": 2023, "coursename": "Algoritma dan Pemrograman 2", "coursedescription": ""},
    {"courseid": 2120, "year": 2023, "coursename": "Probabilitas dan Statistika", "coursedescription": ""},
    {"courseid": 2123, "year": 2023, "coursename": "Aljabar Linier dan Geometri", "coursedescription": ""},
    {"courseid": 2130, "year": 2023, "coursename": "Sistem Operasi", "coursedescription": ""},
    {"courseid": 2150, "year": 2023, "coursename": "Rekayasa Perangkat Lunak", "coursedescription": ""},
    {"courseid": 2003, "year": 2023, "coursename": "Olah Raga", "coursedescription": ""},
    {"courseid": 2180, "year": 2023, "coursename": "Sosio-informatika dan Profesionalisme", "coursedescription": ""},
    {"courseid": 2010, "year": 2024, "coursename": "Pemrograman Berorientasi Objek", "coursedescription": ""},
    {"courseid": 2211, "year": 2024, "coursename": "Strategi Algoritma", "coursedescription": ""},
    {"courseid": 2224, "year": 2024, "coursename": "Teori Bahasa Formal dan Otomata", "coursedescription": ""},
    {"courseid": 2230, "year": 2024, "coursename": "Jaringan Komputer", "coursedescription": ""},
    {"courseid": 2240, "year": 2024, "coursename": "Basis Data", "coursedescription": ""},
    {"courseid": 2022, "year": 2024, "coursename": "Manajemen Proyek", "coursedescription": ""},
    {"courseid": 3110, "year": 2024, "coursename": "Pengembangan Aplikasi Web", "coursedescription": ""},
    {"courseid": 3130, "year": 2024, "coursename": "Sistem Paralel dan Terdistribusi", "coursedescription": ""},
    {"courseid": 3140, "year": 2024, "coursename": "Sistem Basis Data", "coursedescription": ""},
    {"courseid": 3141, "year": 2024, "coursename": "Sistem Informasi", "coursedescription": ""},
    {"courseid": 3151, "year": 2024, "coursename": "Interaksi Manusia Komputer", "coursedescription": ""},
    {"courseid": 3170, "year": 2024, "coursename": "Inteligensi Artifisial", "coursedescription": ""},
    {"courseid": 3210, "year": 2025, "coursename": "Pengembangan Aplikasi Piranti Bergerak", "coursedescription": ""},
    {"courseid": 3250, "year": 2025, "coursename": "Proyek Perangkat Lunak", "coursedescription": ""},
    {"courseid": 3270, "year": 2025, "coursename": "Pembelajaran Mesin", "coursedescription": ""},
    {"courseid": 2004, "year": 2025, "coursename": "Bahasa Inggris", "coursedescription": ""},
    {"courseid": 3211, "year": 2025, "coursename": "Komputasi Domain Spesifik", "coursedescription": ""},
    {"courseid": 2011, "year": 2025, "coursename": "Agama dan Etika Islam", "coursedescription": ""},
    {"courseid": 2012, "year": 2025, "coursename": "Agama dan Etika Protestan", "coursedescription": ""},
    {"courseid": 2013, "year": 2025, "coursename": "Agama dan Etika Katolik", "coursedescription": ""},
    {"courseid": 2014, "year": 2025, "coursename": "Agama dan Etika Hindu", "coursedescription": ""},
    {"courseid": 2015, "year": 2025, "coursename": "Agama dan Etika Budha", "coursedescription": ""},
    {"courseid": 2016, "year": 2025, "coursename": "Agama dan Etika Khonghucu", "coursedescription": ""},
    {"courseid": 2017, "year": 2025, "coursename": "Kepercayaan terhadap Tuhan yang Maha Esa", "coursedescription": ""},
    {"courseid": 2006, "year": 2025, "coursename": "Kewarganegaraan", "coursedescription": ""},
    {"courseid": 4090, "year": 2025, "coursename": "Kerja Praktik", "coursedescription": ""},
    {"courseid": 4091, "year": 2025, "coursename": "Penyusunan Proposal", "coursedescription": ""},
    {"courseid": 4092, "year": 2026, "coursename": "Tugas Akhir", "coursedescription": ""},
]

student_ids = [student["studentid"] for student in rows_students]

rows_attends = [
    {"studentid": 13522122, "courseid": 3140},
    {"studentid": 13522126, "courseid": 3140},
    {"studentid": 13522130, "courseid": 3140},
    {"studentid": 13522158, "courseid": 3140},
    {"studentid": 13522134, "courseid": 3140},
    {"studentid": 13522145, "courseid": 3140},
    {"studentid": 13522155, "courseid": 3140},
    {"studentid": 13522136, "courseid": 3140},
    {"studentid": 13522149, "courseid": 3140},
    {"studentid": 13522138, "courseid": 3140},
    {"studentid": 13522163, "courseid": 3140},
    {"studentid": 13522160, "courseid": 3140},
    {"studentid": 13522151, "courseid": 3140},
    {"studentid": 13522159, "courseid": 3140},
    {"studentid": 13522150, "courseid": 3140},
    {"studentid": 13522128, "courseid": 3140},
    {"studentid": 13522123, "courseid": 3140},
    {"studentid": 13522121, "courseid": 3140},
    {"studentid": 13522152, "courseid": 3140},
    {"studentid": 13522156, "courseid": 3140},
    {"studentid": 13522146, "courseid": 3140},
    *[
        {"studentid": random.choice(student_ids), "courseid": random.choice([
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

with open("Storage_Manager/data_demo_lowercase/students_schema.dat", "wb") as file:
    file.write(students_schema)
    
with open("Storage_Manager/data_demo_lowercase/courses_schema.dat", "wb") as file:
    file.write(course_schema)
    
with open("Storage_Manager/data_demo_lowercase/attends_schema.dat", "wb") as file:
    file.write(attends_schema)
    
with open("Storage_Manager/data_demo_lowercase/students.dat", "wb") as file:
    file.write(students_data)
    
with open("Storage_Manager/data_demo_lowercase/courses.dat", "wb") as file:
    file.write(course_data)

with open("Storage_Manager/data_demo_lowercase/attends.dat", "wb") as file:
    file.write(attends_data)