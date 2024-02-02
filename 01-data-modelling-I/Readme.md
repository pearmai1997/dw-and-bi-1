**Step 01** เปลี่ยน path ให้อยู่ใน 01-data-molling-1
```
$ cd 01-data-modeling-i
```

**Step 02** ใช้ Docker ในการสร้าง Enviroment ตัวอย่างใช้ nginx port 8080
```
$ docker run -p 8080:80 nginx
```

**Step 03** เพื่อให้ Python สามารถเขียนด้วย Code ของ Postgres ต้องลง Libary ที่เกี่ยวข้อง
```
$ pip install psycopg2-binary
```

**Step 04** Run docker-compose.yml เพื่อสร้าง Database Postgres และ Adminer ด้วย docker
```
$ docker-compose up
```

**Step 05** ข้อมูลที่ใช้ในการเข้าถึง Database จะเป็นข้อมูลที่สร้างด้วย docker-compose.yml
- System: PostgreSQL
- Server: postgres
- Username: postgres
- Password: postgres
- Database: postgres

**Step 06** การสร้าง Table ใน Database จะต้องรันด้วย create_tables.py และเขียน code ของ PostgresSQL
```
$ python create_tables.py
```

**Step 07** ในการเติมข้อมูลลงใน Table จะต้องทำการ coding ด้วย etl.py
```
$ python etl.py
```
