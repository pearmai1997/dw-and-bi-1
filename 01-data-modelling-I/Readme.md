## Data model
[Data model diagram](https://github.com/Fooklnwza007/dw-and-bi/blob/main/01-data-modelling-I/Data%20model%20diagram.png)
<br>
<br>

## Documentation

1. การสร้าง Code space เพื่อเขียน code และจัดเก็บ History สามารถเลือกได้ที่ หัวข้อ Code (1) หลังจากนั้นจะได้ชื่อของ Code space จะได้มาจากการสุ่ม
<img width="457" alt="image" src="https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/bcae9179-0fbd-401f-9d28-8618351978be">

2. จากนั้นเข้าหน้า Code space สร้าง Folder 01-data-modelling (3) และ data (4) เพื่อใช้ในการจัดเก็บไฟล์ที่เกี่ยวข้อง
<img width="415" alt="image" src="https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/562ddaf3-d4a8-4e4d-b485-a8a387d9deca">
<br>
<br>

### Implementation

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

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/f979084f-71f6-45e0-8abf-c1eebfded256)


**Step 05** ข้อมูลที่ใช้ในการเข้าถึง Database จะเป็นข้อมูลที่สร้างด้วย docker-compose.yml
- System: PostgreSQL
- Server: postgres
- Username: postgres
- Password: postgres
- Database: postgres

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/6840d98c-637d-4980-a7b9-50714371d3e3)


**Step 06** การสร้าง Table ใน Database จะต้องรันด้วย create_tables.py และเขียน code ของ PostgresSQL
```
$ python create_tables.py
```

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/4269566d-0d94-4074-abce-774553c0c6fb)

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/600bba61-bd66-48ab-879b-6b8acb19566a)



**Step 07** ในการเติมข้อมูลลงใน Table จะต้องทำการ coding ด้วย etl.py
```
$ python etl.py
```
![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/1b9d1bbe-b947-440b-9b91-9337df464d17)


