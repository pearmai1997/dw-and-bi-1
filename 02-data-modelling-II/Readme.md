## Data model
![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/7dd507c8-2ab3-482b-a4dd-95db365c4dde)

<br>
<br>

## Documentation

1. เปลี่ยน Directory ให้อยู่ใน Folder 02-data-modelling-II
```
$ cd 02-data-modelling-II
```
2. สร้าง Environment Python เพื่อรองรับการการติดตั้ง Library ให้เหมาะสมกับงาน
```
$ python -m venv env 
```
![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/f2d058d4-d433-41e7-a4ab-17f9ac2f4043)



3. Active Env. ที่สร้างไว้ให้สามารถใช้งานได้
```
$ source env/bin/activate
```
![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/d666f686-28d6-4fc1-9979-354bd9342aa2)


4. Install Library ที่เกี่ยวข้องกับการใช้งาน
```
$ pip install -r requirements.txt
```
![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/9ccee9c8-6ff0-40fb-bd4d-ed67ea7520c6)


5. เริ่มการใช้งาน Cassandra ด้วย Docker
```
$ docker compose up
```
6. หากต้องการสร้าง table, เพิ่มข้อมูล, Query ข้อมูล สามารถทำได้ใน etl.py
```
$ python ety.py
```
![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/f312c623-0004-41b7-9927-a7923f9cf4e9)

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/c848e211-d046-4ec1-94e8-dce067551cf2)

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/8e2ed138-dd80-48d7-bc19-188c74504884)

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/b721f3bf-70b5-4b19-97f2-03ed8fb1fd3c)

