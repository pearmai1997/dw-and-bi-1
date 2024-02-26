## Data model
![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/1b9ee674-53ad-440c-8658-0d389043fc8c)



<br>
<br>

## Documentation

1. เปลี่ยน Directory ให้อยู่ใน Folder 03-building-a-data-warehouse
```
$ cd 03-building-a-data-warehouse
```
2. สร้าง Environment Python เพื่อรองรับการการติดตั้ง Library ให้เหมาะสมกับงาน
```
$ python -m venv env 
```
![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/7f80f470-cd70-4206-b9b7-43f9fa07c69b)




3. Active Env. ที่สร้างไว้ให้สามารถใช้งานได้
```
$ source env/bin/activate
```
![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/ff31e8ac-0cce-42a7-94be-879a5d37e460)



4. Install Library ที่เกี่ยวข้องกับการใช้งาน
```
$ pip install -r requirements.txt
```
![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/465aaf0c-a82b-4518-abb1-4b53ccb113a3)


5. สร้าง Key ใน Google query เพื่อให้สามารถสร้าง Scipt python เพื่อ Google bigquery ได้

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/6d607f26-a01a-4e71-b52b-98a623751b18)

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/2ead132f-6167-48fe-b1ad-1c29e0ce4808)

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/733da1c7-2b81-4d27-b1c7-03efd015dcb8)


6. จากนั้นนำไฟล์ JSON แล้วเก็บไว้ในคอมฯของเรา

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/9c811f09-917f-496a-b9b8-0e4f502abc1a)

7. สร้าง Folder จัดเก็บไฟล์ JSON ใน code space หากอยู่ folder เดียวกับไฟล์ etl.py ไม่จำเป็นต้องมี ../credentials/ สามารถใช้ชื่อไฟล์ได้เลย
project_id มาจาก project name บน Bigquery หรือ สามารถดูได้จากไฟล์ Key ใน JSON ได้เช่นกัน

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/6273ec6c-2070-45ab-9b4d-692dd9617df4)

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/6c9f511d-90b4-4767-b534-7f77548fe28e)

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/a6c75f51-cf69-48bc-9296-ca0599e22d98)

8. การสร้าง tables และเพิ่มข้อมูลสามารถทำได้ในไฟล์ etl.py ข้อมูลที่เข้ามาจะขึ้นอยู่กับโครงสร้าง และข้อมูลที่กำหนดในไฟล์ etl.py
ในงานนี้จะให้ file_path = github_detail.csv และ ใช้ข้อมูล github_events_01.json
```
$ python etl.py
```
![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/6614add8-c340-41d5-9246-96d508e10c70)

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/541c96e7-0a5d-4c90-ab64-608429e37f4d)

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/6e44caf6-d69f-406e-bfde-a04b9459e684)

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/17e87162-7fe2-49e6-b79d-cdcefa16783b)
