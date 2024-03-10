# Data model

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/02318bc8-5e20-4f48-942c-61e634606c49)


# Instruction

1. เพื่อให้เราสามารถสร้างไฟล์ได้จาก Jupyter Lab ให้รันคำสั่งด้านล่างนี้

```sh
sudo chmod 777 .
```
2. เพื่อให้สามารถเข้า Jupyter Notebook ผ่าน Port 8888:8888 ได้
```sh
docker-compose up
```
![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/b63e4848-4488-41f9-976e-ce60486be0e1)



3. Lab ที่ใช้ชื่อ etl_local.ipynb

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/32482341-9cff-4714-a70b-326ffd73b853)


4. ข้อมูลที่ใช้จะอยู่ใน Folder data และมีไฟล์ JSON 2 ไฟล์ ซึ่งข้อมูลที่ใช้ในการ Test จะใช้เพียง data = "github_events_01.json"

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/b13dab73-7f2a-414e-837f-6f575bba3596)


5. จากนั้นใช้ฟังก์ชั่น data.createOrReplaceTempView("staging_events") เพื่อให้ Spark สามารถใช้ Coding ด้วย SQL ได้

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/41a8142c-8c36-48ed-aba2-74ae356cebcd)


6. จากนั้นเก็บข้อมูลให้เป็นตารางที่แบ่งข้อมูลเป็น Partition ด้วย SQL ให้สอดคล้องกับ Data model แล้วสร้าง Folder เพื่อใช้จัดเก็บไฟล์

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/475f2e3f-9f26-4852-a656-edc2cd92536b)

7. ข้อมูลจะถูกจัดเก็บใน Folder โดยแบ่งข้อมูลออกเป็นหลายๆส่วน ในตัวอย่างเป็นข้อมูล events ถูกแบ่งเป็นไฟล์ 2 ไฟล์

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/ece76d35-8f14-47d8-8765-c29e25865fd1)

