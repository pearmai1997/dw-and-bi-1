# Instruction

1. ดึงไฟล์ docker-compose.yaml เพื่อนำไปสร้าง airflow ด้วย docker ได้

```sh
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.0/docker-compose.yaml'
```
2. สร้าง Folder dags, logs, plugins และ config เพื่อใช้ในการเก็บข้อมูลระหว่างการทำงานใน airflow
```sh
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/49fb388d-db77-4554-b99a-db9107d1331c)



3. สร้าง web server airflow ด้วย docker port 8080
```sh
docker compose up
```
![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/44198261-28fa-4f22-ad59-daa6a4f012df)

4. การ Load File, การสร้าง Tables และการ Process ทำด้วยไฟล์ etl.py โดยมี process การทำงาน

start >> [get_files, create_tables] >> process >> end

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/ded09067-720d-468d-82d1-ffabde2c19e2)

![image](https://github.com/Fooklnwza007/dw-and-bi/assets/131597296/8651764c-acf2-4c20-b78b-2752a85056ed)

5. ปิดการทำงาน docker
```sh
docker compose down
```
