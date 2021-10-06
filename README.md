# Welcome to Data Pipeline on GCP (Google Cloud Platform)
## Description
Data Pipeline ใน GCP นั้นเราใช้ Apache Airflow บน Composer และทำการเก็บข้อมูลไปยังData warehouse(Big Query) <br /> 
โดยที่Data Pipelineจะทำการดึงข้อมูลจากData Base(MySQL) มาทำความสะอาดข้อมูลและแปลงข้อมูลให้เหมาะสมกับการใช้งานและเก็บข้อมูลในData warehouse(Big Query)
## Technologies Used
pymysql,pandas,Google cloud Platform(Composer,Big Query)
## Step
1.เปิดใช้งานComposerบนGoogle Cloud Platform (ใช้เวลา15-20นาทีในการสร้าง) <br />
2.สร้างData setในBig Query(ใช้ที่อยู่เดียวกับComposer) <br />
3.พอComposerสร้างเสร็จให้ทำการเพิ่มPyPi Packagesโดยเพิ่มpymysql,pandas(ใช้เวลา5-10นาทีในการเพิ่มPyPi Packages) <br />
4.ตั้งค่าConnection ในAirflow เพื่อติดต่อกับData Base <br />
5.นำCode Python ไปไว้ในfolder DAGSที่Composerได้สร้างไว้ในGoogle Cloud Storage <br />
6.เสร็จสิ้นและตรวจสอบความสมบูรณ์
## Note
* เราใช้ข้อมูลจาก [www.kaggle.com/jpmiller/work-accidents-in-china](https://www.kaggle.com/jpmiller/work-accidents-in-china)
* ทำการแก้ไข[DATASET],[TABLE_NAME],[GCS_BUCKET]ก่อนCodeไปใช้
