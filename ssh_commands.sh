sudo apt-get update
sudo apt install python3-pip
sudo apt install sqlite3
sudo apt install python3.10-venv
python3 -m venv venv
source venv/bin/activate
sudo apt-get install libpq-dev
pip install "apache-airflow[postgres]==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.7.txt"
airflow db init
sudo apt-get install postgresql postgresql-contrib
sudo -i -u postgres
sudo pip install pandas 
sudo pip install s3fs
