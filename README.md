# 🏠 housing-prices-prediction

Application de prediction de prix de biens immobiliers à partir d'un dataset d'entrainnement récupéré depuis une API.

## 📁 Structure du projet `housing-prices-prediction`

orga : 

```bash
├── airflow
│   ├── Dockerfile
│   ├── dag_housing_orchestrator.py
│   ├── dag_load_data_evidently.py
│   ├── dag_load_to_db_real.py
│   ├── dag_predict.py
│   ├── dag_train_real.py
│   ├── docker-compose.yaml
│   └── requirements.txt
├── app
│   ├── database.py
│   ├── main.py
│   ├── model_predict.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── utilisateur.py
│
├── app_real
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── data
│   ├── real_estate_dataset.csv 
├── evidently
│   └── evidently_dashboard.py
├── jenkins
│   ├── Jenkinsfile.evidently_dashboard
│   ├── Jenkinsfile.load_to_db
│   ├── Jenkinsfile.predict
│   ├── Jenkinsfile.train
│   └── Jenkinsfile.load_to_db
│    
├── mlflow_code
│   ├── Dockerfile
│   ├── MLProject
│   ├── train.py
│   ├── set_model_tag.py
│   └── requirements.txt
├── notebooks
│   └── housing_prices_eda.ipynb
├── src
│   └── load_to_db.py
├── test
│   ├── Dockerfile.load_to_db
│   ├── Dockerfile.train
│   ├── Dockerfile.predict
│   ├── requirements.txt
│   ├── test_load_to_db.py
│   ├── test_train.py
│   └── test_evidently_dashboard.py
├── README.md
├── LDS_block_4_lead.pptx
└── requirements.txt

## 1) Chargement de la table housing_prices dans la bdd NeonDB

Cette table est initialisée avec un fichier csv qui est le dataset d'entrainnement, la colonne price est renseignée la colonne price_predict reste à null.
Cette insertion en BDD est faite à l'initialisation du projet par src/load_to_db.py.
Cette table est ensuite mise à jour avec des DAG Airflow.

En local : python load_to_db.py

## 2) MLFlow sur EC2

http://x.x.x.x:5000/



### suppression de la base MLFlow
```bash
rm mlflow.db
```

### Build de l'image Docker de la partie MLFlow en local pour executer le train.py qui utilise le endpoint train de fastapi
```bash
docker build -t housing-prices-estimator ./mlflow_code 
```

### Run du conteneur de la partie MLFlow
```bash
docker run --rm --env-file .env housing-prices-estimator
```

## 3) Jenkins sur EC2 

http://x.x.x.x:8080/

🛠 Modifier l’URL de Jenkins après redémarrage EC2
	
	🧩 aller dans Jenkins :
	Menu principal > Manage Jenkins > System Configuration
	Modifier le champ “Jenkins URL” :
  Mettre http://<nouvelle-ip>:8080
	Menu principal > Manage Jenkins > Credentials
  Re-uploader le .env dans lequel on a mis à jour l'ip de MLFlow 

Et dans system -> global properties-> variables d'environnement : http://x.x.x.x:4000/
Re-uploader le .env


```bash
./start_jenkins.sh
./stop_jenkins.sh
```

### Docker en local : 
```bash
docker build -f test/Dockerfile.load_to_db -t test-load .
docker run --rm --env-file .env test-load
```
```bash
docker build -f test/Dockerfile.train -t test-train .
docker run --rm test-train
```
```bash
docker build --no-cache -t housing-prices-prediction -f test/Dockerfile.predict .
docker run --rm housing-api-tests
```

A noter dans le Jenkinsfile.predict on utilise le Dockerfile.predictEC2 car le .env est injecté à la différence en local on utilise le Dockerfile.predict car on prend le .env à la racine du projet
```bash
curl -X POST http://51.44.177.253:4000/predict \
-H "Content-Type: application/json" \
-d '{"area": 60, "property_type": "apartment", "rooms_number": 3, "zip_code": 75015, "land_area": 0, "garden": false, "garden_area": 0, "equipped_kitchen": true, "full_address": "Rue de Vaugirard, Paris", "building_state": "good"}'

docker build -f test/Dockerfile.evidently_dashboard -t evidently-dashboard-test .
docker run --rm --env-file .env evidently-dashboard-test pytest  
```
Nettoyage du disque : 

```bash
docker container prune
docker image prune -a
docker volume prune
```
puis

```bash
docker build -t myjenkins-blueocean:2.504.2-1 -f Dockerfile .
```

## 4) Airflow sur EC2 

Penser à changer dans airflow la connexion à jenkins pour avoir la nouvelle ip.

nano pour créer un fichier sur le serveur : 
nano nom du fichier
ctrl O
entrée
ctrl X

## 5) FastAPI et Streamlit dockérisé en local et sur EC2

### FastAPI

### API de prédiction

projet : Un rôle IAM a été créé pour les key aws!
pour le moment il y a un .env sur la EC2, qui ne contient rien de sensible, donc si changement d'ip il faut le modifier : 

POSTGRES_DATABASE=postgresql://neondb_owner:npg_x1bn2gueoPdE@ep-bold-firefly-a2mucnl4-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require
MLFLOW_TRACKING_URI=http://xxxxxx:5000/
API_HOST=http://xxxxxx:4000

Pour executer fastAPI : 

```bash
git pull origin development
```
Modifier le .env sur le serveur (MLflow serveur)


A la racine du projet :

```bash 
#source venv/bin/activate
docker build --no-cache -f app/Dockerfile -t housing-api .
docker run -it --rm -p 4000:4000 housing-api
```

http://x.x.x.x:4000/docs

Arrêt FastAPI
```bash
ps aux | grep uvicorn
lsof -i :4000

sudo kill -9 pid du root
```

### API real time 

API real time

```bash
docker build -t housing-real-api -f app_real/Dockerfile app_real
docker rm -f housing-real-api
docker run \
  --env-file .env \
  -p 8003:8003 \
  --name housing-real-api \
  housing-real-api
```

http://x.x.x.x:8003/docs

### Streamlit

http://x.x.x.x:8501

```bash
docker build -f app/Dockerfile.streamlit -t est-immo-app ./app
docker run -p 8501:8501 --env-file .env est-immo-app
```

### Git

Rappel des commandes : 

```bash
git branch
git checkout development
git status
git add .
git commit -m "xxx"
git push
```