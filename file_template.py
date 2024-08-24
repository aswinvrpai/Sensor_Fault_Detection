import os
from pathlib import Path

file_list = [
    "requirements.txt",
    "setup.py",
    "notebooks/EDA.ipynb",
    "src/utils.py",
    "src/logger.py",
    "src/__init__.py",
    "src/components/__init__.py",
    "src/components/data_ingestion.py",
    "src/components/data_transformation.py",
    "src/components/model_training.py",
    "src/pipelines/training_pipeline.py",
    "src/pipelines/prediction_pipeline.py",
    "static/css/style.css",
    "template/index.html",
    "template/page.html",
    ".env",
    ".gitignore",
    "app.py",
]

# Create the Folder structure;
for file_name in file_list:
    file_name=Path(file_name)
    filedir,filename = os.path.split(file_name)
    
    if filedir != "":
        os.makedirs(filedir,exist_ok=True)
        
        if (not os.path.exists(file_name)) or (os.path.getsize(file_name) == 0):
            with open(file_name,"w") as fOpen:
                pass
    else:
        
        # For Files in same hierarchy as this file;
        if (not os.path.exists(file_name)) or (os.path.getsize(file_name) == 0):
            with open(file_name,"w") as fOpen:
                pass