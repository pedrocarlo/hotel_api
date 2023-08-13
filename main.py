import os
from fastapi import FastAPI, BackgroundTasks
import uvicorn
from db.sql import insert_xml_from_folder


app = FastAPI()
cwd = os.getcwd()


# def move_files(folder):
#     files = os.listdir(folder)
#     for filename in files:
#         filepath = os.path.join(folder, filename)
#         completa_folder = cwd + "/" + "xml/completa"
#         resumida_folder = cwd + "/" + "xml/resumida"

#         nota = get_tags(os.path.join(folder, filename))
#         if nota.completa:
#             shutil.copyfile(filepath, os.path.join(completa_folder, filename))
#         else:
#             shutil.copyfile(filepath, os.path.join(resumida_folder, filename))

# @app.get("/", status_code=202, tags=["info"])
# async def root(background_tasks: BackgroundTasks):
#     background_tasks.add_task(
#         insert_xml_from_folder,
#         cwd + "/" + "xml/completa",
#     )
#     background_tasks.add_task(
#         insert_xml_from_folder,
#         cwd + "/" + "xml/resumida",
#     )
#     return {"message": "Adding files to database"}


@app.get("/", status_code=202, tags=["info"])
async def root():
    return {"message": "Adding files to database"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5000, log_level="info")
