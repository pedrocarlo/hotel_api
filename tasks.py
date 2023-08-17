import logging
from celery import Celery
import celery
from sefaz.distNfe import distNfe
from sefaz.xml_parser import get_tags
from db.model import Nfe
from lxml import etree
import os
from celery.utils.log import get_task_logger

logger = logging.getLogger("uvicorn")

app = Celery('tasks', backend=os.environ["BACKEND_URL"], broker=os.environ["BROKER_URL"])
cwd = os.getcwd()

app.conf.update(
    task_serializer='json',
    accept_content=['json'],  # Ignore other content
    result_serializer='json',
    timezone='America/Sao_Paulo',
    enable_utc=False,
)

@celery.signals.after_setup_logger.connect
def on_after_setup_logger(**kwargs):
    # c_handler = logging.StreamHandler()
    logger.setLevel(logging.INFO)
    # logger.addHandler(c_handler)
    # c_handler.setFormatter(logging.Formatter('%(name)s %(levelname)s - %(message)s'))
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # # add filehandler
    # fh = logging.FileHandler('logs.log')
    # fh.setLevel(logging.DEBUG)
    # fh.setFormatter(formatter)
    # logger.addHandler(fh)

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(1.0, test.s('HELLO WORLD'), name='Ler novas notas')
    

@app.task
def novas_notas():
    with open(os.path.join(cwd, "celery", 'ultNsu.txt'), "r") as f:
        nsu = int(f.readline().strip())
    ult_nsu = nsu
    max_nsu = float("inf")
    while ult_nsu < max_nsu:
        ult_nsu, max_nsu, xmls = distNfe('', ult_nsu, True, False)
        if ult_nsu != 0:
            write_ult_nsu(ult_nsu)
            for xml in xmls:
                nota = get_tags(xml_str=xml)
                path = write_xml(xml, nota)


def write_xml(xml: str, nota: Nfe):
    folder = nota.get_folder()
    path = os.path.join(cwd, "xml", folder, f"{nota.chave}.xml")
    resposta = etree.fromstring(xml)
    resposta.getroottree().write(path, pretty_print=True)
    return path
    

def write_ult_nsu(nsu: int):
    with open(os.path.join(cwd, "celery", 'ultNsu.txt'), "w") as f:
        f.write(str(nsu))

@app.task
def test(name):
    print('HELLO JNKJCNS')
    logger.info('HELLO JNKJCNS')
    return name