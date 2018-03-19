import numpy
import requests
import json
import os
from . import mdaio

class MLStudyScript:
    def __init__(self):
        self._object={}
        self._results_object={}
    def setObject(self,obj):
        self._object=obj
    def setResultsObject(self,obj):
        self._results_object=obj
    def result(self,name):
        return self._results_object[name]['value']

class MLStudy:
    _docstor_url='https://docstor1.herokuapp.com'
    def __init__(self,id='',path=''):
        self._study={}
        self.load(id=id,path=path)
    def setDocStorUrl(self,url):
        self._docstor_url=url
    def load(self,id='',path=''):
        if id:
            req=requests.post(self._docstor_url+'/api/getDocument',json={"id":id,"include_content":True})
            if (req.status_code != 200):
                raise ValueError('Unable to load study with id: '+id)
            self._study=json.loads(req.json()['content'])
            return True
        else:
            if not path:
                path='/working/_private/data/workspace.mlw'
            self._study=json.load(open(path))
            return True
    def description(self):
        return self._study['description']
    def scriptNames(self):
        return self._study['scripts'].keys()
    def script(self,script_name):
        X=MLStudyScript()
        X.setObject(self._study['scripts'][script_name])
        X.setResultsObject(self._study['results_by_script'][script_name])
        return X;
    def datasetIds(self):
        return self._study['datasets'].keys()

def loadFile(obj):
    if 'prv' in obj:
        checksum=obj['prv']['original_checksum']
        return _load_file_from_checksum(checksum)
    else:
        return None

def _load_file_from_checksum(checksum):
    url='https://kbucket.flatironinstitute.org/download/'+checksum
    if ('KBUCKET_DOWNLOAD_DIRECTORY' in os.environ) and (os.path.isdir(os.environ['KBUCKET_DOWNLOAD_DIRECTORY'])):
        kbucket_download_directory=os.environ['KBUCKET_DOWNLOAD_DIRECTORY']
        file_path=os.path.join(kbucket_download_directory,checksum)
        if not os.path.isfile(file_path):
            _download_to_file(url,file_path)
        with open(file_path, "rb") as binary_file:
            return binary_file.read()
    else:
        req=requests.get(url)
        return req.content

def _download_to_file(url,file_path):
    import urllib.request
    tmp_file_name=file_name+'.downloading'
    urllib.request.urlretrieve(url, tmp_file_name)
    os.rename(tmp_file_name,file_name)
    

def loadTextFile(obj):
    return loadFile(obj).decode('utf-8')

def loadJsonFile(obj):
    return json.loads(loadTextFile(obj))

def loadMdaFile(obj):
    return mdaio.mda_from_bytes(loadFile(obj))
