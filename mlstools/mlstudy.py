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
    def loadResult(self,name):
        return loadFile(self.result(name))
    def loadTextResult(self,name):
        return loadTextFile(self.result(name))
    def loadJsonResult(self,name):
        return loadJsonFile(self.result(name))
    def loadMdaResult(self,name):
        return loadMdaFile(self.result(name))
    def getResultPath(self,name):
        return getFilePath(self.result(name))

class MLStudyDataset:
    def __init__(self):
        self._object={}
    def setObject(self,obj):
        self._object=obj
    def fileNames(self):
        return self._object['files'].keys()
    def file(self,file_name):
        return self._object['files'][file_name]
    def loadFile(self,name):
        return loadFile(self.file(name))
    def loadTextFile(self,name):
        return loadTextFile(self.file(name))
    def loadJsonFile(self,name):
        return loadJsonFile(self.file(name))
    def loadMdaFile(self,name):
        return loadMdaFile(self.file(name))
    def getFilePath(self,name):
        return getFilePath(self.file(name))

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
    def dataset(self,dataset_id):
        X=MLStudyDataset()
        X.setObject(self._study['datasets'][dataset_id])
        return X;

def loadFile(obj):
    if type(obj)==str:
        with open(obj, "rb") as binary_file:
            return binary_file.read()
    elif 'prv' in obj:
        checksum=obj['prv']['original_checksum']
        return _load_file_from_checksum(checksum)
    else:
        return None

def getFilePath(obj):
    if 'prv' in obj:
        checksum=obj['prv']['original_checksum']
        return _get_file_path_from_checksum(checksum)
    else:
        return None

def _get_kbucket_url_from_checksum(checksum):
    return 'https://kbucket.flatironinstitute.org/download/'+checksum

def _get_file_path_from_checksum(checksum):
    if ('KBUCKET_DOWNLOAD_DIRECTORY' in os.environ) and (os.path.isdir(os.environ['KBUCKET_DOWNLOAD_DIRECTORY'])):
        url=_get_kbucket_url_from_checksum(checksum)
        kbucket_download_directory=os.environ['KBUCKET_DOWNLOAD_DIRECTORY']
        file_path=os.path.join(kbucket_download_directory,checksum)
        if not os.path.isfile(file_path):
            _download_to_file(url,file_path)
        return file_path
    else:
        return None

def _load_file_from_checksum(checksum):
    if ('KBUCKET_DOWNLOAD_DIRECTORY' in os.environ) and (os.path.isdir(os.environ['KBUCKET_DOWNLOAD_DIRECTORY'])):
        file_path=_get_file_path_from_checksum(checksum)
        with open(file_path, "rb") as binary_file:
            return binary_file.read()
    else:
        url=_get_kbucket_url_from_checksum(checksum)
        req=requests.get(url)
        return req.content

def _download_to_file(url,file_path):
    import urllib.request
    tmp_file_name=file_path+'.downloading'
    urllib.request.urlretrieve(url, tmp_file_name)
    os.rename(tmp_file_name,file_path)

def loadTextFile(obj):
    return loadFile(obj).decode('utf-8')

def loadJsonFile(obj):
    return json.loads(loadTextFile(obj))

def loadMdaFile(obj):
    return mdaio.mda_from_bytes(loadFile(obj))
