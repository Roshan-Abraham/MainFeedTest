import pandas as pd
import numpy as np
import json
import firebase_admin
import requests
from firebase_admin import credentials
from firebase_admin import firestore
from datetime import date, datetime
cred = credentials.Certificate(r'X:\minglewise2019-firebase-adminsdk-5mhc2-9de8374b77.json')
try:    
    firebase_admin.initialize_app(cred)
except:
    pass
db = firestore.client()


class FireBase:

    user_data = {}

    def __init__(self):

        # self.db = firestore.client()
        self.user_data["Drinking"] = 'NA'
        self.user_data["Smoking"] = 'NA'
        self.user_data["Gender"] = 'NA'
        self.user_data["Name"] = 'NA'
        self.user_data["Id"] = 'NA'
        self.user_data["SexPreference"] = 'NA'
        self.user_data["Age"] = 'NA'
        self.user_data["PAgeMax"] = 'NA'
        self.user_data["PAgeMin"] = 'NA'

    def GetD(self):
        # db = firestore.client()
        self.users_ref = db.collection(u'root')
        self.docs = self.users_ref.stream()
        for doc in self.docs:
            self.user_data["Drinking"] = u'{}'.format(doc.to_dict()['Drinking'])
            self.user_data["Smoking"] = u'{}'.format(doc.to_dict()['Smoking'])
            self.user_data["Gender"] = u'{}'.format(doc.to_dict()['Gender'])
            self.user_data["Name"] = u'{}'.format(doc.to_dict()['Name'])
            # self.user_data["Id"] = u'{}'.format(doc.id)
        return(self.user_data)

    def UGet(self, id):
        self.user_ref = db.collection(u'DummyAIML').document(id)
        self.docs = self.user_ref.get()
        # for doc in docs:
        self.user_data["Drinking"] = u'{}'.format(self.docs.to_dict()['Drinking'])
        self.user_data["Smoking"] = u'{}'.format(self.docs.to_dict()['Smoking'])
        self.user_data["Gender"] = u'{}'.format(self.docs.to_dict()['Gender'])
        # self.user_data["Name"] = u'{}'.format(self.docs.to_dict()['n'])

        self.user_data["SexPreference"] = u'{}'.format(self.docs.to_dict()['Wish to Meet'])
        self.user_data["Age"] = u'{}'.format(self.docs.to_dict()['Age'])
        self.user_data["PAgeMax"] = u'{}'.format(self.docs.to_dict()['Preferred partner max age'])
        self.user_data["PAgeMin"] = u'{}'.format(self.docs.to_dict()['Preferred partner min age'])

        # unfit = db.collection('DummyAIML').where(u'Smoking', u'==', self.user_data["Smoking"]).where(u'Drinking', u'==', self.user_data["Drinking"])
        if self.user_data["SexPreference"] == 'both':
            print('Entering first if case')
            all_users = db.collection('DummyAIML').where(u'Smoking', u'==', self.user_data["Smoking"]).where(u'Drinking', u'==', self.user_data["Drinking"])
            print('exit if case')
        else:

            all_users = db.collection('DummyAIML').where(u'Gender', u'==', self.user_data["SexPreference"]).where(u'Smoking', u'==', self.user_data["Smoking"]).where(u'Drinking', u'==', self.user_data["Drinking"])
            print('exit else case')

        all_user = [doc.to_dict() for doc in all_users.stream()]
        all_id = [doc.id for doc in all_users.stream()]
        # all_id = []
        # for docx in all_users.stream():
        #     all_id = f'{docx.id} => {docx.to_dict()}' 

        df = pd.DataFrame.from_dict(all_user)
        # dfid = pd.DataFrame.from_dict(all_id)
        df['id'] = all_id

        return df
        
    def SGet(self, id):
        self.user_ref = db.collection(u'DummyAIML').document(id)
        self.docs = self.user_ref.get()
        # for doc in docs:
        self.user_data["Drinking"] = u'{}'.format(self.docs.to_dict()['Drinking'])
        self.user_data["Smoking"] = u'{}'.format(self.docs.to_dict()['Smoking'])
        self.user_data["Gender"] = u'{}'.format(self.docs.to_dict()['Gender'])
        # self.user_data["Name"] = u'{}'.format(self.docs.to_dict()['n'])

        self.user_data["SexPreference"] = u'{}'.format(self.docs.to_dict()['Wish to Meet'])
        self.user_data["Age"] = u'{}'.format(self.docs.to_dict()['Age'])
        self.user_data["PAgeMax"] = u'{}'.format(self.docs.to_dict()['Preferred partner max age'])
        self.user_data["PAgeMin"] = u'{}'.format(self.docs.to_dict()['Preferred partner min age'])

        unfit = db.collection('DummyAIML').where(u'Smoking', u'==', self.user_data["Smoking"]).where(u'Drinking', u'==', self.user_data["Drinking"])

        # x = f'{self.docs.to_dict()}'
        x = [doc.id for doc in unfit.stream()]
        # x = pd.DataFrame.from_dict(x)
        return x
    # @app.route('/list', methods=['POST'])
    # def list():
    #     try:
    #         gender = request.json['Gender']
    #         all_users = db.collection('root').where(
    #         'Gender', '!=', gender).stream()
    #         user_list = []
    #         #docs = db.collection(u'cities').where(u'capital', u'==', True).stream()
    #         # all_users = [doc.to_dict() for doc in all_users.stream()]
    #         for doc in all_users:
    #             print(f'{doc.id} => {doc.to_dict()}')
    #             d = doc.to_dict()
    #             user_list.append(users(doc.id, d['Name'], d['Gender'], d['Smoking'], d['Drinking']))
    #         return jsonpickle.encode(user_list, unpicklable=False), 200
    #     except Exception as e:
    #         return f"An Error Occured: {e}"

    def SendD(self, cluster, id):
        self.users_refx = db.collection(u'root').document(
            id).collection(u'OnBdCluster')
        self.users_refx.set({u'ClusterNumber': cluster}, merge=True)
        return 'ClusterID uploaded'


def age(d):
    birth_date = datetime.strptime(d, '%Y-%m-%d')
    today = date.today()
    y = today.year - birth_date.year
    if today.month < birth_date.month or today.month == birth_date.month and today.day < birth_date.day:
        y -= 1
    return y

def MCFunc():
    x = FireBase()
    see = x.GetD()
    # print(see)
    return see


def EAFunc(persona, id):
    x = FireBase()
    x.SendD(persona, id)

def DGet(id, smoke):
    FireBase.GetD()
    all_users = db.collection('DummyMLAIHardik').where(u'g', u'==', u'Male').where(u's', u'==', smoke)#.where(u'Drinking', u'==', drink)

    all_users = [doc.to_dict() for doc in all_users.stream()]

    df = pd.DataFrame.from_dict(all_users)

def SendDes(x,id):
    for i in x:
        db.collection(u'DummyAIML').document(id).collection(u'MainFeed').document(i['id']).set(i)
    return 'MFList uploaded'