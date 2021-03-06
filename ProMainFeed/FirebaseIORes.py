import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
cred = credentials.Certificate('key.json')
try:    
    firebase_admin.initialize_app(cred)
except:
    pass
db = firestore.client()

class FireBase:

    user_data = {}

    def __init__(self):

        # self.db = firestore.client()
        self.user_data["Working"] = 'NA'
        self.user_data["Hiring"] = 'NA'
        self.user_data["Investment"] = 'NA'
        self.user_data["Other"] = 'NA'
        self.user_data["Id"] = 'NA'
        # self.user_data["SexPreference"] = 'NA'
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


    def XGet(self, id):
        self.user_ref = db.collection(u'ProUsers').document(id)
        self.doc = self.user_ref.get()
        # for doc in docs:
        # user_data["Hiring"] = u'{}'.format(doc.to_dict()['hiring_goals'])
        
        self.user_data["Working"] = u'{}'.format(self.doc.to_dict()['work_goals'])
        self.user_data["Hiring"] = u'{}'.format(self.doc.to_dict()['hiring_goals'])
        self.user_data["Investment"] = u'{}'.format(self.doc.to_dict()['investment_goals'])
        self.user_data["Other"] = u'{}'.format(self.doc.to_dict()['other_goals'])
        # self.user_data["PrefDrinking"] = u'{}'.format(self.doc.to_dict()['pdrinking'])
        # self.user_data["PrefSmoking"] = u'{}'.format(self.doc.to_dict()['psmoking'])
        # self.user_data["SexPreference"] = u'{}'.format(self.doc.to_dict()['wish_to_meet'])
        # self.user_data["AgeRangeS"] = u'{}'.format(self.doc.to_dict()['preff age']['start'])
        # self.user_data["AgeRangeE"] = u'{}'.format(self.doc.to_dict()['preff age']['end'])

        # unfit = db.collection('DummyAIML').where(u'Smoking', u'==', self.user_data["Smoking"]).where(u'Drinking', u'==', self.user_data["Drinking"])
        # Case 0 : Find all Jobs and mentors
        # Working filled // Hiring and Investment empty
        # Get : Hiring // Working and Investment empty
        if self.user_data["Working"] != '[]' and self.user_data["Hiring"] == '[]' and self.user_data["Investment"] == '[]':

            # all_users = db.collection('ProUsers').where(u'work_goals', u'==', []).where(u'investment_goals', u'==', []).where(u'hiring_goals', u'!=', []).limit(100)
            
            all_users = db.collection('ProUsers').where(u'hiring_goals', u'!=', '[]').limit(100)
            all_users = db.collection('ProUsers').where(u'work_goals', u'==', '[]').limit(100)
            all_users = db.collection('ProUsers').where(u'investment_goals', u'==', '[]').limit(100)
            all_user = [doc.to_dict() for doc in all_users.stream()]
            print('we r past first case')
            all_id = [doc.id for doc in all_users.stream()]
            df = pd.DataFrame.from_dict(all_user)
            df['id'] = all_id
            return df   
        
        # Case 1 : Find all Jobs and investment options
        # Investment filled // Working and Hiring empty
        # Get : Investment // Working and Hiring empty
        elif self.user_data["Working"] == '[]' and self.user_data["Hiring"] == '[]' and self.user_data["Investment"] != '[]':
        
            # all_users = db.collection('ProUsers').where(u'work_goals', u'==', '[]').where(u'investment_goals', u'!=', '[]').where(u'hiring_goals', u'==', '[]').limit(100)

            all_users = db.collection('ProUsers').where(u'work_goals', u'==', '[]').limit(100)
            all_users = db.collection('ProUsers').where(u'investment_goals', u'!=', '[]').limit(100)
            all_users = db.collection('ProUsers').where(u'hiring_goals', u'==', '[]').limit(100)
            print('Investment case block')
            all_user = [doc.to_dict() for doc in all_users.stream()]
            all_id = [doc.id for doc in all_users.stream()]
            df = pd.DataFrame.from_dict(all_user)
            df['id'] = all_id
            return df  

        # Case 2 : Business owners Find all Hirings and business options
        # Hiring filled // Working and Investment empty
        # Get : Working // Hiring and Investment empty
        elif self.user_data["Working"] == '[]' and self.user_data["Hiring"] != '[]' and self.user_data["Investment"] == '[]':
        
            all_users = db.collection('ProUsers').where(u'work_goals', u'!=', '[]').where(u'investment_goals', u'==', '[]').where(u'hiring_goals', u'==', '[]').limit(5)

            # all_users = db.collection('ProUsers').where(u'work_goals', u'!=', '[]').limit(100)
            # all_users = db.collection('ProUsers').where(u'investment_goals', u'==', '[]').limit(100)
            # all_users = db.collection('ProUsers').where(u'hiring_goals', u'==', '[]').limit(100)
            print('Only Hiring case')
            all_user = [doc.to_dict() for doc in all_users.stream()]
            all_id = [doc.id for doc in all_users.stream()]
            df = pd.DataFrame.from_dict(all_user)
            df['id'] = all_id
            return df  
        # Case 3 : Possible HR
        # Working and Hiring filled // Investment empty
        # Get : Working and Hiring // Investment empty
        elif self.user_data["Working"] != '[]' and self.user_data["Hiring"] != '[]' and self.user_data["Investment"] == '[]':

            # all_users = db.collection('ProUsers').where(u'work_goals', u'!=', '[]').where(u'investment_goals', u'==', '[]').where(u'hiring_goals', u'!=', '[]').limit(100)

            all_users = db.collection('ProUsers').whertte(u'work_goals', u'!=', '[]').limit(100)
            all_users = db.collection('ProUsers').where(u'investment_goals', u'==', '[]').limit(100)
            all_users = db.collection('ProUsers').where(u'hiring_goals', u'!=', '[]').limit(100)
            print('multi not equalTo')
            all_user = [doc.to_dict() for doc in all_users.stream()]
            all_id = [doc.id for doc in all_users.stream()]
            df = pd.DataFrame.from_dict(all_user)
            df['id'] = all_id
            return df
        # Case 3 : Possible HR
        # Working and Investment filled // Hiring empty
        # Get : Hiring and Investment// Working  empty
        elif self.user_data["Working"] != '[]' and self.user_data["Hiring"] == '[]' and self.user_data["Investment"] != '[]':

            # all_users = db.collection('ProUsers').where(u'work_goals', u'==', '[]').where(u'investment_goals', u'!=', '[]').where(u'hiring_goals', u'!=', '[]').limit(100)

            all_users = db.collection('ProUsers').where(u'work_goals', u'==', '[]').limit(100)
            all_users = db.collection('ProUsers').where(u'investment_goals', u'!=', '[]').limit(100)
            all_users = db.collection('ProUsers').where(u'hiring_goals', u'!=', '[]').limit(100)

            all_user = [doc.to_dict() for doc in all_users.stream()]
            all_id = [doc.id for doc in all_users.stream()]
            df = pd.DataFrame.from_dict(all_user)
            df['id'] = all_id
            return df
 
        # Case 4 : Possible HR
        # Hiring and Investment filled // Working empty
        # Get : Working and Investment // Hiring empty
        elif self.user_data["Working"] == '[]' and self.user_data["Hiring"] != '[]' and self.user_data["Investment"] != '[]':

            # all_users = db.collection('ProUsers').where(u'work_goals', u'!=', '[]').where(u'investment_goals', u'!=', '[]').where(u'hiring_goals', u'==', '[]').limit(100)

            all_users = db.collection('ProUsers').where(u'work_goals', u'!=', '[]').limit(100)
            all_users = db.collection('ProUsers').where(u'investment_goals', u'!=', '[]').limit(100)
            all_users = db.collection('ProUsers').where(u'hiring_goals', u'==', '[]').limit(100)

            all_user = [doc.to_dict() for doc in all_users.stream()]
            all_id = [doc.id for doc in all_users.stream()]
            df = pd.DataFrame.from_dict(all_user)
            df['id'] = all_id
            return df
 
        # Case 5 : All Possible 
        # Working and Investment and Hiring filled // empty
        # Get : Hiring // Working and Investment empty
        elif self.user_data["Working"] != '[]' and self.user_data["Hiring"] != '[]' and self.user_data["Investment"] != '[]':

            # all_users = db.collection('ProUsers').where(u'work_goals', u'!=', '[]').where(u'investment_goals', u'!=', '[]').where(u'hiring_goals', u'!=', '[]').limit(100)

            all_users = db.collection('ProUsers').where(u'work_goals', u'!=', '[]').limit(100)
            all_users = db.collection('ProUsers').where(u'investment_goals', u'!=', '[]').limit(100)
            all_users = db.collection('ProUsers').where(u'hiring_goals', u'!=', '[]').limit(100)

            all_user = [doc.to_dict() for doc in all_users.stream()]
            all_id = [doc.id for doc in all_users.stream()]
            df = pd.DataFrame.from_dict(all_user)
            df['id'] = all_id
            return df

        else:

            # all_users = db.collection('ProUsers').where(u'work_goals', u'==', '[]').where(u'investment_goals', u'==', '[]').where(u'hiring_goals', u'!=', '[]')
            return('exit else case, what just happened')

        # all_user = [doc.to_dict() for doc in all_users.stream()]
        # all_id = [doc.id for doc in all_users.stream()]
        # # all_id = []
        # # for docx in all_users.stream():
        # #     all_id = f'{docx.id} => {docx.to_dict()}' 

        # df = pd.DataFrame.from_dict(all_user)
        # # dfid = pd.DataFrame.from_dict(all_id)
        # df['id'] = all_id

        # return df
    

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

        unfit = db.collection('DummyAIML').where(u'Smoking', u'==', self.user_data["Smoking"]).where(u'Drinking', u'==', self.user_data["Drinking"]).limit(5)

        # x = f'{self.docs.to_dict()}'
        # x = [doc.id for doc in unfit.stream()]
        x = [doc.to_dict() for doc in unfit.stream()]
        # x = pd.DataFrame.from_dict(x)
        return x

    def SendD(self, cluster, id):
        self.users_refx = db.collection(u'root').document(
            id).collection(u'OnBdCluster')
        self.users_refx.set({u'ClusterNumber': cluster}, merge=True)
        return 'ClusterID uploaded'

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
        db.collection(u'ProUsers').document(id).collection(u'MainFeed').document(i['id']).set(i)
    return 'MFList uploaded'

def DyUpdate(x, id):
    new_user = db.collection(u'DummyAIML').document(id).get()
    new_user_id = u'{}'.format(new_user.id)
    new_user= f'{new_user.to_dict()}'
    # print(type(x))
    # print(new_user)
    # for i in x:
    #     print(i)
    db.collection(u'DummyAIML').document(x).collection(u'MainFeed').document(new_user_id).set(new_user)
    