# from FirebaseIO import SendD
import os
from flask import Flask, request, jsonify
# from firebase_admin import credentials, firestore, initialize_app
# from google.cloud import storage
from ModelPred import PredCluster
from FirebaseIO import FireBase, SendDes
import pandas as pd

app = Flask(__name__)


@app.route('/add', methods=['GET','POST'])
def MFlist():
    # try:

#Identify Anchor (Main) User 
    id = request.args.get('id')
#Get list of filtered users/ in DataFrame format
    y = FireBase()
    df = y.UGet(id)
    dfcp = df[['Personality Traits','Interests']]
    # df = pd.DataFrame.from_dict(all_users)
    # df = df.drop('Name')
    # df.set_index("Name", inplace=True)
    out = PredCluster(dfcp)
    outdf = df.loc[out.index]
    outdf = outdf.to_json(orient ='records')
    outdf = eval(outdf)
    SendDes(outdf, id)
       
    return str(outdf), 200
    # except Exception as e:
    #     return f"An Error Occured: {e}"  



port = int(os.environ.get('PORT', 8080))
if __name__ == '__main__':
    app.run(debug=True, threaded=True, host='0.0.0.0', port=port)