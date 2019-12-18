import joblib
import pandas as pd

from flask import Flask, jsonify, request, make_response
from tensorflow import keras
from tensorflow.python.keras.backend import set_session
from tensorflow.compat.v1 import get_default_graph, Session

sess = Session()
set_session(sess)
graph = get_default_graph()

app = Flask(__name__)

model = keras.models.load_model("assets/model.h5")
transformer = joblib.load("assets/data_transformer.joblib")

@app.route("/v1/predict_fraud", methods=["POST"])
def index():
    global sess
    global graph

    data = request.get_json()

    tx = pd.DataFrame(data, index=[0])
    tx = tx.fillna(value=pd.np.nan)
    tx = transformer.transform(tx)

    with graph.as_default():
        set_session(sess)
        prediction = model.predict(tx).flatten()[0]

    response_body = jsonify({"score": str(prediction)})
    return make_response(response_body, 200)
