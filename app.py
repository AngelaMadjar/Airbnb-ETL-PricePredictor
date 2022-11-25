import numpy as np
from flask import Flask, request
import pickle

# Initializing a Flask app
app = Flask(__name__)

# Loading the saved model
model = pickle.load(open('model/airbnb_price_estimator.pkl', 'rb'))

# Encoding the categorical variables
# NOTE: Should be improved in the future
cities = {
    'Rome': 1,
    'Florence': 2,
    'Milan': 3,
    'Venice': 4,
    'Genoa': 5,
    'L\'Aquila': 6,
    'Bari': 7,
    'Ostuni': 8,
    'Matera': 9,
    'Alberobello': 10,
    'Brindisi': 11,
    'Lecce': 12,
    'Ancona': 13,
    'Catania': 14,
    'Bologna': 15,
    'Amalfi': 16
}

listing_obj_types = {
    'REGULAR': 1,
    'REPRESENTATIVE': 2
}

space_types = {
    'Apartment': 1,
    'Loft': 2,
    'Condo': 3,
    'Private room': 4,
    'Tiny home': 5,
    'Vacation home': 6,
    'Shared room': 7,
    'Hotel room': 8,
    'Cabin': 9,
    'Trullo': 10,
    'Home': 11,
    'Dome': 12,
    'Guest suite': 13,
    'Farm stay': 14,
    'Villa': 15,
    'Townhouse': 16
}

room_type_categories = {
    'entire_home': 1,
    'private_room': 2,
    'shared_room': 3,
    'hotel_room': 4
}

listing_guest_labels = {
    '1': 1,
    '2': 2,
    '3': 3,
    '4': 4,
    '5': 5,
    '6': 6,
    '7': 7
}

bathroom_num = {
    0.5: 1,
    1.0: 2,
    1.5: 3,
    2.0: 4,
    2.5: 5,
    3.0: 6
}

is_super_hosts = {
    True: 1,
    False: 2
}

check_ins = {
    'January': 1,
    'February': 2,
    'March': 3,
    'April': 4,
    'May': 5,
    'July': 6,
    'August': 7,
    'September': 8,
    'December': 9
}


"""
    NOTE: Add Marshmallow schemas to validate the passed attribute types
    Currently: Validation is done on client side, when inserting values in the SpringBoot's form
"""


# exposing the api to http://localhost:5000/predict
@app.route('/predict', methods=['POST'])
def predict():
    # Getting the request from the SpringBoot app
    request_data = request.get_json()

    # Extracting the attributes in order to encode them prior to passing them to the ML pipeline
    data = {
        "city": cities[str(request_data["city"])],
        "avg_rating": float(request_data["avg_rating"]),
        "reviews_count": int(request_data["reviews_count"]),
        "listing_obj_type": listing_obj_types[str(request_data["listing_obj_type"])],
        "space_type": space_types[str(request_data["space_type"])],
        "room_type_category": room_type_categories[str(request_data["room_type_category"])],
        "listing_guest_label": listing_guest_labels[str(request_data["listing_guest_label"])],
        "bathrooms": bathroom_num[float(request_data["bathrooms"])],
        "bedrooms": int(request_data["bedrooms"]),
        "beds": int(request_data["beds"]),
        "check_in": check_ins[str(request_data["check_in"])],
        "is_super_host": is_super_hosts[bool(request_data["is_super_host"])]
    }

    # Collecting the values in a list
    input_data = np.array(list(data.values()))

    # Using the trained model pipeline (MinMaxScaler, PCA, RVR) to predict the price
    prediction = model.predict([input_data])

    # Transforming the prediction accordingly because the model is trained over the square root of the prices
    output = {"Estimated price": round(pow(prediction[0], 2), 2)}

    return output


# Setting the port
if __name__ == '__main__':
    app.run(port=5000, debug=True)
