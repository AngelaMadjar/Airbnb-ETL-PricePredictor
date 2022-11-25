# **Airbnb ETL process & Price Predictor ML model**


![image](https://user-images.githubusercontent.com/74113692/203973174-b02f48a6-a7e6-412d-b8ce-dab5988d518a.png)


This repository contains the Extraction, Transformation and Loading process for data extracted from the public **Airbnb API**. 
In addition, it contains the code for exposing the ML model as a REST API and the Jupyter Notebook where the model is trained as well.

## **USAGE: **

### ETL Process
To run the etl_process/etl.py, replace the API key with yours and change the database credentials accordingly.
The Airbnb API is publicly available on https://rapidapi.com/DataCrawler/api/airbnb19. 
This ETL process utilizes the *Get Property By Place* method. 

*NOTE: The free plan enables 10 API invocations. Currently, the data is extracted for 12 cities (by envoking the API with 2 different keys and appending 
the results in the database. In case of running the code, remove 2 city ids for simplicity purposes.*


### Price Estimator
The Jupyter notebook contains detailed analysis of the extracted data throughout the ETL process.
It includes dealing with outliers, data skewness, multicollinearity, dimensionality reduction, additional preprocessing and visualizations.
Additionaly, it compares the performances of a collection of Linear Regression algorithms including: 
- Simple Linear Regression
- Ordinary Least Squares Regression
- Regularization with Ridge, Lasso and Bayesian Ridge Regressions
- Support Vector Regressor and Relevance Vector Regressor
- Random Forest Regressor
The best performing model is saved locally in a .pkl format.

*NOTE: The database containing extracted accommodation data is not available yet.*


### Exposing the ML model
The model is readily available in the model/airbnb_price_estimator.pkl and there is no need to train it all over again. 
The app.py can be started by running *run flask* in the terminal. It can be tested with Postman with the following url: http://127.0.0.1:5000/predict
Sample Json body:
*{
    "city": "Milan",
    "avg_rating": 4.7,
    "reviews_count": 187,
    "listing_obj_type": "REGULAR",
    "space_type": "Apartment",
    "room_type_category": "entire_home",
    "listing_guest_label": "2",
    "bathrooms": 1.0,
    "bedrooms": 2,
    "beds": 1,
    "check_in": "December",
    "is_super_host": true
}*
The response should be: "Estimated price": 68.82


# Simple Project Schema
![image](https://user-images.githubusercontent.com/74113692/203972987-0baad669-3d9b-460c-bc4a-2832662de6aa.png)









