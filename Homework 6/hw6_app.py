import pymysql
import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# Function to convert an IP string to a numerical value
#Splitting the IP address into its four octets.
#Converting each octet from a string to an integer.
#Multiplying each octet by 256 raised to the power of its position in reverse order (because the first octet is the most significant byte).
#Summing up the results to get the final numerical representation of the IP address.
def ip_to_int(ip):
    octets = ip.split('.')
    return sum(int(octet) * 256 ** (3 - i) for i, octet in enumerate(octets))


# Function to connect to the SQL database
def get_data_from_db(query, connection_params):
    # Connect to the database
    conn = pymysql.connect(**connection_params)
    # Get data
    data = pd.read_sql_query(query, conn)
    conn.close()
    return data

# Functions to preprocess the data

def preprocess_data_for_country_prediction(data):
    # Convert IP to numerical features
    data['ip_numeric'] = data['client_ip'].apply(ip_to_int)
    return data

#convert age ranges into a numerical format to be used for modeling
#represent each range by its midpoint
def preprocess_data_for_income_prediction(data):
    # Dictionary to convert age ranges to midpoint values
    age_midpoints = {
        '0-16': 8, '17-25': 21, '26-35': 30.5, '36-45': 40.5,
        '46-55': 50.5, '56-65': 60.5, '66-75': 70.5, '76+': 80
    }
    
    # Convert age ranges to midpoints
    data['age_midpoint'] = data['age'].map(age_midpoints)
    
    # Mapping for 'gender'
    gender_mapping = {'male': 0, 'female': 1}
    # Apply mapping to 'gender' if the column exists
    if 'gender' in data.columns:
        data['gender'] = data['gender'].map(gender_mapping)
    else:
        print("'gender' column is not present in the dataframe.")
    
    data['ip_numeric'] = data['client_ip'].apply(ip_to_int)
    
    
    return data

# Function to build and evaluate the decision tree model for country prediction
def build_and_evaluate_country_model(data):
    # Preprocess data
    data = preprocess_data_for_country_prediction(data)
    
    # Split the data into features and target label
    X = data[['ip_numeric']]  # Features
    y = data['country']       # Target label
    
    # Split the dataset into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Initialize and train the decision tree classifier
    country_classifier = DecisionTreeClassifier(random_state=42)
    country_classifier.fit(X_train, y_train)
    
    # Make predictions on the test set
    country_predictions = country_classifier.predict(X_test)
    
    # Evaluate the model
    country_accuracy = accuracy_score(y_test, country_predictions)
    print(f'Country prediction accuracy: {country_accuracy:.2%}')
    
    return country_classifier

# Function to build and evaluate the decision tree model for income prediction
def build_and_evaluate_income_model(data):
    # Preprocess data
    data = preprocess_data_for_income_prediction(data)
    
    # Split the data into features and target label
    feature_columns = ['age_midpoint', 'gender', 'ip_numeric'] # Features
    X = data[feature_columns]  
    y = data['income']          # Target label
    
    # Split the dataset into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Initialize and train the decision tree classifier
    income_classifier = DecisionTreeClassifier(random_state=42)
    income_classifier.fit(X_train, y_train)
    
    # Make predictions on the test set
    income_predictions = income_classifier.predict(X_test)
    
    # Evaluate the model
    income_accuracy = accuracy_score(y_test, income_predictions)
    print(f'Income prediction accuracy: {income_accuracy:.2%}')
    
    return income_classifier

# Main program
if __name__ == '__main__':
    connection_params = {
        'host': '34.30.250.225',
        'user': "root",
        'password': "%(\\n9OqkXz\\^k'0[",
        'database': "serena-database"
    }
    
    # Define the SQL query to retrieve the necessary fields for country prediction
    query_for_country = 'SELECT client_ip, country FROM serena_hw5_Requests'  
    
    # Retrieve and preprocess the data for country prediction
    data_for_country = get_data_from_db(query_for_country, connection_params)
    data_for_country = preprocess_data_for_country_prediction(data_for_country)
    # Build and evaluate the country prediction model
    build_and_evaluate_country_model(data_for_country)
    
    # Define the SQL query to retrieve the necessary fields for income prediction
    query_for_income = 'SELECT gender, age, time_of_day, client_ip, country, income FROM serena_hw5_Requests' 
    
    # Retrieve and preprocess the data for income prediction
    data_for_income = get_data_from_db(query_for_income, connection_params)
    data_for_income = preprocess_data_for_income_prediction(data_for_income)
    # Build and evaluate the income prediction model
    build_and_evaluate_income_model(data_for_income)
