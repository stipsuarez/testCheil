﻿from fastapi import FastAPI, Header,Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import jwt
import datetime
import polars as pl

#Secret key to configure jwt
JWT_SECRET = "secret" 
JWT_ALGORITHM = "HS256"
JWT_EXPRID_TIME = 1
tokenA=""

app = FastAPI()

# Add CORS middleware to allow requests from any origin (*)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

#First
csv_path = "testCheil/files/vehicle.csv"
parquet_path = "testCheil/files/vehicles.parquet"
# Read file CSV in a DataFrame Polars
df = pl.read_csv(csv_path)
# Save DataFrame in format PARQUET
df.write_parquet(parquet_path)

#Second
# Use LazyFrame mode to read data from the parquet file
lazy_df = pl.scan_parquet(parquet_path)
# clean incomplete 
cleaned_df = lazy_df.drop_nulls()
# Mostrar el DataFrame resultante


#Third
# List of fields to calculate standard deviation
field_deviation_std = [
    "compactness", "circularity", "distance_circularity", "radius_ratio",
    "pr.axis_aspect_ratio", "max.length_aspect_ratio", "scatter_ratio",
    "elongatedness", "pr.axis_rectangularity", "max.length_rectangularity",
    "scaled_variance", "scaled_variance.1", "scaled_radius_of_gyration",
    "scaled_radius_of_gyration.1", "skewness_about", "skewness_about.1",
    "skewness_about.2", "hollows_ratio"
]
# List field to calculate average
campos_promedio = field_deviation_std
 # Ensure that the DataFrame contains the 'compactness' column

if "compactness" in cleaned_df:
        # Calcalete standar  desvietion grupping fields by "class" column
        desviacion_estandar_df = (
            cleaned_df.groupby("class")
            .agg(   #Agregation function to loop field_deviation_std 
                **{campo: pl.col(campo).std() for campo in field_deviation_std}  #calculate the deviation standar for each field
            )
        )

        # Calculate the average grupping by "class" column
        promedio_df = (
            cleaned_df.groupby("class")
            .agg(
                **{campo: pl.col(campo).mean() for campo in campos_promedio}
             )
        )
else:
        print({"error": "Column 'compactness' not found in the DataFrame."})

#decode token to get claims
def secure(token):
    decoded_token = jwt.decode(token, JWT_SECRET, algorithms=JWT_ALGORITHM)
    # this is often used on the client side to encode the user's email address or other properties
    return decoded_token


@app.post("/login")
def authenticate(username, password):
    global tokenA
    if username == "admin" and password == "password":
        # Create a JWT token with a subject claim "admin" and an expiration time of 1 hour
        payload = {"sub": "admin", "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=1)}
        tokenA = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
        return {"message":"Authorized!","token":tokenA}
    else:
        return {"message":"You don't have pression to acces, please login","status":"400"}

@app.post("/logout")
def logout():
    global tokenA
    tokenA=""
    return {"message":"Logged out!","status":"200"}

@app.get("/getData")
async def getData(token: str = Header(None)):
    global tokenA, cleaned_df
    try:
        # Convertir el token bytes a str
        token_bytes_str = tokenA.decode('utf-8')

        if token == token_bytes_str:
            decoded = secure(token)
            # Execute the query plan and get the result using lazyFrames of polars
            cleaned_df_result = cleaned_df.collect() 
            desviacion_estandar_df_result = desviacion_estandar_df.collect()
            promedio_df_result = promedio_df.collect()
             # Convert PyDataFrame to list of dictionaries for JSON serialization
            cleaned_data = cleaned_df_result.to_struct("data").to_list()
            desviacion_estandar_data = desviacion_estandar_df_result.to_struct("data").to_list()
            promedio_data = promedio_df_result.to_struct("data").to_list()
            return {"data": cleaned_data, "desviacion_estandar_df": desviacion_estandar_data,"promedio_df":promedio_data}
        else:
            return {"message": "Invalid token", "status": "400"}
    except:
        return {"message": "Unauthorized Access, please login!", "status": "404"}