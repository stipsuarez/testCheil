from fastapi import FastAPI, Header,Request, HTTPException
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
print(df)

#Second
lazy_df = pl.read_parquet(parquet_path)
# Realizar la limpieza de datos eliminando registros incompletos
cleaned_df = lazy_df.drop_nulls()
# Mostrar el DataFrame resultante
print("Clened data")
print(cleaned_df)


#Third
# Lista de campos para calcular la desviación estándar
campos_desviacion_estandar = [
    "compactness", "circularity", "distance_circularity", "radius_ratio",
    "pr.axis_aspect_ratio", "max.length_aspect_ratio", "scatter_ratio",
    "elongatedness", "pr.axis_rectangularity", "max.length_rectangularity",
    "scaled_variance", "scaled_variance.1", "scaled_radius_of_gyration",
    "scaled_radius_of_gyration.1", "skewness_about", "skewness_about.1",
    "skewness_about.2", "hollows_ratio"
]
 # Ensure that the DataFrame contains the 'compactness' column
print("before show")
if "compactness" in cleaned_df:
        # Calcular la desviación estándar agrupada por el campo "class"
        print("have data")
        desviacion_estandar_df = (
            cleaned_df.groupby("class")
            .agg(
                **{campo: pl.col(campo).std() for campo in campos_desviacion_estandar}
            )
        )
        # Mostrar el DataFrame resultante con la desviación estándar
        print(desviacion_estandar_df)
        #return {"data": desviacion_estandar_df.to_list()}
else:
        print({"error": "Column 'compactness' not found in the DataFrame."})

def secure(token):
    # if we want to sign/encrypt the JSON object: {"hello": "world"}, we can do it as follows
    # encoded = jwt.encode({"hello": "world"}, JWT_SECRET, algorithm=JWT_ALGORITHM)
    decoded_token = jwt.decode(token, JWT_SECRET, algorithms=JWT_ALGORITHM)
    print("token")
    print(decoded_token)
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

@app.get("/")
async def read_root():
    return {"Hello": "World"}

@app.get("/getData")
async def getData(token: str = Header(None)):
    global tokenA
    try:
        # Convertir el token bytes a str
        token_bytes_str = tokenA.decode('utf-8')
        print(token,tokenA,(token==token_bytes_str))
        if(token==token_bytes_str):
            decoded = secure(token)
            return {"data": cleaned_df.to_struct("data").to_list(),"desviacion_estandar_df": desviacion_estandar_df.to_struct("data").to_list()}
        else:
            return {"message":"Ivalid token","status":"400"}
    except:
        return {"message":"Unauthorized Access, please loggin!","status":"404"}
    raise HTTPException(status_code=404, detail=f"Could not find user with id: {item_id}")